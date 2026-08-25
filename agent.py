#!/usr/bin/env python3
"""Epia demo device agent — BioT connectivity agent for the Epia Neuro demo.

Simulates the Epia exoskeleton (control unit) connected to the Epia Neuro
BioT environment over MQTT/mTLS, following the same flow as a real device:

  - persistent outbound-only MQTT connection (TLS 1.2+, per-device X.509 cert)
  - STATUS telemetry to <clientId>/from-device/status
    (softwareVersion, battery, lastErrorCode)
  - offline store-and-forward queue (SQLite), chronological replay on reconnect
  - remote configuration via the BioT `configuration` named shadow:
    OTA target version (software_version reference, resolved by id via the
    API), stimulation parameters (device-side range check: the cloud
    proposes, the device disposes), and log level
  - OTA install with post-install validation, last-known-good tracking and
    automatic rollback (a version name ending in `-bad` deterministically
    fails validation)
  - device REST API access via the MQTT token flow (<clientId>/from-device/token)
  - session close: uploads a small synthetic raw-signal file via the File API
    and creates a `monitoring` usage session carrying the summary fields
    (startTime, duration, rawDataFile, softwareVersionUsed); the cloud-side
    Data Processing Plugin writes the score

All demo data is synthetic.

Run:  python3 agent.py            (config.json in the same folder)
      python3 agent.py --config <path>
Keys: s=close session, e=inject fault, q=quit
"""
import gzip
import json
import os
import shutil
import socket
import sqlite3
import ssl
import sys
import threading
import time
import random
import urllib.error
import urllib.request
from urllib.parse import urlparse

import paho.mqtt.client as mqtt

# When frozen by PyInstaller, __file__ points into the temp extraction dir, so
# config.json and certs/ must be found next to the .exe instead. Keeping them
# external (not bundled) is also correct: certs are per-device and secret.
if getattr(sys, "frozen", False):
    HERE = os.path.dirname(sys.executable)
else:
    HERE = os.path.dirname(os.path.abspath(__file__))

_cfg_path = os.path.join(HERE, "config.json")
if "--config" in sys.argv:
    _cfg_path = sys.argv[sys.argv.index("--config") + 1]
CFG = json.load(open(_cfg_path))

AGENT_VERSION = "1.0.0"
DEFAULT_SW_VERSION = "exo-fw-1.4.0"  # factory-installed software version
DEBUG_SHADOW = CFG.get("debugShadow", False)

# HTTPS REST verification: prefer certifi's CA bundle; fall back to the
# platform default (macOS ships one).
try:
    import certifi
    _SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except Exception:
    _SSL_CTX = ssl.create_default_context()


# ---------------------------------------------------------------- store ----
class Store:
    """SQLite: offline queue + persistent key/value device state."""

    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS q (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " topic TEXT, payload TEXT, ts INTEGER)"
        )
        self.db.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
        self.db.commit()
        self.lock = threading.Lock()

    def put(self, topic, payload, ts):
        with self.lock:
            self.db.execute("INSERT INTO q (topic, payload, ts) VALUES (?,?,?)", (topic, payload, ts))
            self.db.commit()

    def depth(self):
        with self.lock:
            return self.db.execute("SELECT COUNT(*) FROM q").fetchone()[0]

    def drain(self, publish_fn):
        while True:
            with self.lock:
                row = self.db.execute("SELECT id, topic, payload FROM q ORDER BY id LIMIT 1").fetchone()
            if row is None:
                return 0
            mid, topic, payload = row
            if not publish_fn(topic, payload):
                return self.depth()
            with self.lock:
                self.db.execute("DELETE FROM q WHERE id=?", (mid,))
                self.db.commit()

    def get(self, k, default=None):
        with self.lock:
            row = self.db.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
        return row[0] if row else default

    def set(self, k, v):
        with self.lock:
            self.db.execute("INSERT INTO kv (k,v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)))
            self.db.commit()


OfflineQueue = Store  # test_queue.py compatibility


# ---------------------------------------------------------------- agent ----
class Agent:
    def __init__(self):
        self.client_id = CFG["connectionClientId"]
        self.device_id = CFG["deviceId"]
        self.api = CFG.get("apiBase", "https://api.dev.epia-neuro.biot-med.com")
        self.org_id = CFG["ownerOrganizationId"]
        self.session_template_id = CFG.get("sessionTemplateId", "")
        self.ge_api = CFG.get("geApiVersion", "v3")  # generic-entity API: v1 or v3
        self.limits = CFG.get("limits", {
            "stimulationParam1": {"min": 1, "max": 10},
            "stimulationParam2": {"min": 20, "max": 180},
        })

        self.status_topic = f"{self.client_id}/from-device/status"
        shadow = f"$aws/things/{self.client_id}/shadow/name/configuration"
        self.cfg_delta_topic = f"{shadow}/update/delta"
        self.cfg_get_accepted = f"{shadow}/get/accepted"
        self.cfg_get_topic = f"{shadow}/get"
        self.cfg_update_topic = f"{shadow}/update"
        self.token_sub_topic = f"{self.client_id}/to-device/token"
        self.token_pub_topic = f"{self.client_id}/from-device/token"

        self.store = Store(os.path.join(HERE, CFG.get("queueDb", "offline_queue.db")))
        self.queue = self.store
        self.connected = False
        self.sw_version = self.store.get("sw_version", CFG.get("factorySwVersion", DEFAULT_SW_VERSION))
        self.session_count = int(self.store.get("session_count", 0))
        self.log_level = self.store.get("log_level", "info")
        self.battery = float(self.store.get("battery", 96.0))
        self.stim = {
            "stimulationParam1": int(self.store.get("stimulationParam1", 4)),
            "stimulationParam2": int(self.store.get("stimulationParam2", 80)),
        }
        self.last_error = ""
        self.updating = False
        self.stop = False
        self.require_manual_ota = False   # GUI sets True: operator must click Install
        self.pending_ota = None           # (version_name, version_id, meta) awaiting operator approval
        self.prev_sw_version = self.store.get("prev_sw_version", "")  # last known-good
        self.failed_versions = set()      # versions that failed post-install validation
        self.rejected_params = {}         # attr -> last rejected desired value (no retry loop)
        self.last_update_status = self.store.get("last_update_status", "")
        self.session_active_since = None  # set when a session is started in the GUI
        self.manual_offline = False       # demo: operator-triggered network drop
        self._token = None
        self._token_evt = threading.Event()

        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id, protocol=mqtt.MQTTv311)
        c.tls_set(
            ca_certs=os.path.join(HERE, CFG.get("certsDir", "certs"), "ca.pem"),
            certfile=os.path.join(HERE, CFG.get("certsDir", "certs"), "certificate.pem"),
            keyfile=os.path.join(HERE, CFG.get("certsDir", "certs"), "private_key.pem"),
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        c.reconnect_delay_set(min_delay=CFG.get("reconnectMinSec", 1), max_delay=CFG.get("reconnectMaxSec", 30))
        c.on_connect = self.on_connect
        c.on_disconnect = self.on_disconnect
        c.on_message = self.on_message
        self.mqtt = c

    # -- callbacks
    def on_connect(self, client, userdata, flags, rc, props=None):
        self.connected = True
        log(f"MQTT connected as {self.client_id}")
        client.subscribe([(self.cfg_delta_topic, 1), (self.cfg_get_accepted, 1), (self.token_sub_topic, 1)])
        client.publish(self.cfg_get_topic, "{}", qos=1)  # fetch config missed while offline
        threading.Thread(target=self.drain_queue, daemon=True).start()

    def on_disconnect(self, client, userdata, flags, rc, props=None):
        self.connected = False
        log(f"MQTT disconnected (rc={rc}) — queuing locally, auto-reconnect with backoff")

    def on_message(self, client, userdata, msg):
        raw = msg.payload.decode(errors="replace")
        if DEBUG_SHADOW:
            log(f"SHADOW MSG on {msg.topic}: {raw[:400]}")
        try:
            body = json.loads(raw or "{}")
        except Exception:
            return
        if msg.topic == self.token_sub_topic:
            self._token = (body.get("data") or {}).get("accessJwt", {}).get("token")
            if self._token:
                self._token_evt.set()
            return
        if msg.topic == self.cfg_delta_topic:
            state = body.get("state") or {}
        elif msg.topic == self.cfg_get_accepted:
            state = (body.get("state") or {}).get("delta") or {}
        else:
            return
        if state:
            threading.Thread(target=self.apply_config, args=(state,), daemon=True).start()

    # -- device REST API access (docs: device-api-access)
    def get_api_token(self, timeout=10):
        """Fresh JWT per batch of API calls, per BioT recommendation."""
        self._token_evt.clear()
        self._token = None
        self.mqtt.publish(self.token_pub_topic, "", qos=1)
        if not self._token_evt.wait(timeout):
            raise RuntimeError("device API token not received within timeout")
        return self._token

    def api_request(self, method, path, token, body=None, data=None, content_type="application/json"):
        url = path if path.startswith("http") else f"{self.api}{path}"
        payload = data if data is not None else (json.dumps(body).encode() if body is not None else None)
        req = urllib.request.Request(url, data=payload, method=method)
        if not path.startswith("http"):
            req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(req, timeout=30, context=_SSL_CTX) as res:
                text = res.read().decode()
                return json.loads(text) if text else {}
        except urllib.error.HTTPError as e:
            # surface the server's validation message, not just the status line
            detail = ""
            try:
                detail = e.read().decode()[:400]
            except Exception:
                pass
            raise RuntimeError(f"HTTP {e.code} {method} {path.split('?')[0]}: {detail}") from None

    def get_generic_entity(self, template_name, entity_id, token):
        """GET a generic entity by id, handling the V1/V3 API split."""
        if self.ge_api == "v3":
            return self.api_request("GET", f"/generic-entity/v3/generic-entities/{template_name}/{entity_id}", token)
        return self.api_request("GET", f"/generic-entity/v1/generic-entities/{entity_id}", token)

    # -- fault injection: sets lastErrorCode in status (the one injected fault)
    def handle_error_event(self):
        code = f"E-{random.randint(1000,9999)}"
        self.last_error = code
        log(f"fault injected: {code} — reported via device status (lastErrorCode)")
        self.send_status()

    # -- session close: raw file upload + monitoring usage session -----------
    def close_session(self, duration_min=None):
        threading.Thread(target=self._do_close_session, args=(duration_min,), daemon=True).start()

    def _do_close_session(self, duration_min=None):
        try:
            if duration_min is None:
                duration_min = random.randint(30, 60)
            start_ts = time.time() - duration_min * 60
            start_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(start_ts))
            end_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
            log(f"SESSION: closing monitoring session ({duration_min} min) — uploading raw signal batch...")
            token = self.get_api_token()
            # 1. synthetic raw signal batch (small, labelled synthetic)
            raw = synthetic_signal_file(self.device_id, self.sw_version, duration_min)
            fname = f"{self.device_id}-session-{int(time.time())}.synthetic.csv.gz"
            f = self.api_request("POST", "/file/v1/files/upload", token,
                                 body={"name": fname, "mimeType": "application/gzip"})
            self.api_request("PUT", f["signedUrl"], token, data=raw, content_type="application/gzip")
            log(f"SESSION: raw file uploaded ({fname}, {len(raw)} bytes)")
            # 2. monitoring usage session: create ACTIVE, then close DONE with
            #    the summary fields nested under _summary (live-verified 2.4.2
            #    contract). The cloud-side Data Processing Plugin fires on the
            #    close and writes the score.
            created = self.api_request("POST", f"/device/v1/devices/{self.device_id}/usage-sessions", token, body={
                "_templateId": self.session_template_id,
                "_startTime": start_iso,
                "_state": "ACTIVE",
            })
            session_id = created.get("_id") or created.get("id")
            self.api_request("PATCH", f"/device/v1/devices/{self.device_id}/usage-sessions/{session_id}", token, body={
                "_state": "DONE",
                "_endTime": end_iso,
                "_summary": {
                    "_stopReason": "Session completed",
                    "_stopReasonCode": "COMPLETION",
                    "startTime": start_iso,
                    "duration": duration_min,
                    "rawDataFile": {"id": f["id"]},
                    "softwareVersionUsed": self.sw_version,
                },
            })
            self.session_count += 1
            self.store.set("session_count", self.session_count)
            self.battery = max(15.0, self.battery - random.uniform(3, 7))
            self.store.set("battery", self.battery)
            log(f"SESSION: monitoring session created (total={self.session_count}) — score arrives from the Data Processing Plugin")
            self.send_status()
        except Exception as e:
            log(f"SESSION: FAILED — {e}")

    # -- remote configuration -------------------------------------------------
    def apply_config(self, state):
        if "logLevel" in state and state["logLevel"]:
            self.log_level = state["logLevel"]
            self.store.set("log_level", self.log_level)
            log(f"CONFIG: log level -> {self.log_level}")
            self.report_config({"logLevel": self.log_level})

        # therapy / stimulation parameters: the cloud proposes, the device
        # disposes. Hard safety limits live HERE, on the device.
        for attr in ("stimulationParam1", "stimulationParam2"):
            if attr not in state or state[attr] is None:
                continue
            desired = state[attr]
            lim = self.limits.get(attr, {})
            try:
                val = int(desired)
                in_range = lim.get("min", val) <= val <= lim.get("max", val)
            except (TypeError, ValueError):
                val, in_range = None, False
            if in_range:
                self.stim[attr] = val
                self.store.set(attr, val)
                self.rejected_params.pop(attr, None)
                log(f"PARAM: {attr} -> {val} (within device limits {lim.get('min')}..{lim.get('max')}) — applied and reported")
                self.report_config({attr: val})
            else:
                if self.rejected_params.get(attr) == desired:
                    return  # already rejected this exact value; don't loop
                self.rejected_params[attr] = desired
                self.last_error = f"PARAM_REJECTED {attr}={desired} outside device limits [{lim.get('min')},{lim.get('max')}]"
                log(f"PARAM: {attr}={desired} REJECTED by device — outside device-side limits "
                    f"[{lim.get('min')},{lim.get('max')}]. Device keeps {self.stim[attr]} (authoritative).")
                # report the actual applied value: the device state is the truth
                self.report_config({attr: self.stim[attr]})
                self.send_status()

        # OTA target: `software_version` is a reference attribute under
        # _configuration — the shadow delivers it as {"id": ...} only; the
        # agent resolves the version details by id via the API.
        target = state.get("software_version")
        if target:
            version_id = target.get("id") if isinstance(target, dict) else target
            reported = {"software_version": target}
            if not version_id:
                self.report_config(reported)
                return
            try:
                token = self.get_api_token()
                ver = self.get_generic_entity("software_version", version_id, token)
                name = ver.get("_name") or ""
                vtype = ver.get("type") or "firmware"
                pccp = ver.get("pccp_category") or ""
            except Exception as e:
                log(f"OTA: failed to resolve software_version {version_id} — {e}")
                return
            label = f"{name} ({vtype}" + (f", PCCP: {pccp}" if pccp else "") + ")"
            if name in self.failed_versions:
                # A version that failed post-install validation is not retried; the
                # device stays on the last known-good until a new target is approved.
                log(f"OTA: {label} previously failed validation — not reinstalling; awaiting a new approved target")
                self.report_config(reported)
                return
            if name and name != self.sw_version and not self.updating:
                if self.require_manual_ota:
                    self.pending_ota = (name, reported, label)
                    log(f"OTA: approved update {label} available — awaiting operator install")
                else:
                    self.run_ota(name, reported, label)
            elif name == self.sw_version:
                log(f"CONFIG: target version {label} already installed")
                self.report_config(reported)

    def _apply_package(self, version_name, label="update"):
        """Simulated package apply: download, verify, install.
        A real device streams the package via the BioT file API and verifies
        its signature."""
        for step, secs in [(f"{label}: downloading package", 3),
                           ("verifying signature and integrity", 2),
                           ("installing (no reboot required)", 3)]:
            log(f"OTA: {step}...")
            time.sleep(secs)
        self.sw_version = version_name
        self.store.set("sw_version", version_name)

    def _validate_install(self, version_name):
        """Post-install health check / auto-rollback trigger.
        A real device runs a self-test suite; here a package whose version
        name ends in '-bad' (or is listed in config knownBadVersions)
        deterministically fails, so the automatic-rollback path is demoable."""
        log("OTA: running post-install validation...")
        time.sleep(1)
        bad = version_name.lower().endswith("-bad") or version_name in CFG.get("knownBadVersions", [])
        return not bad

    def run_ota(self, version_name, reported, label=None):
        """Install an approved package, validate it, and auto-roll-back on failure.
        Preserves the prior version as last known-good."""
        self.updating = True
        old = self.sw_version
        log(f"OTA: update available -> {label or version_name}")
        self._apply_package(version_name)
        if self._validate_install(version_name):
            self.prev_sw_version = old
            self.store.set("prev_sw_version", old)
            self.last_update_status = "ok"
            self.store.set("last_update_status", "ok")
            self.last_error = ""
            self.updating = False
            log(f"OTA: SUCCESS — {old} -> {version_name} (post-install validation passed)")
            self.report_config(reported)
            self.send_status()
        else:
            # Automated rollback to the last known-good state.
            log(f"OTA: post-install validation FAILED for {version_name} — starting automatic rollback")
            self.failed_versions.add(version_name)
            self._apply_package(old, label="rollback to last known-good")
            self.last_update_status = "rolled_back"
            self.store.set("last_update_status", "rolled_back")
            self.last_error = f"UPDATE_ROLLBACK {version_name}->{old}"
            self.updating = False
            log(f"OTA: ROLLED BACK to last known-good {old} — device operational, update rejected")
            # Report the processed target so the shadow delta clears; the device
            # will not retry this version (see failed_versions guard).
            self.report_config(reported)
            self.send_status()

    def approve_pending_ota(self):
        """Operator clicked Install in the GUI."""
        if not self.pending_ota or self.updating:
            return
        name, reported, label = self.pending_ota
        self.pending_ota = None
        threading.Thread(target=self.run_ota, args=(name, reported, label), daemon=True).start()

    def go_offline(self):
        """Demo: drop the MQTT connection for real (no OS network change).
        Telemetry queues locally until go_online()."""
        self.manual_offline = True
        try:
            self.mqtt.disconnect()   # explicit disconnect: paho will NOT auto-reconnect
        except Exception:
            pass
        self.connected = False
        log("DEMO: connection dropped — device is offline, telemetry will queue locally")

    def go_online(self):
        """Demo: re-establish the connection; on_connect drains the queue in order.
        A clean disconnect() also stops paho's loop thread, so we must restart the
        network loop after reconnecting or the CONNACK is never serviced."""
        self.manual_offline = False
        log("DEMO: reconnecting...")
        try:
            self.mqtt.reconnect()
        except Exception:
            try:
                self.mqtt.connect_async(CFG["iotEndpoint"], 8883, keepalive=60)
            except Exception as e:
                log(f"DEMO: reconnect failed — {e}")
        try:
            self.mqtt.loop_start()   # no-op if already running; restarts the thread if disconnect() stopped it
        except Exception:
            pass

    def report_config(self, reported):
        self.mqtt.publish(self.cfg_update_topic, json.dumps({"state": {"reported": reported}}), qos=1)

    # -- publishing
    def publish_raw(self, topic, payload):
        if not self.connected:
            return False
        info = self.mqtt.publish(topic, payload, qos=1)
        try:
            info.wait_for_publish(timeout=5)
        except Exception:
            info = None
        ok = bool(info) and info.is_published()
        if not ok:
            self.connected = False
            log("publish timed out — treating link as offline")
        return ok

    def drain_queue(self):
        if self.store.depth() == 0:
            return
        n = self.store.drain(self.publish_raw)
        if n == 0:
            log("offline queue drained (chronological order)")
        else:
            log(f"queue drain interrupted, {n} left")

    def send_status(self):
        if self.connected and self.store.depth() > 0:
            self.drain_queue()
        data = {
            "softwareVersion": self.sw_version,
            "battery": round(self.battery, 1),
        }
        if self.last_error:
            data["lastErrorCode"] = self.last_error
        ts = int(time.time() * 1000)
        payload = json.dumps({"metadata": {"timestamp": ts}, "data": data})
        if self.publish_raw(self.status_topic, payload):
            log(f"status sent (sw={self.sw_version}, battery={data['battery']}%, queue={self.store.depth()})")
        else:
            self.store.put(self.status_topic, payload, ts)
            log(f"OFFLINE — status queued (depth={self.store.depth()})")

    # -- demo triggers (stdin)
    def stdin_loop(self):
        if not sys.stdin:  # windowed exe has no console/stdin
            return
        log("keys: s=close session (upload), e=inject fault, q=quit")
        for line in sys.stdin:
            k = line.strip().lower()
            if k == "s":
                self.close_session()
            elif k == "e":
                self.handle_error_event()
            elif k == "q":
                self.stop = True
                return

    def run(self):
        log(f"Epia demo agent v{AGENT_VERSION} — device {self.device_id} (installed SW {self.sw_version}) — ALL DATA SYNTHETIC")
        threading.Thread(target=self.stdin_loop, daemon=True).start()
        self.mqtt.connect_async(CFG["iotEndpoint"], 8883, keepalive=60)
        self.mqtt.loop_start()
        interval = CFG.get("statusIntervalSec", 10)
        while not self.stop:
            self.send_status()
            for _ in range(interval * 10):
                if self.stop:
                    break
                time.sleep(0.1)
        self.mqtt.loop_stop()
        self.mqtt.disconnect()
        log("agent stopped")


def synthetic_signal_file(device_id, sw, duration_min):
    """Small synthetic raw-signal batch (gzipped CSV), clearly labelled.
    Matches A5: batch upload per session, not a live 1.5 kHz stream."""
    lines = [f"# SYNTHETIC DEMO DATA — {device_id} — sw {sw} — {duration_min} min session",
             "# channel_1,channel_2,channel_3,channel_4"]
    for _ in range(4000):
        lines.append(",".join(f"{random.gauss(0, 1):.4f}" for _ in range(4)))
    return gzip.compress("\n".join(lines).encode())


def disk_free_gb():
    return shutil.disk_usage(HERE).free / 2**30  # cross-platform (statvfs is Unix-only)


# ---------------------------------------------------- pre-install checks ----
# Real network pre-flight for the onboarding wizard and the standalone
# validator (`python agent.py --preflight`): endpoint reachability, DNS,
# port, and TLS-interception detection before install.
def _host_of(url):
    u = urlparse(url if "://" in url else "https://" + url)
    return u.hostname


def _tcp_check(name, host, port):
    try:
        with socket.create_connection((host, port), timeout=8):
            return {"name": name, "ok": True, "detail": f"{host}:{port} reachable"}
    except Exception as e:
        return {"name": name, "ok": False, "detail": f"{host}:{port} unreachable — {e}"}


def _tls_check(name, host, port=443):
    # A verified handshake failing on an otherwise-reachable host is the classic
    # signal of a corporate TLS-inspection proxy re-signing with a private CA.
    try:
        with socket.create_connection((host, port), timeout=8) as sock:
            with _SSL_CTX.wrap_socket(sock, server_hostname=host) as s:
                issuer = dict(x[0] for x in s.getpeercert().get("issuer", ())).get(
                    "organizationName", "unknown")
                return {"name": name, "ok": True, "detail": f"TLS verified, issuer: {issuer}"}
    except ssl.SSLCertVerificationError:
        return {"name": name, "ok": False,
                "detail": "certificate not trusted — likely TLS interception/proxy; allowlist the endpoint"}
    except Exception as e:
        return {"name": name, "ok": False, "detail": str(e)}


def _dns_check(name, host):
    try:
        return {"name": name, "ok": True, "detail": f"{host} -> {socket.gethostbyname(host)}"}
    except Exception as e:
        return {"name": name, "ok": False, "detail": f"{host}: {e}"}


def preflight_checks(cfg=CFG):
    """Return [{name, ok, detail}] for the device's connectivity prerequisites."""
    iot = cfg["iotEndpoint"]
    api_host = _host_of(cfg.get("apiBase", ""))
    return [
        _dns_check("IoT endpoint DNS resolves", iot),
        _dns_check("API endpoint DNS resolves", api_host),
        _tcp_check("MQTT/TLS port open (8883)", iot, 8883),
        _tls_check("API TLS handshake (443)", api_host),
    ]


LOG_SINKS = []  # optional callables(str): UIs subscribe here; console print always happens


def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    try:
        print(line, flush=True)  # windowed exe may have no stdout
    except Exception:
        pass
    for sink in list(LOG_SINKS):
        try:
            sink(line)
        except Exception:
            pass


def _run_preflight_cli():
    all_ok = True
    print("Pre-install network validation:")
    for r in preflight_checks():
        print(f"  [{'PASS' if r['ok'] else 'FAIL'}] {r['name']} — {r['detail']}")
        all_ok &= r["ok"]
    return all_ok


if __name__ == "__main__":
    if "--preflight" in sys.argv:
        sys.exit(0 if _run_preflight_cli() else 1)
    Agent().run()
