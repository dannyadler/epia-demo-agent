# Epia Demo Agent — BioT connectivity agent (Epia Neuro demo)

Python device agent simulating the Epia exoskeleton control unit (`EXO-0001`)
connected to the Epia Neuro BioT environment (2.4.2, US). Forked from
`dannyadler/hologic-demo-agent`. **All demo data is synthetic.**

Confidential — contains Epia Neuro confidential material.

## What it implements

| Capability | Mechanism |
|---|---|
| Simplified install & onboarding (non-IT operator) | `agent_gui.py` first-run Activation wizard: site/serial/enrollment code -> Activate, credentials staged, connects and registers |
| Pre-install network validation | `preflight_checks()` (in wizard + `agent.py --preflight`): DNS, port 8883, API TLS handshake / interception detection |
| Persistent outbound-only connectivity | MQTT/mTLS (per-device X.509), clientId `dev_EXO-0001`, `_status._connection` on platform |
| Status telemetry | `<clientId>/from-device/status`: softwareVersion, battery, lastErrorCode — every 10s |
| Offline store-and-forward, chronological replay | SQLite queue, drains on reconnect, no silent loss |
| OTA via the FOTA module | `configuration` shadow delivers `software_version` as `{id}`; agent resolves name/type/pccp_category by id via the API, installs, validates, reports |
| Auto rollback | tracks last known-good; a version name ending `-bad` (or in config `knownBadVersions`) fails post-install validation, auto-rolls-back, is not retried |
| Stimulation parameters — the cloud proposes, the device disposes | `stimulationParam1/2` arrive over the shadow; device range-checks against `limits` in config.json; applies+reports or rejects+reports the actual value. Hard safety limits live on the device |
| Session close → Data Processing Plugin | `s` key / Close Session button: uploads a small synthetic raw batch (File API) and creates a `monitoring` usage session (startTime, duration, rawDataFile, softwareVersionUsed); the cloud plugin writes `score` |
| Device REST API access | MQTT token flow: publish empty to `<clientId>/from-device/token`, JWT arrives on `to-device/token` |
| Fault injection | `e` key / Inject Fault: sets `lastErrorCode` in status |

## Run (macOS)

```sh
python3 -m venv .venv && source .venv/bin/activate
pip install paho-mqtt certifi
python3 agent_gui.py            # GUI: first launch shows the Activation wizard
python3 agent_gui.py --reenroll # replay the wizard for a demo
python3 agent.py                # headless (keys: s close session, e fault, q quit)
python3 agent.py --preflight    # standalone pre-install network validator
python3 agent.py --config <p>   # alternate config (additional device identities)
```

**Certificates:** `certs/` is not in git. Generate a per-device certificate in the
BioT console (device → generate certificate), download the JSON bundle, then run
`python3 install_certs.py <bundle.json>` to stage `certs/certificate.pem`,
`certs/private_key.pem`, `certs/ca.pem`.

**Rollback demo:** deploy a good version (e.g. `exo-fw-1.4.2`) so the prior
version becomes last known-good; then assign a version named with a `-bad`
suffix (e.g. `exo-mw-2.1.0-bad`). Post-install validation fails and the agent
automatically rolls back to the last known-good and refuses to retry.

**Parameter rejection demo:** PATCH `stimulationParam1` to a value outside the
device-side limits in config.json (portal range checks stop this client-side,
so use the API) — the device rejects it, keeps its own value, reports the
actual state, and raises `PARAM_REJECTED` in `lastErrorCode`.

Self-checks: `python3 test_queue.py`, `python3 test_rollback.py`.
Set `"debugShadow": true` in config.json to log raw shadow traffic.

## Files

- `agent.py` — agent core (single file by design for the demo)
- `agent_gui.py` — tkinter GUI: onboarding wizard + device console
- `config.json` — device identity, endpoints, org id, monitoring session template id, device-side stimulation limits (no secrets). `geApiVersion` selects the generic-entity API (v1/v3)
- `certs/` — NOT in git. Per-device X.509 cert from BioT (console: device → generate certificate). Place `certificate.pem`, `private_key.pem`, `ca.pem` here
- `install_certs.py` — stages the console-downloaded credential bundle into `certs/`
- `test_queue.py` — offline-queue self-check
- `test_rollback.py` — post-install validation + rollback self-check

## Platform contracts (live-verified on 2.4.2)

- Status attrs come back NESTED under `_status` on GET /device/v2/devices/{id}; config attrs under `_configuration`.
- Device create API needs `_id` + `_templateId` (flat); wrong body shape returns a misleading 403 ACCESS_DENIED.
- Config shadow (`$aws/things/<clientId>/shadow/name/configuration`) delivers reference attributes as `{id}` ONLY — no display name. This agent resolves the version by id via the generic-entity API (v1.2 model: no name-label attribute).
- Report config changes back to `.../configuration/update` as `{"state":{"reported":{...}}}` or the delta refires forever.
- Usage sessions: POST creates ACTIVE (`_templateId`, `_startTime`, `_state`), then PATCH to DONE with the summary fields nested under `_summary` (incl. `_stopReason`, `_stopReasonCode`).
- File upload: POST `/file/v1/files/upload` `{name, mimeType}` → `{id, signedUrl}` → PUT bytes to signedUrl → attach `{id}` to the entity's FILE attribute.
- File download: GET `/file/v1/files/{id}/download` → `{signedUrl}` → GET bytes.
- Don't call `wait_for_publish()` inside paho callbacks (deadlocks the network loop).

## Deliberate demo shortcuts (hardening backlog)

1. OTA install + validation are simulated (`_apply_package` sleeps; `_validate_install` fails on `-bad`). A real device downloads the `software_file` via the File API, verifies its signature, and installs A/B.
2. The raw-signal batch is generated synthetic content.
3. Credential provisioning is pre-staged; real fleet-provisioning cert issuance is future work.
4. Token handling: fresh token per batch; add retry/backoff.
5. Queue: unbounded; add size cap + overflow alert.
