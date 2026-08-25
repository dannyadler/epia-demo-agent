#!/usr/bin/env python3
"""Epia Device Console — GUI front-end for the Epia demo agent.

A field-service-friendly window on the device (no command line). Same agent
core as agent.py; this only adds a UI. Demonstrates:
  - Simplified installation & onboarding: a first-run Activation wizard that
    a non-IT operator completes (site, serial, enrollment code -> Activate).
  - Pre-install validation: the wizard runs real network pre-flight checks
    (DNS, port 8883, API TLS handshake / interception) before connecting.
  - Operator-approved self-update: updates arrive as an approved package and
    the operator clicks Install. A `-bad` version auto-rolls-back.
  - Stimulation parameter tiles: the cloud proposes, the device disposes —
    out-of-limit values are rejected on the device and the rejection is shown.
  - Close Session button: uploads the synthetic raw batch and creates the
    monitoring session the Data Processing Plugin scores.
  - Go Offline / Go Online toggle (real MQTT disconnect); Last Known Good tile.

All demo data is synthetic.

Run:  python agent_gui.py                 (first run shows the wizard)
      python agent_gui.py --reenroll      (replay the wizard for a demo)
tkinter ships with Python — no extra install.
"""
import os
import queue
import sys
import threading
import time
import tkinter as tk

from agent import Agent, AGENT_VERSION, LOG_SINKS, CFG, HERE, preflight_checks

PURPLE = "#1F2A5A"
ACCENT = "#2E6BE6"
OK = "#10B981"
GREY = "#9AA0A6"
AMBER = "#F59E0B"
RED = "#EF4444"

DEFAULT_ORG = "Bayview Rehabilitation Center"  # display-only; real org id is in config.json


def ensure_started(agent):
    """Start the agent's run loop exactly once (wizard or console may call)."""
    if not getattr(agent, "_ui_started", False):
        agent._ui_started = True
        threading.Thread(target=agent.run, daemon=True).start()


# ---------------------------------------------------------- onboarding ----
class Wizard:
    """First-run activation. Non-IT operator fills three fields and clicks
    Activate; we validate, run pre-flight checks, provision credentials, connect,
    and confirm the device is registered in the fleet."""

    STEPS = [
        "Validate enrollment code",
        "Check network prerequisites",
        "Provision secure credentials",
        "Connect to BioT",
        "Register device in fleet",
    ]

    def __init__(self, root, agent, on_done):
        self.root = root
        self.agent = agent
        self.on_done = on_done
        self.frame = tk.Frame(root, bg="white")
        self.frame.pack(fill="both", expand=True)
        self.step_rows = []

        hdr = tk.Frame(self.frame, bg=PURPLE)
        hdr.pack(fill="x")
        tk.Label(hdr, text="EPIA NEURO", bg=PURPLE, fg="white",
                 font=("Helvetica", 18, "bold")).pack(anchor="w", padx=16, pady=(12, 0))
        tk.Label(hdr, text="Exoskeleton Control Unit  ·  Device Activation  ·  DEMO (synthetic data)",
                 bg=PURPLE, fg="#C7D2F2", font=("Helvetica", 11)).pack(anchor="w", padx=16, pady=(0, 12))

        body = tk.Frame(self.frame, bg="white")
        body.pack(fill="both", expand=True, padx=24, pady=18)
        tk.Label(body, text="Activate this device", bg="white", fg="#141414",
                 font=("Helvetica", 16, "bold")).pack(anchor="w")
        tk.Label(body, text="No IT specialist required. Enter the details from your activation card.",
                 bg="white", fg=GREY, font=("Helvetica", 11)).pack(anchor="w", pady=(2, 14))

        self.entries = {}
        for label, default in [("Site / Organization", DEFAULT_ORG),
                               ("Device Serial", agent.device_id),
                               ("Enrollment Code", "")]:
            row = tk.Frame(body, bg="white")
            row.pack(fill="x", pady=5)
            tk.Label(row, text=label, bg="white", fg="#141414", width=18, anchor="w",
                     font=("Helvetica", 11)).pack(side="left")
            e = tk.Entry(row, font=("Helvetica", 11), relief="solid", bd=1)
            e.pack(side="left", fill="x", expand=True, ipady=3)
            if default:
                e.insert(0, default)
            self.entries[label] = e
        self.entries["Enrollment Code"].insert(0, "7K4P-2M9Q")  # demo default; any 4+ chars accepts

        self.activate_btn = tk.Button(body, text="Activate",
                                      font=("Helvetica", 12, "bold"), command=self.on_activate)
        self.activate_btn.pack(anchor="w", pady=(12, 6), ipadx=18, ipady=4)

        self.msg = tk.Label(body, text="", bg="white", fg=RED, font=("Helvetica", 10),
                            wraplength=560, justify="left")
        self.msg.pack(anchor="w", pady=(0, 6))

        steps = tk.Frame(body, bg="white")
        steps.pack(fill="x", pady=(6, 0))
        for name in self.STEPS:
            r = tk.Frame(steps, bg="white")
            r.pack(fill="x", pady=3)
            glyph = tk.Label(r, text="○", bg="white", fg=GREY, font=("Helvetica", 13), width=2)
            glyph.pack(side="left")
            tk.Label(r, text=name, bg="white", fg="#141414", font=("Helvetica", 11)).pack(side="left")
            detail = tk.Label(r, text="", bg="white", fg=GREY, font=("Helvetica", 10))
            detail.pack(side="left", padx=8)
            self.step_rows.append((glyph, detail))

    def set_step(self, i, state, detail=""):
        # state: pending | run | ok | fail
        glyph, det = self.step_rows[i]
        mark = {"pending": ("○", GREY), "run": ("…", ACCENT),
                "ok": ("✓", OK), "fail": ("✗", RED)}[state]
        self.root.after(0, lambda: (glyph.config(text=mark[0], fg=mark[1]), det.config(text=detail)))

    def show_msg(self, text, color=RED):
        self.root.after(0, lambda: self.msg.config(text=text, fg=color))

    def on_activate(self):
        self.activate_btn.config(state="disabled")
        self.show_msg("")
        for i in range(len(self.STEPS)):
            self.set_step(i, "pending")
        threading.Thread(target=self._run_activation, daemon=True).start()

    def _fail(self, i, detail, hint):
        self.set_step(i, "fail", detail)
        self.show_msg(hint)
        self.root.after(0, lambda: self.activate_btn.config(state="normal", text="Retry"))

    def _run_activation(self):
        # 1. enrollment code
        self.set_step(0, "run")
        code = self.entries["Enrollment Code"].get().strip()
        if len(code) < 4:
            self._fail(0, "invalid", "Enter the enrollment code from your activation card (at least 4 characters).")
            return
        self.set_step(0, "ok", code)

        # 2. network pre-flight (real checks)
        self.set_step(1, "run")
        results = preflight_checks()
        bad = [r for r in results if not r["ok"]]
        if bad:
            self._fail(1, f"{len(bad)} check(s) failed",
                       "Network prerequisites not met: "
                       + "; ".join(f"{r['name']} ({r['detail']})" for r in bad)
                       + ". See the device network-requirements sheet, then Retry.")
            return
        self.set_step(1, "ok", "DNS, port 8883, TLS all OK")

        # 3. credentials (pre-staged for the demo; production issues a unique
        #    per-device cert via fleet provisioning triggered by the code)
        self.set_step(2, "run")
        certs = [os.path.join(HERE, CFG.get("certsDir", "certs"), f)
                 for f in ("ca.pem", "certificate.pem", "private_key.pem")]
        if not all(os.path.exists(p) for p in certs):
            self._fail(2, "missing", "Device credentials not found. Reinstall the activation package.")
            return
        self.set_step(2, "ok", "device certificate present")

        # 4. connect
        self.set_step(3, "run")
        ensure_started(self.agent)
        for _ in range(300):  # up to ~30s
            if self.agent.connected:
                break
            time.sleep(0.1)
        if not self.agent.connected:
            self._fail(3, "timeout", "Could not establish the secure connection. Check the network and Retry.")
            return
        self.set_step(3, "ok", f"MQTT/mTLS as {self.agent.client_id}")

        # 5. fleet registration confirmed once the platform sees the connection
        self.set_step(4, "run")
        self.agent.send_status()
        self.set_step(4, "ok", "visible in portal fleet")

        # persist so subsequent launches skip the wizard
        self.agent.store.set("enrolled", "1")
        self.agent.store.set("enroll_site", self.entries["Site / Organization"].get().strip())
        self.agent.store.set("enroll_code", code)
        self.show_msg("Device activated. Opening console…", OK)
        self.root.after(900, self._finish)

    def _finish(self):
        self.frame.destroy()
        self.on_done()


# -------------------------------------------------------------- console ----
class Console:
    def __init__(self, root, agent):
        self.root = root
        self.agent = agent
        self.log_q = queue.Queue()
        LOG_SINKS.append(self.log_q.put)

        root.configure(bg="white")
        self.frame = tk.Frame(root, bg="white")
        self.frame.pack(fill="both", expand=True)

        # header
        hdr = tk.Frame(self.frame, bg=PURPLE)
        hdr.pack(fill="x")
        tk.Label(hdr, text="EPIA NEURO", bg=PURPLE, fg="white",
                 font=("Helvetica", 18, "bold")).pack(anchor="w", padx=16, pady=(12, 0))
        tk.Label(hdr, text="Exoskeleton Control Unit  ·  Device Agent  ·  DEMO (synthetic data)",
                 bg=PURPLE, fg="#C7D2F2", font=("Helvetica", 11)).pack(anchor="w", padx=16, pady=(0, 12))

        # status grid
        grid = tk.Frame(self.frame, bg="white")
        grid.pack(fill="x", padx=16, pady=12)
        self.vals = {}
        fields = ["Connection", "Installed SW", "Last Known Good",
                  "Sessions", "Battery", "Queue Depth",
                  "Stim Amplitude (P1)", "Stim Frequency (P2)", "Last Error"]
        for i, f in enumerate(fields):
            r, c = divmod(i, 3)
            cell = tk.Frame(grid, bg="white")
            cell.grid(row=r, column=c, sticky="w", padx=(0, 32), pady=6)
            tk.Label(cell, text=f.upper(), bg="white", fg=GREY,
                     font=("Helvetica", 9, "bold")).pack(anchor="w")
            v = tk.Label(cell, text="—", bg="white", fg="#141414", font=("Helvetica", 15, "bold"))
            v.pack(anchor="w")
            self.vals[f] = v

        # OTA banner (hidden until an approved update is pending)
        self.ota = tk.Frame(self.frame, bg="#FBF3D9")
        self.ota_label = tk.Label(self.ota, bg="#FBF3D9", fg="#8a6d0b", font=("Helvetica", 12))
        self.ota_label.pack(side="left", padx=12, pady=10)
        self.install_btn = tk.Button(self.ota, text="Install Update",
                                     font=("Helvetica", 11, "bold"),
                                     command=self.on_install)
        self.install_btn.pack(side="right", padx=12, pady=8)

        # log pane
        self.activity_label = tk.Label(self.frame, text="Activity", bg="white", fg=GREY,
                                       font=("Helvetica", 9, "bold"))
        self.activity_label.pack(anchor="w", padx=16)
        self.text = tk.Text(self.frame, height=12, bg="#141414", fg="#E7E5E4",
                            font=("Menlo", 11), relief="flat", wrap="word")
        self.text.pack(fill="both", expand=True, padx=16, pady=(4, 8))
        self.text.configure(state="disabled")

        # buttons
        btns = tk.Frame(self.frame, bg="white")
        btns.pack(fill="x", padx=16, pady=(0, 14))
        tk.Button(btns, text="Close Session (upload)", relief="flat", bg="#EFEDEB",
                  font=("Helvetica", 11), command=lambda: self.agent.close_session()
                  ).pack(side="left")
        tk.Button(btns, text="Inject Fault", relief="flat", bg="#EFEDEB",
                  font=("Helvetica", 11), command=lambda: self.bg(self.agent.handle_error_event)
                  ).pack(side="left", padx=8)
        self.net_btn = tk.Button(btns, text="Go Offline", relief="flat", bg="#EFEDEB",
                                 font=("Helvetica", 11), command=self.on_toggle_net)
        self.net_btn.pack(side="right")

        ensure_started(self.agent)
        self.tick()

    def bg(self, fn):
        threading.Thread(target=fn, daemon=True).start()

    def on_install(self):
        self.agent.approve_pending_ota()

    def on_toggle_net(self):
        if self.agent.manual_offline:
            self.bg(self.agent.go_online)
        else:
            self.bg(self.agent.go_offline)

    def tick(self):
        while not self.log_q.empty():
            line = self.log_q.get_nowait()
            self.text.configure(state="normal")
            self.text.insert("end", line + "\n")
            self.text.see("end")
            self.text.configure(state="disabled")
        a = self.agent
        self.vals["Connection"].config(text="Online" if a.connected else "Offline",
                                       fg=OK if a.connected else GREY)
        self.vals["Installed SW"].config(text=a.sw_version)
        self.vals["Last Known Good"].config(text=a.prev_sw_version or "—")
        self.vals["Sessions"].config(text=str(a.session_count))
        self.vals["Battery"].config(text=f"{a.battery:.0f}%")
        self.vals["Queue Depth"].config(text=str(a.store.depth()))
        self.vals["Stim Amplitude (P1)"].config(text=str(a.stim["stimulationParam1"]))
        self.vals["Stim Frequency (P2)"].config(text=str(a.stim["stimulationParam2"]))
        self.vals["Last Error"].config(text=a.last_error or "—",
                                       fg=RED if a.last_error else "#141414")
        if a.manual_offline:
            self.net_btn.config(text="Go Online", bg="#FBE3B3")
        else:
            self.net_btn.config(text="Go Offline", bg="#EFEDEB")
        if a.pending_ota and not a.updating:
            self.ota_label.config(text=f"Approved update {a.pending_ota[2]} is available for this device.")
            if not self.ota.winfo_ismapped():
                self.ota.pack(fill="x", padx=16, pady=(0, 8), before=self.activity_label)
            self.install_btn.config(state="normal")
        elif self.ota.winfo_ismapped():
            self.ota.pack_forget()
        self.root.after(1000, self.tick)


# ------------------------------------------------------------------ app ----
def main():
    root = tk.Tk()
    root.title(f"Epia Device Console — {CFG['deviceId']} (DEMO)")
    root.geometry("720x640")

    agent = Agent()
    agent.require_manual_ota = True  # operator approves installs

    if "--reenroll" in sys.argv:
        agent.store.set("enrolled", "")

    def close():
        agent.stop = True
        root.after(300, root.destroy)
    root.protocol("WM_DELETE_WINDOW", close)

    if agent.store.get("enrolled"):
        Console(root, agent)
    else:
        Wizard(root, agent, on_done=lambda: Console(root, agent))
    root.mainloop()


if __name__ == "__main__":
    main()
