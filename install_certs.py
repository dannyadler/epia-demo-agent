#!/usr/bin/env python3
"""Install device credentials from the console-downloaded bundle.

Usage:  python3 install_certs.py [~/Downloads/epia-EXO-0001-certs.json]

Reads the JSON bundle (certificate, privateKey, caCertificate) downloaded from
the BioT console session and writes certs/certificate.pem, certs/private_key.pem,
certs/ca.pem next to this script. The bundle file is then safe to delete.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
src = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/Downloads/epia-EXO-0001-certs.json")
bundle = json.load(open(src))
os.makedirs(os.path.join(HERE, "certs"), exist_ok=True)
for key, fname in [("certificate", "certificate.pem"), ("privateKey", "private_key.pem"), ("caCertificate", "ca.pem")]:
    path = os.path.join(HERE, "certs", fname)
    with open(path, "w") as f:
        f.write(bundle[key])
    os.chmod(path, 0o600)
print("Installed certs/ for", bundle.get("connectionClientId", "device"),
      "->", bundle.get("endPointUrl", ""))
print("You can delete", src)
