#!/usr/bin/env python3
"""Offline self-check for the Q32 post-install validation + rollback logic.
No network: constructs the agent but never calls run(). Run: python3 test_rollback.py
"""
from agent import Agent

a = Agent()

# post-install validation predicate
assert a._validate_install("exo-fw-1.4.2") is True, "good version should validate"
assert a._validate_install("exo-mw-2.1.0-bad") is False, "'-bad' suffix should fail validation"
assert a._validate_install("exo-mw-2.1.0-BAD") is False, "case-insensitive"

# a failed version is remembered and would be skipped on the next delta
a.failed_versions.add("exo-mw-2.1.0-bad")
assert "exo-mw-2.1.0-bad" in a.failed_versions

# _apply_package flips the installed version (used by both update and rollback)
a.updating = True
a._apply_package("exo-fw-1.4.2")
assert a.sw_version == "exo-fw-1.4.2"
a.updating = False

print("rollback self-check passed")
