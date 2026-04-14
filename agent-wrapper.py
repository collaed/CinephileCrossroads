#!/usr/bin/env python3
"""CinephileCrossroads Agent Wrapper — restarts agent on update or crash."""
import subprocess, sys, os, time

AGENT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent.py")
ARGS = sys.argv[1:]

print(f"[wrapper] Agent: {AGENT}")
print(f"[wrapper] Args: {ARGS}")

while True:
    cmd = [sys.executable, AGENT] + ARGS
    print(f"[wrapper] Running: {' '.join(cmd)}")
    proc = subprocess.run(cmd)
    code = proc.returncode
    if code == 42:
        print("[wrapper] Agent requested restart (code 42)")
        time.sleep(1)
        continue
    elif code != 0:
        print(f"[wrapper] Agent crashed (code {code}), restarting in 10s...")
        time.sleep(10)
        continue
    else:
        print("[wrapper] Agent exited cleanly")
        break
