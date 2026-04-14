# Agent Wrapper & Self-Update Pattern
# A reusable pattern for any self-hosted service with a remote agent

## Problem
You have a server (Docker/cloud) and an agent (user's machine) that need to stay in sync.
The agent runs on Windows/Mac/Linux, talks to the server, and needs to:
- Stay running 24/7 (survive crashes)
- Update itself when the server pushes new code
- Run background tasks without blocking
- Handle unreliable network (SMB, NFS, internet)

## Architecture

```
┌─────────────────┐         ┌──────────────────┐
│  agent-wrapper.py│         │   Server (Docker) │
│  (supervisor)    │         │                  │
│                  │         │  Task Queue      │
│  ┌────────────┐  │  HTTPS  │  ┌────────────┐  │
│  │ agent.py   │◄─┼────────►│  │ app.py     │  │
│  │ (worker)   │  │  GET/   │  │            │  │
│  └────────────┘  │  POST   │  └────────────┘  │
│                  │         │                  │
│  Exit 42 = restart         │  Push new code   │
│  Exit !0 = crash, retry    │  via update_agent│
│  Exit 0  = clean stop      │  task type       │
└─────────────────┘         └──────────────────┘
```

## Wrapper (agent-wrapper.py)
- 15 lines of Python, zero dependencies
- Runs agent.py as a subprocess, passing all CLI args
- Monitors exit code:
  - **42**: Agent updated itself, restart immediately (1s delay)
  - **non-zero**: Crash, restart after 10s backoff
  - **0**: Clean exit (user pressed Ctrl+C), stop
- Never dies unless killed directly
- Logs restart events to console

```python
#!/usr/bin/env python3
import subprocess, sys, os, time

AGENT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent.py")
ARGS = sys.argv[1:]

while True:
    print(f"[wrapper] Starting agent: {AGENT}")
    proc = subprocess.run([sys.executable, AGENT] + ARGS)
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
```

## Self-Update Flow

1. Server detects new agent code is available
2. Server queues `update_agent` task with priority -1 (highest):
   ```json
   {
     "type": "update_agent",
     "priority": -1,
     "params": {
       "code": "<full agent.py source>",
       "path": "C:\\Users\\user\\agent.py"
     }
   }
   ```
3. Agent picks up task on next poll (every 15s)
4. Agent writes new code to disk:
   - Backup current: `agent.py` → `agent.py.bak`
   - Write new code to `agent.py`
5. Agent reports success to server
6. Agent calls `sys.exit(42)`
7. Wrapper sees exit code 42, restarts agent
8. New agent code runs with all improvements

## Threaded Task Execution

The agent runs two types of tasks concurrently:

### Foreground (main thread)
- size_files, hash_files, check_quality, download_subs
- One task per poll cycle (15s)
- Reports result immediately

### Background (daemon thread)
- exec_code, scan_incoming (long-running)
- Runs in parallel with foreground tasks
- Can be cancelled when new bg task arrives
- Uses `cancelled()` callback for cooperative cancellation

```
Poll → see exec_code → spawn bg thread → continue polling
Poll → see hash_files → run inline → report → sleep 15s
Poll → see size_files → run inline → report → sleep 15s
...bg thread still running...
Poll → bg thread done → pick up next exec_code
```

## Offline Buffering

When the server is unreachable:
1. Agent catches connection error
2. Saves result to `agent_buffer.json` on disk
3. Exponential backoff: 15s → 30s → 60s (capped)
4. On reconnect, flushes buffer before fetching new tasks
5. No data loss even during extended outages

## Version Tracking

Agent reports version in every heartbeat:
```python
AGENT_VERSION = "2.1.04141041"  # major.minor.MMDDHHMM
```
Server displays in Library dashboard. Confirms updates landed.

## Task Types (built-in, not exec_code)

| Type | Params | Description |
|------|--------|-------------|
| size_files | paths[], imdb_ids[] | Get file sizes (5s timeout per file) |
| hash_files | paths[] | OpenSubtitles hash (head+tail 64KB) |
| check_quality | paths[] | ffprobe for codec/resolution/audio |
| download_subs | imdb_id, path, language | Search OpenSubtitles by hash |
| scan_incoming | path, min_size | Walk folder for new video files |
| move_file | source, destination | Move + mkdir + cleanup empty dirs |
| delete_file | path, confirm="yes_delete" | Delete with safety token |
| generate_thumb | path, seek | ffmpeg frame extraction |
| find_duplicates | paths[] | Group by file size |
| exec_code | code | Run arbitrary Python (prototyping) |
| update_agent | code, path | Self-update + exit 42 |
| diag | | System info, disk space, path check |

## Reuse in Other Projects

This pattern works for any server+agent architecture:
- **Audiobookshelf agent**: scan downloads, identify books, organize library
- **Photo organizer agent**: scan camera imports, detect faces, tag locations
- **Backup agent**: monitor folders, compress, upload to server
- **Home automation agent**: poll sensors, report to dashboard

The key ingredients:
1. Wrapper for resilience (15 lines)
2. Task queue for communication (GET polling, no websockets needed)
3. Self-update for zero-touch maintenance
4. Threaded execution for responsiveness
5. Offline buffering for reliability
