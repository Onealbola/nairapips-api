NAIRAPIPS DD SUPERVISOR RECOVERY — WINDOWS VPS ONLY

The observed on-disk supervisor has 2,929 bytes, no main function and no startup
entry point. Python now imports the broker dependencies, but this incomplete
script returns immediately, and the read-only process check found no matching DD
supervisor or V3.20 workers.

1. Extract this ZIP into its own NEW folder on the Windows VPS.
2. Double-click REPAIR_SUPERVISOR.cmd.
3. Send the final result, then start the repaired supervisor from the SAME black
   command window you previously used, preserving its environment settings.

This recovery checks source hashes and Python dependencies, verifies the complete
supervisor's main/entry point, and refuses to run while a DD supervisor or worker
is already present. It backs up the old supervisor and helper files, installs
these complete programs plus role locks, and retires only exact DD roster/relay
helper processes belonging to the confirmed engine folder. This consolidates
old duplicate helpers under one supervisor. No generic Python process kill is
used. Other engines and MT5 processes are not stopped.

The installed Static/Trailing DD worker is NOT replaced. All latch, outbox,
journal and terminal directories remain unchanged. The script does not start a
supervisor itself, open new plans, change credentials or write financial data.
The supervisor still requires a restart and live shard verification afterward.
