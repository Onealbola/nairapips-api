# Windows Python standard-library recovery — 21 November 2026

VPS check failed before applying any trailing update: Python could not import
`encodings`, and PowerShell confirmed the expected `Lib/encodings/__init__.py`
was missing. `-I` from another directory also failed. The running worker and
scheduled interactive task use the same Python311/python.exe. Programs and
Features repair could not launch its cached installer; the newly downloaded
official Python 3.11.0 installer refused repair because a reboot was pending.
The boot DD task is disabled; the interactive task starts only on logon.
A reboot has NOT been instructed because monitor recovery is unproven.

The add-only recovery ZIP provides the official CPython v3.11.0 pure standard
library from the pinned source archive. It excludes tests, GUI packages,
activation scripts, site-packages and native executables/DLLs. SHA256 manifest
checks every payload file before copying. Recovery checks executable product
version and the existing native socket/SSL/ctypes components before changes.
It restores missing files only, using atomic File.Move without overwrite,
preserves existing modules, and records exactly which files it created.
It does not reboot, stop/start processes, edit startup tasks, touch broker
sessions or modify financial/account/risk state. Python and monitoring imports
are tested after restore; a failure requires review before any worker restart.
No import-time MT5 initialization or broker login is invoked by the helper.

Tests verify delivery, all manifest hashes, path policy, add-only launch guards,
and startup with the recovered library in an isolated Python 3.11 environment.
These Linux checks do not claim a successful Windows repair or native imports.
The user must run the helper and share its result on the VPS.

Endpoint: /downloads/python311-library-recovery.zip
Helper: RESTORE_MISSING_LIBRARY.ps1
Build source: tools/build_python_library_recovery.py
Only after recovery prints both PYTHON STANDARD LIBRARY OK and REQUESTS AND MT5
IMPORT OK should CHECK_ONLY.cmd be retried for the original trailing update.
