"""Account-free target scan heartbeat, visible across Gunicorn preload/fork."""
import json
import os
from pathlib import Path
import tempfile
import time


def _path(pid):
    return Path(tempfile.gettempdir()) / ('np-target-scan-%s.json' % pid)


def record_scan(summary):
    payload = {'producer_pid': os.getpid(), 'recorded_at': time.time(),
               'last_scan': {k: summary.get(k) for k in
                             ('checked_at', 'checked', 'recorded', 'errors')}}
    path = _path(os.getpid())
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(payload))
    os.replace(temporary, path)


def read_report(interval=300):
    reports = []
    for pid in {os.getpid(), os.getppid()}:
        try:
            payload = json.loads(_path(pid).read_text())
            if payload.get('producer_pid') != pid:
                continue
            age = time.time() - float(payload['recorded_at'])
            scan = {k: payload['last_scan'].get(k) for k in
                    ('checked_at', 'checked', 'recorded', 'errors')}
            reports.append({'authority': 'V214_EXACT_LIVE_PASS_RECONCILIATION',
                            'last_scan': scan, 'age_seconds': round(age, 1),
                            'healthy': -5 <= age <= max(300, float(interval)) + 120
                            and scan.get('errors') == 0})
        except (OSError, ValueError, TypeError, KeyError):
            continue
    if reports:
        return min(reports, key=lambda item: abs(item['age_seconds']))
    return {'authority': 'V214_EXACT_LIVE_PASS_RECONCILIATION',
            'healthy': False, 'last_scan': None, 'age_seconds': None}
