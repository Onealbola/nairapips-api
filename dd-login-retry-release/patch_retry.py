"""Surgical failed-login starvation repair. Leaves all DD/risk and saved state intact."""
from pathlib import Path
import ast,sys,datetime,os
MARKER='# NP_BOUNDED_LOGIN_RETRY_20261112'
OLD='''            for x in rows:
                if x not in verified or x in invalid_live_capital:
                    continue
                last_good=last_valid_dd_read.get(x)
'''
NEW='''            for x in rows:
                # NP_BOUNDED_LOGIN_RETRY_20261112
                # An initial failed login must not remain behind always-due
                # verified accounts. Existing rescue/risk alternation is retained.
                if x not in verified:
                    if (x in first_attempted and next_due.get(x,0)<=now
                            and auth_quarantine_until.get(x,0)<=now
                            and now-last_attempted_at.get(x,now)>=120.0):
                        coverage_overdue.append(x)
                    continue
                if x in invalid_live_capital:
                    continue
                last_good=last_valid_dd_read.get(x)
'''
OLD_SORT='''key=lambda x: last_valid_dd_read.get(x,0.0)'''
NEW_SORT='''key=lambda x: (last_valid_dd_read.get(x,0.0) if x in verified else last_attempted_at.get(x,0.0))'''
def patched(source):
 if MARKER in source:
  if NEW not in source or NEW_SORT not in source:raise ValueError('Incomplete or changed retry repair; review required')
  return source
 if source.count(OLD)!=1 or source.count(OLD_SORT)!=1:raise ValueError('Unknown scheduler version; no files changed')
 result=source.replace(OLD,NEW).replace(OLD_SORT,NEW_SORT)
 ast.parse(result);return result

def install(path):
 path=Path(path);raw=path.read_bytes();source=raw.decode('utf-8-sig').replace('\r\n','\n');result=patched(source)
 if result==source:print('Retry repair is already installed; source unchanged.');return
 backup=path.with_name(path.name+'.before-login-retry-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'.bak')
 with backup.open('xb') as f:f.write(raw)
 newline='\r\n' if b'\r\n' in raw else '\n'
 staged=path.with_name(path.name+'.retry-staged');staged.write_bytes(result.replace('\n',newline).encode('utf-8'))
 if path.read_bytes()!=raw:staged.unlink();raise ValueError('Worker changed during repair; no replacement made')
 os.replace(staged,path);print('Retry repair installed. Backup:',backup)
if __name__=='__main__':install(sys.argv[1])
