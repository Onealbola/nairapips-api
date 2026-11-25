param([ValidatePattern('^[0-9]{5,20}$')][string]$Login='477799283')
$ErrorActionPreference='Stop'
$root='C:\NairaPips_DD_Fleet'
$report=[ordered]@{release='DD_TIMING_AUDIT_20261125';checked_at=[DateTime]::UtcNow.ToString('o');mt5_login=$Login;read_only=$true;local_timezone=[TimeZoneInfo]::Local.Id;local_utc_offset_minutes=[DateTimeOffset]::Now.Offset.TotalMinutes;health=@();worker_settings=@();log_evidence=@();breach_evidence=@();warnings=@()}
# Read only whitelisted health fields. Never export roster, credentials or latch contents.
$healthFields=@('release','timestamp','shard','total_shards','owned','monitor_ready','close_ready','close_blocked','close_pending','oldest_valid_seconds','oldest_mt5','stale_mt5','daily_history_probe_ready','closed_balance_history_probe_ready')
foreach($file in @(Get-ChildItem -LiteralPath (Join-Path $root 'state') -Filter 'dd_health_shard_*.json' -ErrorAction SilentlyContinue)){
 try{$data=Get-Content -LiteralPath $file.FullName -Raw|ConvertFrom-Json;$item=[ordered]@{};foreach($key in $healthFields){$item[$key]=$data.$key};$report.health+=,$item}catch{$report.warnings+='A health file could not be read: '+$file.Name}
}
# Inspect code settings without executing it or touching any process/MT5 terminal.
$workers=@(Get-CimInstance Win32_Process | Where-Object {$_.Name -match '^python(w)?\.exe$' -and [string]$_.CommandLine -match 'DD_POLICE.*\.py'})
$seen=@{}
foreach($worker in $workers){
 foreach($m in [regex]::Matches([string]$worker.CommandLine,'(?:"(?<quoted>[^"\r\n]+\.py)"|(?<plain>[A-Za-z]:\\[^\s"]+\.py))')){
  $path=$m.Groups['quoted'].Value;if(-not $path){$path=$m.Groups['plain'].Value};if($seen.ContainsKey($path) -or -not (Test-Path -LiteralPath $path)){continue};$seen[$path]=$true
  $settings=@(Get-Content -LiteralPath $path | Where-Object {$_ -match '^(RELEASE|IDLE_SECONDS|ACTIVE_SECONDS|WARNING_SECONDS|CRITICAL_SECONDS|HOT_SECONDS|AUTH_RETRY_SECONDS|DISCOVERY_TIMEOUT|CLOSE_RETRY_SECONDS|MAX_CONSECUTIVE_RISK_TURNS|MAX_COVERAGE_RESCUE_STREAK)\s*='})
  $report.worker_settings+=,[ordered]@{file=[IO.Path]::GetFileName($path);settings=$settings}
 }
}
# Bounded streaming tail reads: at most 16MiB/file, eight recent logs, 250 matching events/file.
$pattern='(?<![0-9])'+[regex]::Escape($Login)+'(?![0-9])'
$events='DD WATCH|BREACH LATCHED|BREACH CLOSED|BREACH CLOSE FAILED|BREACH CLOSE BLOCKED|BREACH CLOSE ABORT|ZERO EXPOSURE|BREACH POLICED|BREACH RELAY QUEUED|DAILY SNAPSHOT CHANGED|HISTORY UNAVAILABLE|JOURNAL UNAVAILABLE|ACCOUNT VERIFIED'
$files=@(Get-ChildItem -LiteralPath (Join-Path $root 'logs') -File -ErrorAction SilentlyContinue | Where-Object {$_.Name -match '(dd_police|breach_relay).*\.log'} | Sort-Object LastWriteTime -Descending | Select-Object -First 8)
foreach($file in $files){
 $stream=$null;$reader=$null
 try{
  $stream=New-Object IO.FileStream($file.FullName,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::ReadWrite)
  $start=[Math]::Max(0,$stream.Length-16777216);[void]$stream.Seek($start,[IO.SeekOrigin]::Begin);$reader=New-Object IO.StreamReader($stream);if($start -gt 0){[void]$reader.ReadLine()}
  $matchedEvents=New-Object 'System.Collections.Generic.Queue[string]'
  while(-not $reader.EndOfStream){$line=$reader.ReadLine();if($line -match $pattern -and $line -match $events){
   # These operational log event types contain no credential payload; redact defensively.
   $line=$line-replace '(?i)(password|authorization|api[_-]?key|token)\s*[:=]\s*\S+','$1=[REDACTED]'
   if($matchedEvents.Count -ge 250){[void]$matchedEvents.Dequeue()};$matchedEvents.Enqueue($line)
  }}
  $report.log_evidence+=,[ordered]@{file=$file.Name;tail_only=($start -gt 0);events=@($matchedEvents.ToArray())}
 }catch{$report.warnings+='Could not read log: '+$file.Name}finally{if($reader){$reader.Dispose()}elseif($stream){$stream.Dispose()}}
}
# Read only exact-login outbox records through a fixed field allowlist.
# The outbox is appended after the existing zero-exposure check. It is never modified.
$evidenceFields=@('trader_account_id','mt5_login','timestamp','balance','equity','current_balance','current_equity','daily_drawdown','daily_realised','breach_equity_level','drawdown_percent','dd_used_percent','first_breach_positions_unavailable','relay_queued_at','breach_latch_release')
foreach($file in @(Get-ChildItem -LiteralPath (Join-Path $root 'state') -Filter 'breach_outbox_shard_*.jsonl' -ErrorAction SilentlyContinue | Select-Object -First 4)){
 $stream=$null;$reader=$null
 try{
  $stream=New-Object IO.FileStream($file.FullName,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::ReadWrite)
  $start=[Math]::Max(0,$stream.Length-16777216);[void]$stream.Seek($start,[IO.SeekOrigin]::Begin);$reader=New-Object IO.StreamReader($stream);if($start -gt 0){[void]$reader.ReadLine()}
  while(-not $reader.EndOfStream){$line=$reader.ReadLine();if($line -notmatch $pattern){continue};try{$ev=$line|ConvertFrom-Json}catch{continue};if([string]$ev.mt5_login -ne $Login){continue}
   $safe=[ordered]@{file=$file.Name;positions_at_detection_count=$null}
   foreach($key in $evidenceFields){$value=$ev.$key;if($null -eq $value -or $value -is [ValueType] -or $value -is [string]){$safe[$key]=$value}}
   if($null -ne $ev.open_positions_at_breach){$safe.positions_at_detection_count=@($ev.open_positions_at_breach).Count}
   if($report.breach_evidence.Count -lt 30){$report.breach_evidence+=,$safe}
  }
 }catch{$report.warnings+='Could not read exact-account outbox evidence: '+$file.Name}finally{if($reader){$reader.Dispose()}elseif($stream){$stream.Dispose()}}
}
if(-not $report.health.Count){$report.warnings+='No health files found under the standard fleet state directory.'}
$report.warnings+='Log tails are not a complete broker history. Missing events do not prove no enforcement occurred.'
$output=Join-Path ([Environment]::GetFolderPath('Desktop')) ('NairaPips-Risk-Timing-'+$Login+'.json')
$report|ConvertTo-Json -Depth 8|Set-Content -LiteralPath $output -Encoding UTF8
Write-Host ('Read-only timing report saved: '+$output)
Write-Host 'No workers were restarted. No broker login, trading command, rule change or status change was performed.'
notepad.exe $output
