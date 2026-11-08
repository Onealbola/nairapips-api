param([switch]$Apply)
$ErrorActionPreference='Stop'
$name='NAIRAPIPS_MT5_ENGINE_NON_DD_OPERATIONS_DIRECT_TELEMETRY_V3_20261004.py'
$found=@()
foreach($p in (Get-CimInstance Win32_Process | Where-Object {$_.Name -match '^python(w)?\.exe$'})) {
 foreach($m in [regex]::Matches([string]$p.CommandLine,'"([^"\r\n]+\.py)"|([A-Za-z]:\\[^\s"]+\.py)')) {
  $path=if($m.Groups[1].Success){$m.Groups[1].Value}else{$m.Groups[2].Value}
  if((Split-Path -Leaf $path) -eq $name -and (Test-Path -LiteralPath $path)) {$found+=(Resolve-Path -LiteralPath $path).Path}
 }
}
$found=@($found|Sort-Object -Unique)
if($found.Count -eq 0){
 $relative=@(Get-CimInstance Win32_Process | Where-Object {$_.Name -match '^python(w)?\.exe$' -and ([string]$_.CommandLine).Contains($name)})
 $known=Join-Path 'C:\Users\Administrator\Documents\nairapips_mt5_rotation_engine (1)' $name
 if($relative.Count -eq 1 -and (Test-Path -LiteralPath $known)){
  $found=@((Resolve-Path -LiteralPath $known).Path)
  Write-Host 'The existing command uses a relative script name. Using the recorded setup path; file hash is checked below.'
 }
}
if($found.Count -ne 1){throw 'Exactly one existing Operations engine path must be confirmed. No files changed.'}
$target=$found[0];$destination=Split-Path -Parent $target
$hash=(Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant()
$body=[IO.File]::ReadAllText($target)
if($hash -ne 'eb7f2799b5f949559c85d9153c2698c2a67eac327523365cb2a366233e4f0863' -and -not $body.Contains('NP_WITHDRAWAL_RULES_V1_20261108')){throw 'Operations file differs from the reviewed original. No files changed.'}
$manifest=Get-Content -LiteralPath (Join-Path $PSScriptRoot 'SHA256.json') -Raw|ConvertFrom-Json
foreach($file in @($name,'withdrawal_controls.py')) {
 $source=Join-Path $PSScriptRoot $file
 if((Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant() -ne $manifest.$file){throw 'Package checksum failed. No files changed.'}
 & python -c "import ast,pathlib,sys; ast.parse(pathlib.Path(sys.argv[1]).read_bytes())" $source
 if($LASTEXITCODE -ne 0){throw 'Python source syntax failed. No files changed.'}
}
Write-Host "Confirmed Operations file: $target"
if(-not $Apply){Write-Host 'CHECK ONLY: no file changed, no process stopped.';exit 0}
$backup=Join-Path $destination ('backup-withdrawal-rules-'+(Get-Date -Format 'yyyyMMdd-HHmmss-fff'))
New-Item -ItemType Directory -Path $backup|Out-Null
Copy-Item -LiteralPath $target -Destination $backup
$oldModule=Join-Path $destination 'withdrawal_controls.py'
if(Test-Path -LiteralPath $oldModule){Copy-Item -LiteralPath $oldModule -Destination $backup}
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'withdrawal_controls.py') -Destination $oldModule -Force
$temp=$target+'.staged'
Copy-Item -LiteralPath (Join-Path $PSScriptRoot $name) -Destination $temp
Move-Item -LiteralPath $temp -Destination $target -Force
Write-Host "Installed files. Backup: $backup"
Write-Host 'Existing Operations process is still running old code. Restart only that Operations engine in its existing window.'
Write-Host 'Keep the DD supervisor and its four shards running. Do not start a duplicate Operations engine.'
Write-Host 'Custom caps remain blocked until broker operation is verified and NP_PLAN_WITHDRAWAL_VERIFIED is configured on the main API.'
