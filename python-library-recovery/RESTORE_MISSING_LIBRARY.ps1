param([string]$PythonRoot = (Join-Path $env:LOCALAPPDATA 'Programs\Python\Python311'))
$ErrorActionPreference = 'Stop'
$python = Join-Path $PythonRoot 'python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw 'Expected Python executable not found. No files changed.' }
$version = [Diagnostics.FileVersionInfo]::GetVersionInfo($python).ProductVersion
if ($version -notmatch '^3\.11\.0(?:\s|$)') { throw "This recovery is only for Python 3.11.0. Found: $version. No files changed." }
foreach ($name in @('python311.dll','DLLs\_socket.pyd','DLLs\_ssl.pyd','DLLs\_ctypes.pyd')) {
    if (-not (Test-Path -LiteralPath (Join-Path $PythonRoot $name) -PathType Leaf)) { throw "Native Python component missing: $name. A complete installer repair is needed. No files changed." }
}
$manifest = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'LIBRARY_SHA256.json') -Raw | ConvertFrom-Json
$prepared = @()
foreach ($entry in $manifest.PSObject.Properties) {
    $relative = [string]$entry.Name
    if ($relative -notmatch '^Lib/' -or $relative.Contains('\') -or $relative.Contains(':') -or $relative.Split('/') -contains '..') { throw 'Invalid library manifest path. No files changed.' }
    $source = Join-Path $PSScriptRoot $relative
    if (-not (Test-Path -LiteralPath $source -PathType Leaf) -or (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant() -ne $entry.Value) { throw "Library checksum failed: $relative. No files changed." }
    $destination = Join-Path $PythonRoot $relative
    if (-not (Test-Path -LiteralPath $destination)) { $prepared += [PSCustomObject]@{Source=$source;Destination=$destination;Relative=$relative} }
}
Write-Host "Confirmed Python $version at $PythonRoot"
Write-Host ('Verified library files. Missing files to restore: ' + $prepared.Count)
Write-Host 'Existing files, third-party packages, running processes, tasks and monitoring state are preserved.'
$created = New-Object 'System.Collections.Generic.List[string]'
$receipt = Join-Path $PSScriptRoot ('restored-files-' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff') + '.json')
try {
    foreach ($entry in $prepared) {
        $parent = Split-Path -Parent $entry.Destination
        if (-not (Test-Path -LiteralPath $parent)) { New-Item -ItemType Directory -Path $parent -Force | Out-Null }
        $temporary = Join-Path $parent ('.np-library-' + [guid]::NewGuid().ToString('N') + '.tmp')
        try {
            [IO.File]::Copy($entry.Source,$temporary,$false)
            # File.Move without an overwrite option refuses a newly existing destination.
            if (-not (Test-Path -LiteralPath $entry.Destination)) {
                [IO.File]::Move($temporary,$entry.Destination)
                $created.Add($entry.Relative)
            }
        } finally {
            if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary -Force }
        }
    }
} finally {
    [IO.File]::WriteAllText($receipt, (ConvertTo-Json -InputObject @($created.ToArray())))
    Write-Host ('Restored missing files: ' + $created.Count + '. Receipt: ' + $receipt)
}
Push-Location -LiteralPath $PythonRoot
try {
    & $python -I -c "import sys, encodings, os, ssl, socket, ctypes; assert sys.version_info[:3] == (3,11,0); print('PYTHON STANDARD LIBRARY OK'); print(sys.version); print(sys.prefix)"
    if ($LASTEXITCODE -ne 0) { throw 'Restored files are recorded, but Python still needs attention. Keep existing monitors running and share this output.' }
    & $python -c "import requests, MetaTrader5 as m; assert callable(getattr(m,'initialize',None)); print('REQUESTS AND MT5 IMPORT OK - no broker session started')"
    if ($LASTEXITCODE -ne 0) { throw 'Python starts, but monitoring dependencies need attention. No monitoring process was restarted.' }
} finally { Pop-Location }
Write-Host 'RECOVERY CHECKS PASSED. No reboot or monitoring restart was performed.'
Write-Host 'Send this result before retrying the trailing update.'
