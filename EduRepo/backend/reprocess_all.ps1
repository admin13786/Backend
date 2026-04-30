$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

$dbPath = Join-Path $PSScriptRoot "data\\edurepo.db"
if (!(Test-Path $dbPath)) { throw "DB not found: $dbPath" }

$ts = Get-Date -Format "yyyyMMdd-HHmmss"
$bak = Join-Path $PSScriptRoot ("data\\edurepo.db.bak-" + $ts)
Copy-Item $dbPath $bak -Force
Write-Host ("Backup: " + $bak)

Write-Host "Resetting processed fields -> pending..."
python reset_all_to_pending.py

Write-Host ""
Write-Host "Reprocessing pending items with LLM (batch size=20, concurrency=2)..."
python reprocess_pending.py
