param(
    [string]$Database = "moex_stock.db",
    [string]$Tickers = "GAZP",
    [string]$Start = "2010-01-01",
    [string]$End = ""
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$sqlitePath = Join-Path $projectRoot "sqlite3.exe"

if (-not (Test-Path $sqlitePath)) {
    throw "sqlite3.exe not found: $sqlitePath"
}

function Invoke-SqlScript {
    param([string]$ScriptName)

    $scriptPath = Join-Path $projectRoot $ScriptName
    if (-not (Test-Path $scriptPath)) {
        throw "SQL script not found: $scriptPath"
    }

    Write-Host "Running $ScriptName"
    & $sqlitePath $Database ".read $scriptPath"
    if ($LASTEXITCODE -ne 0) {
        throw "sqlite3 failed while running $ScriptName"
    }
}

function Get-PythonCommand {
    $candidates = @(
        @{ File = "python"; Args = @("--version") },
        @{ File = "py"; Args = @("-3", "--version") },
        @{ File = "py"; Args = @("--version") }
    )

    foreach ($candidate in $candidates) {
        try {
            & $candidate.File @($candidate.Args) *> $null
            if ($LASTEXITCODE -eq 0) {
                return $candidate
            }
        }
        catch {
        }
    }

    throw "Python launcher not found. Install Python or run load_moex_to_sqlite.py manually."
}

$pythonCommand = Get-PythonCommand
$pythonArgs = @()

if ($pythonCommand.File -eq "py" -and $pythonCommand.Args.Count -gt 0 -and $pythonCommand.Args[0] -eq "-3") {
    $pythonArgs += "-3"
}

$pythonArgs += @(
    (Join-Path $projectRoot "load_moex_to_sqlite.py"),
    "--db", $Database,
    "--tickers", $Tickers,
    "--start", $Start
)

if ($End) {
    $pythonArgs += @("--end", $End)
}

Write-Host "Recreating database $Database"
Invoke-SqlScript -ScriptName "drop_all.sql"
Invoke-SqlScript -ScriptName "schema.sql"

Write-Host "Loading raw data"
& $pythonCommand.File @pythonArgs
if ($LASTEXITCODE -ne 0) {
    throw "Raw layer load failed"
}

Invoke-SqlScript -ScriptName "staging.sql"
Invoke-SqlScript -ScriptName "mart.sql"

Write-Host "Project build completed"
