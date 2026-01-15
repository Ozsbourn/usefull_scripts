param(
    [ValidateSet("off","hdd","ssd")]
    [string]$parallel = "off",
    [switch]$traceSql,
    [switch]$seedSafe
)

# ================= CONFIG =================
$SQL_DIR = ".\transactions"
$LOG_FILE = "execution.log"

$DB_HOST = "localhost" # Important!: anyway leave localhost
$DB_PORT = "5432"
$DB_NAME = "YOUR_DB_NAME"
$DB_USER = "YOUR_DB_USER"
$DB_PASS = "YOUR_DB_PASS"

$env:PGPASSWORD = $DB_PASS
$ErrorActionPreference = "Stop"
$ProgressPreference = "Continue"

# ================= LOG =================
function Log($msg) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $LOG_FILE -Value "[$ts] $msg"
}

Remove-Item $LOG_FILE -ErrorAction SilentlyContinue

# ================= POSTGRES DETECT =================
$PG_MODE = "LOCAL"
$PG_CONTAINER = $null

if (-not (Get-Command psql -ErrorAction SilentlyContinue)) {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "Neither psql nor docker found"
    }

    $PG_CONTAINER = docker ps --format '{{.Names}} {{.Image}}' |
        Where-Object { $_ -match 'postgres' } |
        Select-Object -First 1 |
        ForEach-Object { ($_ -split ' ')[0] }

    if (-not $PG_CONTAINER) {
        throw "PostgreSQL container not found"
    }

    $PG_MODE = "DOCKER"
}

Log "PostgreSQL mode: $PG_MODE"
if ($PG_CONTAINER) { Log "Container: $PG_CONTAINER" }

# ================= PSQL WRAPPER =================
function Invoke-PsqlFile {
    param([string]$File)

    if ($traceSql) { Log "[TRACE] Executing $File" }

    $tmpErr = New-TemporaryFile

    if ($PG_MODE -eq "LOCAL") {
        & psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME `
            -v ON_ERROR_STOP=0 -f $File 2> $tmpErr
        $rc = $LASTEXITCODE
    }
    else {
        docker cp $File "${PG_CONTAINER}:/tmp/run.sql" | Out-Null
        docker exec $PG_CONTAINER sh -c `
            "psql -U $DB_USER -d $DB_NAME -v ON_ERROR_STOP=0 -f /tmp/run.sql 2>/tmp/psql.err"
        $rc = $LASTEXITCODE
        docker cp "${PG_CONTAINER}:/tmp/psql.err" $tmpErr | Out-Null
        docker exec $PG_CONTAINER rm -f /tmp/psql.err | Out-Null
    }

    $stderr = Get-Content $tmpErr -Raw
    Remove-Item $tmpErr

    return @{ ExitCode = $rc; Stderr = $stderr }
}

# ================= EXECUTION =================
$files = Get-ChildItem $SQL_DIR -Filter *.sql | Sort-Object Name
$total = $files.Count
$i = 0

foreach ($file in $files) {
    $i++
    Write-Progress `
        -Activity "Executing SQL transactions" `
        -Status "$i / $total : $($file.Name)" `
        -PercentComplete (($i/$total)*100)

    Log "START $($file.Name)"

    $res = Invoke-PsqlFile $file.FullName

    if ($res.ExitCode -ne 0) {
        if ($seedSafe -and $res.Stderr -match 'duplicate key value violates unique constraint') {
            Log "WARNING duplicate key in $($file.Name) — partial apply"
            continue
        }

        Log "FAILED $($file.Name)"
        Log $res.Stderr
        throw "Execution failed on $($file.Name)"
    }

    Log "DONE $($file.Name)"
}

Write-Progress -Activity "Executing SQL transactions" -Completed
Log "ALL DONE"
