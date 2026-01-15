param (
    [Parameter(Mandatory = $true)]
    [string]$InputFile,

    [string]$OutputDir = "transactions",
    [int]$MaxParametersPerTransaction = 65000,

    [switch]$DryRun,
    [switch]$Resume,
    [switch]$ForceUtf8,

    [string]$StateFile = "state.json",
    [string]$JsonLog   = "transactions.json"
)

# =========================================================
# Encoding detection
# =========================================================
function Detect-Encoding {
    param ([string]$File)

    if ($ForceUtf8) {
        Write-Host "Encoding: forced UTF-8"
        return New-Object System.Text.UTF8Encoding($false)
    }

    $bytes = [System.IO.File]::ReadAllBytes($File)

    if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
        Write-Host "Encoding: UTF-8 BOM"
        return New-Object System.Text.UTF8Encoding($true)
    }

    if ($bytes.Length -ge 2) {
        if ($bytes[0] -eq 0xFF -and $bytes[1] -eq 0xFE) {
            Write-Host "Encoding: UTF-16 LE"
            return [System.Text.Encoding]::Unicode
        }
        if ($bytes[0] -eq 0xFE -and $bytes[1] -eq 0xFF) {
            Write-Host "Encoding: UTF-16 BE"
            return [System.Text.Encoding]::BigEndianUnicode
        }
    }

    try {
        [System.Text.Encoding]::UTF8.GetString($bytes) | Out-Null
        Write-Host "Encoding: UTF-8 (no BOM)"
        return New-Object System.Text.UTF8Encoding($false)
    }
    catch {
        Write-Host "Encoding: fallback Windows-1251"
        return [System.Text.Encoding]::GetEncoding(1251)
    }
}

# =========================================================
# Helpers
# =========================================================
function Count-ValuesGroups {
    param ([string]$Sql)

    # only after VALUES
    if ($Sql -notmatch 'VALUES\s*(.+)$') {
        return 0
    }

    $text = $matches[1]

    $groups = 0
    $depth = 0
    $inString = $false
    $inDollar = $false

    for ($i = 0; $i -lt $text.Length; $i++) {
        $c = $text[$i]

        if (-not $inString -and $text.Substring($i) -match '^\$[a-zA-Z0-9_]*\$') {
            $inDollar = -not $inDollar
            continue
        }

        if ($inDollar) { continue }

        if ($c -eq "'") {
            $inString = -not $inString
            continue
        }

        if ($inString) { continue }

        if ($c -eq '(') {
            if ($depth -eq 0) { $groups++ }
            $depth++
            continue
        }

        if ($c -eq ')') {
            $depth--
            continue
        }
    }

    return $groups
}

function Count-Columns-In-ValuesTuple {
    param ([string]$Text)

    $count = 1
    $depth = 0
    $inString = $false
    $inDollar = $false

    for ($i = 0; $i -lt $Text.Length; $i++) {
        $c = $Text[$i]

        if (-not $inString -and $Text.Substring($i) -match '^\$[a-zA-Z0-9_]*\$') {
            $inDollar = -not $inDollar
            continue
        }

        if ($inDollar) { continue }

        if ($c -eq "'") {
            $inString = -not $inString
            continue
        }

        if ($inString) { continue }

        if ($c -eq '(') { $depth++; continue }
        if ($c -eq ')') { $depth--; continue }

        if ($c -eq ',' -and $depth -eq 0) {
            $count++
        }
    }
    return $count
}

function Is-End-Of-Insert {
    param ([string]$Text)

    $inString = $false
    $inDollar = $false

    for ($i = 0; $i -lt $Text.Length; $i++) {
        $c = $Text[$i]

        if (-not $inString -and $Text.Substring($i) -match '^\$[a-zA-Z0-9_]*\$') {
            $inDollar = -not $inDollar
            continue
        }

        if ($inDollar) { continue }

        if ($c -eq "'") {
            $inString = -not $inString
            continue
        }

        if (-not $inString -and $c -eq ';') {
            return $true
        }
    }
    return $false
}

# =========================================================
# Init
# =========================================================
if (-not (Test-Path $InputFile)) {
    Write-Error "Input file not found"
    exit 1
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

$encoding = Detect-Encoding $InputFile
$reader = New-Object System.IO.StreamReader($InputFile, $encoding)

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

$transactionNumber = 1
$currentBatch = @()
$currentParams = 0
$report = @()

$insideInsert = $false
$insertBuffer = @()

# =========================================================
function Save-Transaction {

    if ($script:currentBatch.Count -eq 0) { return }

    Write-Host "Transaction $script:transactionNumber : $script:currentParams params"

    if (-not $DryRun) {
        $file = Join-Path $OutputDir ("transaction_{0:D4}.sql" -f $script:transactionNumber)

        $writer = New-Object System.IO.StreamWriter(
            $file,
            $false,
            $script:utf8NoBom
        )

        try {
            $writer.WriteLine("BEGIN;")
            foreach ($line in $script:currentBatch) {
                $writer.WriteLine($line)
            }
            $writer.WriteLine("COMMIT;")
        }
        finally {
            $writer.Close()
        }
    }

    $script:transactionNumber++
    $script:currentBatch  = @()
    $script:currentParams = 0
}


# =========================================================
# Main loop
# =========================================================
$fileSize = (Get-Item $InputFile).Length
$bytesRead = 0
$lastProgressUpdate = Get-Date
$progressIntervalMs = 150   # update ~6 times per sec

while (-not $reader.EndOfStream) {
    $line = $reader.ReadLine()

    # (approximately, stable tho)
    $bytesRead += $encoding.GetByteCount($line) + 1

    $now = Get-Date
    if (($now - $lastProgressUpdate).TotalMilliseconds -ge $progressIntervalMs) {
        $percent = [math]::Min(100, ($bytesRead / $fileSize) * 100)

        Write-Progress `
            -Activity "Processing SQL dump" `
            -Status ("Transactions: {0}, Params in current: {1}" -f `
                      $script:transactionNumber, $script:currentParams) `
            -PercentComplete $percent

        $lastProgressUpdate = $now
    }

    if (-not $insideInsert) {
        if ($line -match '^\s*INSERT\s+INTO') {
            $insideInsert = $true
            $insertBuffer = @($line)
        }
        else {
            $currentBatch += $line
        }
    }
    else {
        $insertBuffer += $line
    }

    if ($insideInsert -and (Is-End-Of-Insert $line)) {
        $insideInsert = $false
        $sql = ($insertBuffer -join "`n")

        if ($sql -notmatch 'VALUES\s*\((.*)\)') {
            Write-Error "Cannot parse VALUES"
            exit 1
        }

        $tuple = $matches[1]
        $cols  = Count-Columns-In-ValuesTuple $tuple
        $groups = Count-ValuesGroups $sql
        $params = $cols * $groups

        if ($params -gt $MaxParametersPerTransaction) {
            Write-Error "Single INSERT exceeds parameter limit: $params"
            exit 1
        }

        if (($currentParams + $params) -gt $MaxParametersPerTransaction) {
            Save-Transaction
        }

        $currentBatch += $sql
        $currentParams += $params
        $insertBuffer = @()
    }
}

$reader.Close()
Save-Transaction
Write-Progress -Activity "Processing SQL dump" -Completed

$report | ConvertTo-Json -Depth 3 | Set-Content $JsonLog -Encoding UTF8

if ($DryRun) {
    Write-Host "DryRun: no files created"
}

Write-Host "Done. Transactions: $($transactionNumber - 1)"
