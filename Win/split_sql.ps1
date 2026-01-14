# Init
param(
    [Parameter(Mandatory=$true)]
    [string]$InputFile,
    
    [string]$OutputDir = "transactions",
    
    [int]$MaxParametersPerTransaction = 64500  # Keep the stock up to the limit
)

function Show-Help {
    Write-Host "How use: .\split_sql.ps1 -InputFile <input_file.sql> [options]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -InputFile       Path to input SQL-file (required)"
    Write-Host "  -OutputDir       Directory for output files (default: 'transactions')"
    Write-Host "  -MaxParameters   Max parameters count in one transaction (default: 64500)"
    Write-Host ""
    exit
}

function Count-Parameters {
    param($sqlContent)
    
    $totalParameters = 0
    $inInsert = $false
    
    $sqlContent | ForEach-Object {
        $line = $_.Trim()
        
        # Skip empty strings and comments
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith('--')) { return }
        
        if ($line -cmatch '^INSERT\s+INTO') {
            $inInsert = $true
        }
        
        if ($inInsert) {
            # Find values in brackets after VALUES
            if ($line -match 'VALUES\s*\((.*)\)') {
                $values = $matches[1]
                
                # splitting
                $valueList = $values -split '(?:\s*,\s*)(?![^()]*\))'
                
                foreach ($value in $valueList) {
                    $trimmedValue = $value.Trim()
                    
                    if ($trimmedValue -eq 'NULL') {
                        $totalParameters++
                    }
                    elseif ($trimmedValue.StartsWith("'") -and $trimmedValue.EndsWith("'")) {
                        $totalParameters++
                    }
                    elseif ($trimmedValue -match '^-?\d+(\.\d+)?$') {
                        $totalParameters++
                    }
                    elseif ($trimmedValue -match '^(TRUE|FALSE)$') {
                        $totalParameters++
                    }
                }
            }
            
            # End of INSERT
            if ($line.EndsWith(';')) {
                $inInsert = $false
            }
        }
    }
    
    return $totalParameters
}


# Params checking
if (-not (Test-Path $InputFile)) {
    Write-Host "ERROR: file '$InputFile' was not found."
    Show-Help
}

# Create output directory
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

# Calc param count
$firstInsertParams = 0
$inFirstInsert = $true
$currentBatch = @()
$totalParameters = 0
$transactionCount = 1
$insertCount = 0  # counter of INSERT in curr transaction

Get-Content $InputFile | ForEach-Object {
    $line = $_.Trim()
    
    if ($inFirstInsert -and $line -cmatch '^INSERT\s+INTO') {
        if ($line -match '\(([^)]*)\)') {
            $params = $matches[1].Split(',') | ForEach-Object { $_.Trim() }
            $firstInsertParams = Count-Parameters (Get-Content $InputFile)
            $inFirstInsert = $false
        }
    }
}

if ($firstInsertParams -eq 0) {
    Write-Host "ERROR: couldn't calc params num in first INSERT."
    exit 1
}

$maxInsertsPerTransaction = [math]::Floor($MaxParametersPerTransaction / $firstInsertParams)

# Main execution
$inInsert = $false  # is in INSERT
$currentInsert = @()  # buffer for current INSERT

Get-Content $InputFile | ForEach-Object {
    $line = $_.Trim()

    if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith('--')) {
        return
    }

    # new INSERT row
    if ($line -cmatch '^INSERT\s+INTO') {
        if ($inInsert) {
            # Done previous before start new INSERT
            $currentBatch += $currentInsert
            $insertCount++
            $totalParameters += $firstInsertParams
            $currentInsert = @()
        }
        $inInsert = $true
        $currentInsert += $line
    }
    elseif ($inInsert -and $line.EndsWith(';')) {
        $currentInsert += $line
        $currentBatch += $currentInsert
        $insertCount++
        $totalParameters += $firstInsertParams
        $currentInsert = @()
        $inInsert = $false

        # Check param limit
        if ($totalParameters -ge $MaxParametersPerTransaction) {
            $outputFile = Join-Path $OutputDir ("transaction_{0:D4}.sql" -f $transactionCount)
            
            Set-Content -Path $outputFile -Value "BEGIN;"
            Add-Content -Path $outputFile -Value $currentBatch
            Add-Content -Path $outputFile -value "COMMIT;"
            
            Write-Host "Saved: $outputFile (INSERT: $insertCount, params: $totalParameters)"
            
            # flush buffer
            $currentBatch = @()
            $transactionCount++
            $totalParameters = 0
            $insertCount = 0
        }
    }
    # inside INSERT
    elseif ($inInsert) {
        $currentInsert += $line
    }
    # out of insert just copying
    else {
        $currentBatch += $line
    }
}

# Save remaining
if ($currentBatch.Count -gt 0 -or $currentInsert.Count -gt 0) {
    if ($currentInsert.Count -gt 0) {
        $currentBatch += $currentInsert
        $insertCount++
        $totalParameters += $firstInsertParams
    }

    $outputFile = Join-Path $OutputDir ("transaction_{0:D4}.sql" -f $transactionCount)
    
    Set-Content -Path $outputFile -Value "BEGIN;"
    Add-Content -Path $outputFile -Value $currentBatch
    Add-Content -Path $outputFile -Value "COMMIT;"
    
    Write-Host "Saved: $outputFile (remaining, INSERT: $insertCount, params: $totalParameters)"
}

Write-Host "Done. Transactions was created: $transactionCount"
Write-Host "Params num in first INSERT: $firstInsertParams"
Write-Host "Max INSERT number in one transaction: $maxInsertsPerTransaction"
