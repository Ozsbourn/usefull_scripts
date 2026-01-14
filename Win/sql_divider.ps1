param(
    [Parameter(Mandatory=$true)]
    [string]$InputFile,
    
    [string]$OutputDir = "transactions",
    
    [int]$MaxParametersPerTransaction = 65000
)

function Show-Help {
    Write-Host "Использование: .\split_sql.ps1 -InputFile <входной_файл.sql> [опции]"
    Write-Host ""
    Write-Host "Опции:"
    Write-Host "  -InputFile       Путь к входному SQL-файлу (обязательно)"
    Write-Host "  -OutputDir       Директория для выходных файлов (по умолчанию: 'transactions')"
    Write-Host "  -MaxParameters   Максимальное количество параметров в транзакции (по умолчанию: 65000)"
    Write-Host ""
    exit
}

# Проверка параметров
if (-not (Test-Path $InputFile)) {
    Write-Host "Ошибка: файл '$InputFile' не найден."
    Show-Help
}

# Создаем выходную директорию
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

# Определяем количество параметров в первой INSERT
$firstInsertParams = 0
$inFirstInsert = $true
$currentBatch = @()
$totalParameters = 0
$transactionCount = 1

# Проходим по файлу для определения параметров в первой INSERT
Get-Content $InputFile | ForEach-Object {
    $line = $_
    
    if ($inFirstInsert) {
        if ($line -cmatch '^\s*INSERT\s+INTO') {
            # Ищем параметры в скобках
            if ($line -match '\(([^)]*)\)') {
                $params = $matches[1].Split(',') | ForEach-Object { $_.Trim() }
                $firstInsertParams = $params.Count
                $inFirstInsert = $false
            }
        }
    }
}

if ($firstInsertParams -eq 0) {
    Write-Host "Ошибка: не удалось определить параметры в первой INSERT команде."
    exit 1
}

# Рассчитываем максимальное количество INSERT в транзакции
$maxInsertsPerTransaction = [math]::Floor($MaxParametersPerTransaction / $firstInsertParams)

# Основной процесс обработки
Get-Content $InputFile | ForEach-Object {
    $line = $_
    
    # Проверяем начало нового INSERT
    if ($line -cmatch '^\s*INSERT\s+INTO') {
        $currentBatch += $line
        $totalParameters += $firstInsertParams
    }
    else {
        $currentBatch += $line
    }

    # Проверяем конец INSERT
    if ($line -notmatch '^\s*INSERT\s+INTO' -and $currentBatch.Count -gt 0) {
        if ($totalParameters -ge $MaxParametersPerTransaction) {
            $outputFile = Join-Path $OutputDir ("transaction_{0:D4}.sql" -f $transactionCount)
            
            Set-Content -Path $outputFile -Value "BEGIN;"
            Add-Content -Path $outputFile -Value $currentBatch
            Add-Content -Path $outputFile -Value "COMMIT;"
            
            Write-Host "Сохранено: $outputFile (параметров: $totalParameters)"
            
            # Очищаем буфер
            $currentBatch = @()
            $transactionCount++
            $totalParameters = 0
        }
    }
}

# Сохраняем оставшуюся часть
if ($currentBatch.Count -gt 0) {
    $outputFile = Join-Path $OutputDir ("transaction_{0:D4}.sql" -f $transactionCount)
    
    Set-Content -Path $outputFile -Value "BEGIN;"
    Add-Content -Path $outputFile -Value $currentBatch
    Add-Content -Path $outputFile -Value "COMMIT;"
    
    Write-Host "Сохранено: $outputFile (остаток, параметров: $totalParameters)"
}

Write-Host "Готово. Создано транзакций: $transactionCount"
Write-Host "Параметры в первой INSERT: $firstInsertParams"
Write-Host "Максимальное количество INSERT в транзакции: $maxInsertsPerTransaction"
