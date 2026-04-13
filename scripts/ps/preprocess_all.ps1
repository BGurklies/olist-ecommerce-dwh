param(
    [string]$SqlInstance = 'localhost'
)

$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# Query active RAW targets that need preprocessing (is_active = 1 + needs_preprocessing = 1)
# from orchestration.pipeline_config. Joins audit.load_log to retrieve the
# last SUCCESS timestamp — pipeline_config.last_run_ts is overwritten on each
# run, so load_log is the only reliable source for skip-logic.
# ---------------------------------------------------------------------------
function Get-PipeTargets {
    param([string]$Instance)

    $connStr = "Server=$Instance;Database=OlistDWH;Integrated Security=True;"
    $conn    = New-Object System.Data.SqlClient.SqlConnection($connStr)
    $conn.Open()

    $cmd = $conn.CreateCommand()
    $cmd.CommandText = "
        SELECT
            pc.file_path AS PipePath,
            MAX(CASE WHEN ll.status = 'SUCCESS' THEN ll.load_ts ELSE NULL END) AS LastSuccessTs
        FROM orchestration.pipeline_config pc
        LEFT JOIN audit.load_log ll
            ON ll.file_name = pc.file_name
        WHERE pc.layer              = 'RAW'
          AND pc.is_active          = 1
          AND pc.needs_preprocessing = 1
        GROUP BY pc.file_path
        ORDER BY pc.file_path;
        "

    $reader = $cmd.ExecuteReader()
    $rows   = @()
    while ($reader.Read()) {
        $pipePath = $reader['PipePath'].ToString()
        $lastTs   = if ($reader.IsDBNull(1)) { $null } else {
                        [DateTime]::SpecifyKind($reader.GetDateTime(1), [DateTimeKind]::Utc)
                    }
        $rows += New-Object PSObject -Property @{
            PipePath      = $pipePath
            SourcePath    = ($pipePath -replace '_pipe\.csv$', '.csv')
            LastSuccessTs = $lastTs
        }
    }
    $reader.Close()
    $conn.Close()
    $conn.Dispose()
    return $rows
}

# ---------------------------------------------------------------------------
# Worker scriptblock — runs in each runspace thread.
# Must be self-contained: runspaces do not inherit parent scope or modules.
# ---------------------------------------------------------------------------
$workerScript = {
    param(
        [string]$InputPath,
        [string]$OutputPath
    )

    Add-Type -AssemblyName Microsoft.VisualBasic

    $delimiter = '|'
    $encoding  = New-Object System.Text.UTF8Encoding($false)

    $parser = New-Object Microsoft.VisualBasic.FileIO.TextFieldParser($InputPath)
    $parser.TextFieldType = [Microsoft.VisualBasic.FileIO.FieldType]::Delimited
    $parser.SetDelimiters(',')
    $parser.HasFieldsEnclosedInQuotes = $true

    $writer         = New-Object System.IO.StreamWriter($OutputPath, $false, $encoding)
    $writer.NewLine = "`n"

    $rowCount = 0

    try {
        $headers = $parser.ReadFields()
        $writer.WriteLine($headers -join $delimiter)

        while (-not $parser.EndOfData) {
            $fields  = $parser.ReadFields()
            $rowCount++
            $cleaned = $fields | ForEach-Object {
                $_.Replace('|', ' ').Replace("`r", ' ').Replace("`n", ' ')
            }
            $writer.WriteLine($cleaned -join $delimiter)
        }

        $result = New-Object PSObject -Property @{
            File    = $InputPath
            Rows    = $rowCount
            Success = $true
            Error   = $null
        }
        return $result
    }
    catch {
        $result = New-Object PSObject -Property @{
            File    = $InputPath
            Rows    = $rowCount
            Success = $false
            Error   = $_.Exception.Message
        }
        return $result
    }
    finally {
        $parser.Close()
        $writer.Close()
    }
}

# ---------------------------------------------------------------------------
# Resolve targets: skip files unchanged since the last successful RAW load.
# ---------------------------------------------------------------------------
$allTargets = Get-PipeTargets -Instance $SqlInstance

$targets = @()
foreach ($t in $allTargets) {
    if (-not (Test-Path $t.SourcePath)) {
        throw "Source CSV not found: $($t.SourcePath)"
    }

    $csvModifiedUtc = (Get-Item $t.SourcePath).LastWriteTimeUtc
    $label          = [System.IO.Path]::GetFileName($t.SourcePath)

    if ($null -eq $t.LastSuccessTs -or $csvModifiedUtc -gt $t.LastSuccessTs) {
        $targets += $t
        Write-Output "Queued  : $label (CSV modified $($csvModifiedUtc.ToString('yyyy-MM-dd HH:mm:ss')) UTC)"
    }
    else {
        Write-Output "Skipped : $label (not modified since last load at $($t.LastSuccessTs.ToString('yyyy-MM-dd HH:mm:ss')) UTC)"
    }
}

if ($targets.Count -eq 0) {
    Write-Output 'All pipe files up-to-date - nothing to preprocess.'
    exit 0
}

# ---------------------------------------------------------------------------
# RunspacePool - bounded by the lesser of target count and logical CPU count.
# ---------------------------------------------------------------------------
$maxThreads = [Math]::Min($targets.Count, [Environment]::ProcessorCount)
$pool = [System.Management.Automation.Runspaces.RunspaceFactory]::CreateRunspacePool(1, $maxThreads)
$pool.Open()

Write-Output "Preprocessing $($targets.Count) file(s) - pool size: $maxThreads thread(s)"
$startAll = Get-Date

$jobs = @()
foreach ($target in $targets) {
    $ps = [System.Management.Automation.PowerShell]::Create()
    $ps.RunspacePool = $pool
    [void]$ps.AddScript($workerScript)
    [void]$ps.AddArgument($target.SourcePath)
    [void]$ps.AddArgument($target.PipePath)

    $handle = $ps.BeginInvoke()
    $jobs += New-Object PSObject -Property @{ PowerShell = $ps; Handle = $handle }
}

$anyFailed = $false
foreach ($job in $jobs) {
    $result = $job.PowerShell.EndInvoke($job.Handle)[0]
    $job.PowerShell.Dispose()

    if ($result.Success) {
        Write-Output "  OK  : $($result.Rows) rows - $($result.File)"
    }
    else {
        Write-Output "  FAIL: $($result.File) - $($result.Error)"
        $anyFailed = $true
    }
}

$pool.Close()
$pool.Dispose()

$elapsed = (Get-Date) - $startAll
Write-Output ("Done in {0:mm\:ss\.ff}" -f $elapsed)

if ($anyFailed) {
    throw 'One or more preprocessing jobs failed - see output above.'
}
