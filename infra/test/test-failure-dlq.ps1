$ErrorActionPreference = "Stop"

$baseUrl = "http://localhost:7071/api"
$pk = "demo-user"

Write-Host ""
Write-Host "=== WS1-09 Failure + DLQ demo ==="
Write-Host "This script will create a failing job and keep polling its status."
Write-Host "Press Ctrl + C to stop."
Write-Host ""

# 1) Create failing job
$body = @{
  pk = $pk
  type = "csv_cleaning_validation"
  parameters = @{
    fail = $true
  }
} | ConvertTo-Json -Depth 5

Write-Host "Creating failing job..."

$res = Invoke-RestMethod `
  -Method Post `
  -Uri "$baseUrl/jobs" `
  -ContentType "application/json" `
  -Body $body

$jobId = $res.jobId

if (-not $jobId) {
  throw "jobId not returned by POST /jobs"
}

Write-Host "Job created successfully."
Write-Host "jobId = $jobId"
Write-Host ""

# 2) Poll job status forever
while ($true) {
  try {
    $job = Invoke-RestMethod `
      -Method Get `
      -Uri "$baseUrl/jobs/$($jobId)?pk=$pk"

    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

    $status = $job.status
    $attempts = $job.attempts
    $progress = $job.progress

    $errorCode = $null
    $errorMessage = $null
    $errorStage = $null

    if ($job.error) {
      $errorCode = $job.error.code
      $errorMessage = $job.error.message
      $errorStage = $job.error.stage
    }

    Write-Host "[$timestamp] status=$status attempts=$attempts progress=$progress"

    if ($errorCode -or $errorMessage -or $errorStage) {
      Write-Host "           error.code=$errorCode"
      Write-Host "           error.stage=$errorStage"
      Write-Host "           error.message=$errorMessage"
    }

    Write-Host ""
  }
  catch {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] Polling error: $($_.Exception.Message)"
    Write-Host ""
  }

  Start-Sleep -Seconds 3
}