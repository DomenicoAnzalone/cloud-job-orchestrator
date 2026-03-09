# 1) Crea job
$body = @{
  pk = "demo-user"
  type = "csv_cleaning_validation"
  parameters = @{
    delimiter = ","
    trimWhitespace = $true
  }
} | ConvertTo-Json -Depth 5

$res = Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:7071/api/jobs" `
  -ContentType "application/json" `
  -Body $body

$jobId = $res.jobId
Write-Host "jobId: $jobId"

# 2) Poll finché non è done
do {
  Start-Sleep -Seconds 5
  $status = Invoke-RestMethod `
    -Method Get `
    -Uri "http://localhost:7071/api/jobs/$($jobId)?pk=demo-user"

  Write-Host ("status={0} progress={1}" -f $status.status, $status.progress)
} while ($status.status -ne "done")

# 3) Chiedi output-link
$linkRes = Invoke-RestMethod `
  -Method Get `
  -Uri "http://localhost:7071/api/jobs/$($jobId)/output-link?pk=demo-user"

$linkRes
$linkRes.downloadUrl

# 4) Apri il link nel browser
Start-Process $linkRes.downloadUrl