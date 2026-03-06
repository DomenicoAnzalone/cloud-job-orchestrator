param(
  [string]$BaseUrl = "http://localhost:7071",
  [string]$Pk = "demo-user",
  [string]$Type = "csv_cleaning_validation",
  [int]$PollCount = 30,
  [int]$PollIntervalSeconds = 2
)

$body = @{
  pk = $Pk
  type = $Type
  parameters = @{
    delimiter = ","
    trimWhitespace = $true
  }
} | ConvertTo-Json -Depth 5

Write-Host "Creating job..."
$res = Invoke-RestMethod `
  -Method Post `
  -Uri "$BaseUrl/api/jobs" `
  -ContentType "application/json" `
  -Body $body

$jobId = $res.jobId.ToString().Trim()
$url = "$($res.statusUrl)?pk=$Pk"

Write-Host "jobId: $jobId"
Write-Host "statusUrl: $url"
Write-Host ""

1..$PollCount | ForEach-Object {
  Write-Host "Poll #$_"
  $status = Invoke-RestMethod -Method Get -Uri $url
  $status | ConvertTo-Json -Depth 10
  Write-Host ""
  Start-Sleep -Seconds $PollIntervalSeconds
}