param(
  [string]$Version = '2.11.6',
  [string]$Destination = (Join-Path $PSScriptRoot '..\.tools')
)

$ErrorActionPreference = 'Stop'

$archiveName = "nats-server-v$Version-windows-amd64.zip"
$downloadUrl = "https://github.com/nats-io/nats-server/releases/download/v$Version/$archiveName"
$archivePath = Join-Path $Destination $archiveName
$extractPath = Join-Path $Destination "nats-server-v$Version-windows-amd64"

New-Item -ItemType Directory -Path $Destination -Force | Out-Null

Write-Host "Downloading NATS Server v$Version..."
Invoke-WebRequest -Uri $downloadUrl -OutFile $archivePath

if (Test-Path $extractPath) {
  Remove-Item $extractPath -Recurse -Force
}

Expand-Archive -Path $archivePath -DestinationPath $Destination -Force
Remove-Item $archivePath -Force

$exe = Join-Path $extractPath 'nats-server.exe'
if (-not (Test-Path $exe)) {
  throw "nats-server.exe was not found after extracting $archiveName"
}

Write-Host "NATS Server ready: $exe"
Write-Output $exe
