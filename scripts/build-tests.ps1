param(
  [ValidateSet('Debug', 'Release')]
  [string]$Config = 'Debug',

  [ValidateSet('Win32', 'Win64')]
  [string]$Platform = 'Win32',

  [string]$BdsRoot = '',

  [switch]$ParserV2
)

$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$project = Join-Path $repoRoot 'Tests\Dext.Net.Nats.Tests.dproj'

function Find-RsVars {
  param([string]$ExplicitRoot)

  if ($ExplicitRoot) {
    $candidate = Join-Path $ExplicitRoot 'bin\rsvars.bat'
    if (Test-Path $candidate) { return (Resolve-Path $candidate).Path }
    throw "rsvars.bat not found under BdsRoot: $ExplicitRoot"
  }

  $studioRoot = 'C:\Program Files (x86)\Embarcadero\Studio'
  if (-not (Test-Path $studioRoot)) {
    throw "RAD Studio installation root not found: $studioRoot"
  }

  $candidates = Get-ChildItem $studioRoot -Directory |
    Sort-Object { [version]$_.Name } -Descending |
    ForEach-Object { Join-Path $_.FullName 'bin\rsvars.bat' } |
    Where-Object { Test-Path $_ }

  $found = $candidates | Select-Object -First 1
  if (-not $found) {
    throw 'No RAD Studio rsvars.bat was found.'
  }
  return (Resolve-Path $found).Path
}

$rsvars = Find-RsVars -ExplicitRoot $BdsRoot
Write-Host "Using RAD Studio environment: $rsvars"
Write-Host "Building: $project"
Write-Host "Config=$Config Platform=$Platform ParserV2=$($ParserV2.IsPresent)"

$properties = '/p:Config=' + $Config + ' /p:Platform=' + $Platform
if ($ParserV2) {
  # Global MSBuild property is intentional for this validation build. It selects
  # Dext.Net.Nats.Internal.ParserSelector without changing source files.
  $properties += ' /p:DCC_Define=DEXT_NATS_PARSER_V2'
}

$command = 'call "' + $rsvars + '" && msbuild "' + $project + '" /t:Build ' + $properties
& cmd.exe /d /s /c $command
if ($LASTEXITCODE -ne 0) {
  throw "Delphi build failed with exit code $LASTEXITCODE"
}

$exe = Join-Path $repoRoot ("Output\{0}\{1}\Dext.Net.Nats.Tests.exe" -f $Platform, $Config)
if (-not (Test-Path $exe)) {
  throw "Build completed but test executable was not found: $exe"
}

Write-Host "Build succeeded: $exe"
Write-Output $exe
