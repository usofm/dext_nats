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

function Quote-CmdArgument {
  param([string]$Value)
  return '"' + ($Value -replace '"', '""') + '"'
}

$rsvars = Find-RsVars -ExplicitRoot $BdsRoot
$parserMode = if ($ParserV2) { 'ParserV2' } else { 'ParserV1' }
$outputDir = Join-Path $repoRoot ("Output\{0}\{1}\{2}" -f $Platform, $Config, $parserMode)

Write-Host "Using RAD Studio environment: $rsvars"
Write-Host "Building: $project"
Write-Host "Config=$Config Platform=$Platform Parser=$parserMode"
Write-Host "Output=$outputDir"

if (Test-Path $outputDir) {
  Remove-Item $outputDir -Recurse -Force
}
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

# Keep parser modes physically isolated. This prevents a DCU compiled with one
# conditional define from being reused by the other validation build.
$properties = @(
  '/p:Config=' + $Config,
  '/p:Platform=' + $Platform,
  '/p:DCC_DcuOutput=' + (Quote-CmdArgument $outputDir),
  '/p:DCC_ExeOutput=' + (Quote-CmdArgument $outputDir)
)

if ($ParserV2) {
  # Command-line global properties replace the project property. Preserve the
  # conventional configuration symbol explicitly rather than relying on a
  # literal $(DCC_Define) reference supplied on the command line.
  $configDefine = if ($Config -eq 'Release') { 'RELEASE' } else { 'DEBUG' }
  $properties += '/p:DCC_Define=' + (Quote-CmdArgument ("DEXT_NATS_PARSER_V2;$configDefine"))
}

$propertyText = $properties -join ' '
$command = 'call "' + $rsvars + '" && msbuild "' + $project + '" /t:Rebuild ' + $propertyText
& cmd.exe /d /s /c $command
if ($LASTEXITCODE -ne 0) {
  throw "Delphi build failed with exit code $LASTEXITCODE"
}

$exe = Join-Path $outputDir 'Dext.Net.Nats.Tests.exe'
if (-not (Test-Path $exe)) {
  throw "Build completed but test executable was not found: $exe"
}

Write-Host "Build succeeded: $exe"
Write-Output $exe
