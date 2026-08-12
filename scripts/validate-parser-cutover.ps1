param(
  [ValidateSet('Debug', 'Release')]
  [string]$Config = 'Release',

  [ValidateSet('Win32', 'Win64')]
  [string]$Platform = 'Win32',

  [string]$BdsRoot = '',

  [switch]$LiveNats,
  [switch]$Benchmark
)

$ErrorActionPreference = 'Stop'

function Invoke-TestExe {
  param(
    [string]$Exe,
    [string]$ParserName,
    [bool]$RunLive,
    [bool]$RunBenchmark
  )

  Write-Host ''
  Write-Host "=== $ParserName tests ==="

  $oldRequireLive = $env:DEXT_NATS_REQUIRE_LIVE
  $oldSkipLive = $env:DEXT_NATS_SKIP_LIVE
  $oldBench = $env:DEXT_NATS_RUN_BENCH
  try {
    if ($RunLive) {
      $env:DEXT_NATS_REQUIRE_LIVE = '1'
      Remove-Item Env:DEXT_NATS_SKIP_LIVE -ErrorAction SilentlyContinue
    } else {
      $env:DEXT_NATS_SKIP_LIVE = '1'
      Remove-Item Env:DEXT_NATS_REQUIRE_LIVE -ErrorAction SilentlyContinue
    }

    if ($RunBenchmark) {
      $env:DEXT_NATS_RUN_BENCH = '1'
    } else {
      Remove-Item Env:DEXT_NATS_RUN_BENCH -ErrorAction SilentlyContinue
    }

    & $Exe
    if ($LASTEXITCODE -ne 0) {
      throw "$ParserName tests failed with exit code $LASTEXITCODE"
    }
  }
  finally {
    if ($null -eq $oldRequireLive) { Remove-Item Env:DEXT_NATS_REQUIRE_LIVE -ErrorAction SilentlyContinue }
    else { $env:DEXT_NATS_REQUIRE_LIVE = $oldRequireLive }

    if ($null -eq $oldSkipLive) { Remove-Item Env:DEXT_NATS_SKIP_LIVE -ErrorAction SilentlyContinue }
    else { $env:DEXT_NATS_SKIP_LIVE = $oldSkipLive }

    if ($null -eq $oldBench) { Remove-Item Env:DEXT_NATS_RUN_BENCH -ErrorAction SilentlyContinue }
    else { $env:DEXT_NATS_RUN_BENCH = $oldBench }
  }
}

$common = @('-Config', $Config, '-Platform', $Platform)
if ($BdsRoot) { $common += @('-BdsRoot', $BdsRoot) }

Write-Host 'Dext.Nats parser runtime cutover validation'
Write-Host "Config=$Config Platform=$Platform LiveNats=$($LiveNats.IsPresent) Benchmark=$($Benchmark.IsPresent)"

$v1Exe = & (Join-Path $PSScriptRoot 'build-tests.ps1') @common
if (-not $v1Exe) { throw 'Parser V1 build did not return an executable path.' }
Invoke-TestExe -Exe $v1Exe -ParserName 'Parser V1 runtime' -RunLive $LiveNats.IsPresent -RunBenchmark $Benchmark.IsPresent

$v2Args = $common + '-ParserV2'
$v2Exe = & (Join-Path $PSScriptRoot 'build-tests.ps1') @v2Args
if (-not $v2Exe) { throw 'Parser V2 build did not return an executable path.' }
Invoke-TestExe -Exe $v2Exe -ParserName 'Parser V2 runtime' -RunLive $LiveNats.IsPresent -RunBenchmark $Benchmark.IsPresent

Write-Host ''
Write-Host 'Parser cutover compile/test gate PASSED for both V1 and V2.'
Write-Host "V1: $v1Exe"
Write-Host "V2: $v2Exe"
