# Build + Test + Run + Benchmark — one-click cho Windows
$ErrorActionPreference = "Stop"

Write-Host "=== [1/5] Configure CMake ===" -ForegroundColor Cyan
cmake -B build -DCMAKE_BUILD_TYPE=Release
if ($LASTEXITCODE -ne 0) { throw "CMake configure failed" }

Write-Host "=== [2/5] Build Release ===" -ForegroundColor Cyan
$cores = [Environment]::ProcessorCount
cmake --build build --config Release --parallel $cores
if ($LASTEXITCODE -ne 0) { throw "Build failed" }

Write-Host "=== [3/5] Run Unit Tests ===" -ForegroundColor Cyan
Push-Location build
ctest -C Release --output-on-failure
$testResult = $LASTEXITCODE
Pop-Location
if ($testResult -ne 0) { Write-Host "WARNING: Some tests failed" -ForegroundColor Yellow }

Write-Host "=== [4/5] Run IoT App Demo (10s) ===" -ForegroundColor Cyan
if (Test-Path "dashboard\data.csv") { Remove-Item "dashboard\data.csv" }
& ".\build\Release\iot_app.exe"

Write-Host "=== [5/5] Run Benchmark ===" -ForegroundColor Cyan
& ".\build\Release\run_benchmarks.exe"

Write-Host "=== DONE ===" -ForegroundColor Green
Write-Host "CSV output: dashboard\data.csv"
Write-Host "Chạy dashboard: cd dashboard; pip install -r requirements.txt; python dashboard.py"

