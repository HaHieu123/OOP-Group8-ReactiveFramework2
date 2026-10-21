Remove-Item -Recurse -Force build -ErrorAction SilentlyContinue
Remove-Item -Force dashboard\data.csv -ErrorAction SilentlyContinue
Write-Host "Cleaned build/ and dashboard/data.csv" -ForegroundColor Green

