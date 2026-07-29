@echo off
setlocal
cd /d "%~dp0"

powershell.exe -NoProfile -ExecutionPolicy Bypass ^
  -File "%~dp0scripts\test-pulseguard-public-release-e2e.ps1" ^
  -ProjectRoot "%~dp0" ^
  -ConfirmDataLoss ^
  -CleanAfterTest

set "EXIT_CODE=%ERRORLEVEL%"
echo.
if not "%EXIT_CODE%"=="0" (
  echo PulseGuard Docker E2E failed with exit code %EXIT_CODE%.
  echo Review the failure diagnostics ZIP in your Downloads folder.
  pause
  exit /b %EXIT_CODE%
)

echo PulseGuard Docker E2E passed.
echo Review the validation report in your Downloads folder.
pause
