@echo off
setlocal

echo ==========================================
echo      IBRX Record Counter
echo ==========================================

:: 1. Check New Filings
set "file=out\sec_new_filings.csv"
if exist "%file%" (
    <nul set /p="New Filings: "
    findstr /i "^IBRX," "%file%" | find /c /v ""
) else (
    echo New Filings: FILE NOT FOUND
)

:: 2. Check Worklist
set "file=out\sec_worklist.csv"
if exist "%file%" (
    <nul set /p="Worklist:    "
    findstr /i "^IBRX," "%file%" | find /c /v ""
) else (
    echo Worklist:    FILE NOT FOUND
)

:: 3. Check Events
set "file=out\sec_events.csv"
if exist "%file%" (
    <nul set /p="Events:      "
    findstr /i "^IBRX," "%file%" | find /c /v ""
) else (
    echo Events:      FILE NOT FOUND
)

echo ==========================================
pause