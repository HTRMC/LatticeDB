@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
set "ZIG_EXE=%PROJECT_DIR%\compiler\zig\zig.exe"

:: Ensure zig is installed
if not exist "%ZIG_EXE%" (
    echo Zig not found. Running install script...
    call "%SCRIPT_DIR%install.bat"
    if errorlevel 1 exit /b 1
)

:: Run zig with arguments, default to "build run"
if "%~1"=="" (
    "%ZIG_EXE%" build run
) else (
    "%ZIG_EXE%" %*
)

endlocal
