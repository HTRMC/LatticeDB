@echo off
setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
set "COMPILER_DIR=%PROJECT_DIR%\compiler"
set "ZIG_DIR=%COMPILER_DIR%\zig"
set "ZIG_EXE=%ZIG_DIR%\zig.exe"
set "ZIGVERSION_FILE=%PROJECT_DIR%\.zigversion"

:: Read version from .zigversion file
if not exist "%ZIGVERSION_FILE%" (
    echo Error: .zigversion file not found
    exit /b 1
)

set /p VERSION=<"%ZIGVERSION_FILE%"
for /f "tokens=* delims= " %%a in ("%VERSION%") do set "VERSION=%%a"

echo Zig version from .zigversion: %VERSION%

:: Check if zig executable exists
if exist "%ZIG_EXE%" (
    echo Zig already installed at %ZIG_EXE%
    exit /b 0
)

:: Zig not found, download it
echo Zig not found. Downloading...

set "ARCH=x86_64"
set "OS=windows"
set "URL=https://ziglang.org/builds/zig-%ARCH%-%OS%-%VERSION%.zip"
set "ARCHIVE_NAME=zig-%ARCH%-%OS%-%VERSION%"
set "TEMP_FILE=%COMPILER_DIR%\zig-download.zip"

echo URL: %URL%

:: Create compiler directory
if not exist "%COMPILER_DIR%" mkdir "%COMPILER_DIR%"

:: Download using curl (skip if already downloaded)
if exist "%TEMP_FILE%" (
    echo Zip already downloaded, skipping download...
) else (
    echo Downloading...
    curl -L -o "%TEMP_FILE%" "%URL%"
    if errorlevel 1 (
        echo Error: Failed to download Zig
        exit /b 1
    )
)

:: Extract using tar
echo Extracting...
if exist "%ZIG_DIR%" rmdir /s /q "%ZIG_DIR%"
tar -xf "%TEMP_FILE%" -C "%COMPILER_DIR%"
if errorlevel 1 (
    echo Error: Failed to extract Zig
    exit /b 1
)

:: Rename extracted folder to 'zig'
rename "%COMPILER_DIR%\%ARCHIVE_NAME%" "zig"

:: Clean up
del /f "%TEMP_FILE%"

echo Zig installed successfully to %ZIG_DIR%
endlocal
