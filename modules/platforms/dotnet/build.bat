@echo OFF

rem uncomment the following if having problems with Microsoft.Cpp.Default.props
rem SET VCTargetsPath=C:\Program Files (x86)\MSBuild\Microsoft.Cpp\v4.0\V120

set PLATFORM=%1
if [%PLATFORM%]==[] set PLATFORM=x64

set TARGET_DIR=bin
if not [%PLATFORM%]==[x64] set TARGET_DIR=%TARGET_DIR%\%PLATFORM%

rem Validate path to .Net installation.
IF NOT EXIST %DOTNET_PATH%\MSBuild.exe SET DOTNET_PATH=c:\Windows\Microsoft.NET\Framework\v4.0.30319
IF NOT EXIST %DOTNET_PATH%\MSBuild.exe GOTO INVALID_DOTNET_PATH

set PATH0=%PATH%
set PATH=%PATH0%;%DOTNET_PATH%

echo Switch to build script directory %~dp0
cd %~dp0

rem Build project.
echo .
echo Build project for platform: %PLATFORM%
MSBuild.exe Apache.Ignite.sln /t:Clean;Rebuild /p:Configuration=Release /p:Platform=%PLATFORM% /val /m /nologo /ds

IF %ERRORLEVEL% NEQ 0 GOTO ERROR

set PATH=%PATH0%

rem Build distributions.
echo .
echo Copy client libraries into the distribution directory: %TARGET_DIR%

rmdir /S /Q %TARGET_DIR%
IF NOT EXIST %TARGET_DIR%\NUL mkdir %TARGET_DIR%

copy /Y Apache.Ignite\bin\%PLATFORM%\Release\*.* %TARGET_DIR%

copy /Y readme.txt %TARGET_DIR%

goto DONE

:INVALID_DOTNET_PATH
echo DOTNET_PATH=%DOTNET_PATH% is invalid path to .Net installation.

set ERRORLEVEL=1
goto ERROR

:INVALID_TEST_RESULT
echo No test results generated during tests execution.

set ERRORLEVEL=1

goto ERROR

:ERROR
set _ERRORLVL=%ERRORLEVEL%

echo Breaked due to upper errors with exit code: %_ERRORLVL%

echo ON

@exit /b %_ERRORLVL%

:DONE

echo.
echo Done!

:END

echo ON
