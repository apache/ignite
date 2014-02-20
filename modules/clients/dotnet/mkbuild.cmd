@echo OFF

rem Validate path to .Net installation.
IF NOT EXIST %DOTNET_PATH%\MSBuild.exe SET DOTNET_PATH=c:\Windows\Microsoft.NET\Framework\v4.0.30319
IF NOT EXIST %DOTNET_PATH%\MSBuild.exe GOTO INVALID_DOTNET_PATH

set PATH0=%PATH%
set PATH=%PATH0%;%DOTNET_PATH%

echo Switch to build script directory %~dp0
cd %~dp0

rem Build project.
MSBuild.exe dotnet.sln /t:Clean;Rebuild /p:Configuration=Release /val /m /nologo /ds

IF %ERRORLEVEL% NEQ 0 GOTO ERROR

set PATH=%PATH0%

rem Build distributions.
echo .
echo Copy client libraries into the distribution directory.

set TARGET_DIR=client-lib\libs
rmdir /S /Q %TARGET_DIR%
IF NOT EXIST %TARGET_DIR%\NUL mkdir %TARGET_DIR%

copy /Y client-lib\bin\Release\*.dll %TARGET_DIR%
copy /Y client-lib\bin\Release\*.xml %TARGET_DIR%
copy /Y client-lib\bin\Release\*.pdb %TARGET_DIR%

copy /Y readme.md %TARGET_DIR%
copy /Y readme.txt %TARGET_DIR%

rem rem Execute tests.
rem cd builds
rem ..\client-lib-test\bin\Release\GridGainTest.exe
rem cd ..
rem
rem IF NOT EXIST builds\TestResult.xml GOTO INVALID_TEST_RESULT

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
