::
:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to You under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License.  You may obtain a copy of the License at
::
::      http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::
::
:: The utility for offline analysis index.bin and optionally partitions.
::

@echo off
Setlocal EnableDelayedExpansion

if "%OS%" == "Windows_NT"  setlocal

:: Check JAVA_HOME.
if defined JAVA_HOME  goto checkJdk
    echo %0, ERROR:
    echo JAVA_HOME environment variable is not found.
    echo Please point JAVA_HOME variable to location of JDK 1.8 or later.
    echo You can also download latest JDK at http://java.com/download.
goto error_finish

:checkJdk
:: Check that JDK is where it should be.
if exist "%JAVA_HOME%\bin\java.exe" goto checkJdkVersion
    echo %0, ERROR:
    echo JAVA is not found in JAVA_HOME=%JAVA_HOME%.
    echo Please point JAVA_HOME variable to installation of JDK 1.8 or later.
    echo You can also download latest JDK at http://java.com/download.
goto error_finish

:checkJdkVersion
set cmd="%JAVA_HOME%\bin\java.exe"
for /f "tokens=* USEBACKQ" %%f in (`%cmd% -version 2^>^&1`) do (
    set var=%%f
    goto :LoopEscape
)
:LoopEscape

for /f "tokens=1-3  delims= " %%a in ("%var%") do set JAVA_VER_STR=%%c
set JAVA_VER_STR=%JAVA_VER_STR:"=%

for /f "tokens=1,2 delims=." %%a in ("%JAVA_VER_STR%.x") do set MAJOR_JAVA_VER=%%a& set MINOR_JAVA_VER=%%b
if %MAJOR_JAVA_VER% == 1 set MAJOR_JAVA_VER=%MINOR_JAVA_VER%

if %MAJOR_JAVA_VER% LSS 8 (
    echo %0, ERROR:
    echo The version of JAVA installed in %JAVA_HOME% is incorrect.
    echo Please point JAVA_HOME variable to installation of JDK 1.8 or later.
    echo You can also download latest JDK at http://java.com/download.
    goto error_finish
)

:: Check IGNITE_HOME.
:checkIgniteHome1
if defined IGNITE_HOME goto checkIgniteHome2
    pushd "%~dp0"/..
    set IGNITE_HOME=%CD%
    popd

:checkIgniteHome2
:: Strip double quotes from IGNITE_HOME
set IGNITE_HOME=%IGNITE_HOME:"=%

:: remove all trailing slashes from IGNITE_HOME.
if %IGNITE_HOME:~-1,1% == \ goto removeTrailingSlash
if %IGNITE_HOME:~-1,1% == / goto removeTrailingSlash
goto checkIgniteHome3

:removeTrailingSlash
set IGNITE_HOME=%IGNITE_HOME:~0,-1%
goto checkIgniteHome2

:checkIgniteHome3
if exist "%IGNITE_HOME%\config" goto checkIgniteHome4
    echo %0, ERROR: Ignite installation folder is not found or IGNITE_HOME environment variable is not valid.
    echo Please create IGNITE_HOME environment variable pointing to location of
    echo Ignite installation folder.
    goto error_finish

:checkIgniteHome4

::
:: Set SCRIPTS_HOME - base path to scripts.
::
set SCRIPTS_HOME=%IGNITE_HOME%\bin

:: Remove trailing spaces
for /l %%a in (1,1,31) do if /i "%SCRIPTS_HOME:~-1%" == " " set SCRIPTS_HOME=%SCRIPTS_HOME:~0,-1%

if /i "%SCRIPTS_HOME%\" == "%~dp0" goto setProgName
    echo %0, WARN: IGNITE_HOME environment variable may be pointing to wrong folder: %IGNITE_HOME%

:setProgName
::
:: Set program name.
::
set PROG_NAME=ignite.bat
if "%OS%" == "Windows_NT" set PROG_NAME=%~nx0%

:run

::
:: Set IGNITE_LIBS
::
call "%SCRIPTS_HOME%\include\setenv.bat"
call "%SCRIPTS_HOME%\include\build-classpath.bat"
set CP=%IGNITE_LIBS%

::
:: Process 'restart'.
::
set RANDOM_NUMBER_COMMAND="!JAVA_HOME!\bin\java.exe" -cp %CP% org.apache.ignite.startup.cmdline.CommandLineRandomNumberGenerator
for /f "usebackq tokens=*" %%i in (`!RANDOM_NUMBER_COMMAND!`) do set RANDOM_NUMBER=%%i

set RESTART_SUCCESS_FILE="%IGNITE_HOME%\work\ignite_success_%RANDOM_NUMBER%"
set RESTART_SUCCESS_OPT=-DIGNITE_SUCCESS_FILE=%RESTART_SUCCESS_FILE%

::
:: JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
::
:: ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
::
"%JAVA_HOME%\bin\java.exe" -version 2>&1 | findstr "1\.[7]\." > nul
if %ERRORLEVEL% equ 0 (
    if "%CONTROL_JVM_OPTS%" == "" set CONTROL_JVM_OPTS=-Xms256m -Xmx1g
) else (
    if "%CONTROL_JVM_OPTS%" == "" set CONTROL_JVM_OPTS=-Xms256m -Xmx1g
)

::
:: Uncomment if you get StackOverflowError.
:: On 64 bit systems this value can be larger, e.g. -Xss16m
::
:: set CONTROL_JVM_OPTS=%CONTROL_JVM_OPTS% -Xss4m

::
:: Assertions are disabled by default since version 3.5.
:: If you want to enable them - set 'ENABLE_ASSERTIONS' flag to '1'.
::
set ENABLE_ASSERTIONS=1

::
:: Set '-ea' options if assertions are enabled.
::
if %ENABLE_ASSERTIONS% == 1 set CONTROL_JVM_OPTS=%CONTROL_JVM_OPTS% -ea

:run_java

::
:: Set main class to start service (grid node by default).
::

if "%MAIN_CLASS%" == "" set MAIN_CLASS=org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader

::
:: Final CONTROL_JVM_OPTS for Java 9+ compatibility
::
call "%SCRIPTS_HOME%\include\jvmdefaults.bat" %MAJOR_JAVA_VER% "%CONTROL_JVM_OPTS%" CONTROL_JVM_OPTS

if defined JVM_OPTS (
    echo JVM_OPTS environment variable is set, but will not be used. To pass JVM options use CONTROL_JVM_OPTS
    echo JVM_OPTS=%JVM_OPTS%
)

"%JAVA_HOME%\bin\java.exe" %CONTROL_JVM_OPTS% %QUIET% %RESTART_SUCCESS_OPT% ^
    -DIGNITE_UPDATE_NOTIFIER=false -DIGNITE_HOME="%IGNITE_HOME%" -DIGNITE_PROG_NAME="%PROG_NAME%" %JVM_XOPTS% ^
    -cp "%CP%" %MAIN_CLASS% %*

set JAVA_ERRORLEVEL=%ERRORLEVEL%

:: errorlevel 130 if aborted with Ctrl+c
if %JAVA_ERRORLEVEL%==130 goto finish

:: Exit if first run unsuccessful (Loader must create file).
if not exist %RESTART_SUCCESS_FILE% goto error_finish
del %RESTART_SUCCESS_FILE%

goto run_java

:finish
if not exist %RESTART_SUCCESS_FILE% goto error_finish
del %RESTART_SUCCESS_FILE%

:error_finish

if not "%NO_PAUSE%" == "1" pause

goto :eof
