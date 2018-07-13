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
:: Starts Ignite Visor Console.
::

@echo off
Setlocal EnableDelayedExpansion

if "%OS%" == "Windows_NT"  setlocal

:: Check JAVA_HOME.
if defined JAVA_HOME  goto checkJdk
    echo %0, ERROR:
    echo JAVA_HOME environment variable is not found.
    echo Please point JAVA_HOME variable to location of JDK 1.8 or JDK 9.
    echo You can also download latest JDK at http://java.com/download.
goto error_finish

:checkJdk
:: Check that JDK is where it should be.
if exist "%JAVA_HOME%\bin\java.exe" goto checkJdkVersion
    echo %0, ERROR:
    echo JAVA is not found in JAVA_HOME=%JAVA_HOME%.
    echo Please point JAVA_HOME variable to installation of JDK 1.8 or JDK 9.
    echo You can also download latest JDK at http://java.com/download.
goto error_finish

:checkJdkVersion
set cmd="%JAVA_HOME%\bin\java.exe"
for /f "tokens=* USEBACKQ" %%f in (`%cmd% -version 2^>^&1`) do (
    set var=%%f
    goto :LoopEscape
)
:LoopEscape

set var=%var:~14%
set var=%var:"=%
for /f "tokens=1,2 delims=." %%a in ("%var%") do set MAJOR_JAVA_VER=%%a & set MINOR_JAVA_VER=%%b

if %MAJOR_JAVA_VER% == 1 set MAJOR_JAVA_VER=%MINOR_JAVA_VER%

if %MAJOR_JAVA_VER% LSS 8 (
    echo %0, ERROR:
    echo The %MAJOR_JAVA_VER% version of JAVA installed in %JAVA_HOME% is incorrect.
    echo Please point JAVA_HOME variable to installation of JDK 1.8 or JDK 9.
    echo You can also download latest JDK at http://java.com/download.
	goto error_finish
)

if %MAJOR_JAVA_VER% GTR 9 (
	echo %0, WARNING:
    echo The %MAJOR_JAVA_VER% version of JAVA installed in %JAVA_HOME% was not tested with Apache Ignite.
    echo Run it on your own risk or point JAVA_HOME variable to installation of JDK 1.8 or JDK 9.
    echo You can also download latest JDK at http://java.com/download.
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
set PROG_NAME=ignitevisorcmd.bat
if "%OS%" == "Windows_NT" set PROG_NAME=%~nx0%

::
:: Set IGNITE_LIBS
::
call "%SCRIPTS_HOME%\include\setenv.bat"
call "%SCRIPTS_HOME%\include\build-classpath.bat" &:: Will be removed in the binary release.
set CP=%IGNITE_HOME%\bin\include\visor-common\*;%IGNITE_HOME%\bin\include\visorcmd\*;%IGNITE_LIBS%

::
:: Parse command line parameters.
::
call "%SCRIPTS_HOME%\include\parseargs.bat" %*
if %ERRORLEVEL% neq 0 (
    echo Arguments parsing failed
    exit /b %ERRORLEVEL%
)

::
:: JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
::
:: ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
::
if "%JVM_OPTS_VISOR%" == "" set JVM_OPTS_VISOR=-Xms1g -Xmx1g -XX:MaxPermSize=128M

::
:: Uncomment to set preference to IPv4 stack.
::
:: set JVM_OPTS_VISOR=%JVM_OPTS_VISOR% -Djava.net.preferIPv4Stack=true

::
:: Assertions are disabled by default since version 3.5.
:: If you want to enable them - set 'ENABLE_ASSERTIONS' flag to '1'.
::
set ENABLE_ASSERTIONS=1

::
:: Set '-ea' options if assertions are enabled.
::
if %ENABLE_ASSERTIONS% == 1 set JVM_OPTS_VISOR=%JVM_OPTS_VISOR% -ea

::
:: Program args.
::
if "%ARGS%" == "" set ARGS=%*

::
:: Final JVM_OPTS for Java 9+ compatibility
::
if %MAJOR_JAVA_VER% GEQ 9 set JVM_OPTS=--add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED --add-exports jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-modules java.xml.bind %JVM_OPTS%

::
:: Starts Visor console.
::
"%JAVA_HOME%\bin\java.exe" %JVM_OPTS_VISOR% -DIGNITE_PROG_NAME="%PROG_NAME%" ^
-DIGNITE_DEPLOYMENT_MODE_OVERRIDE=ISOLATED %QUIET% %JVM_XOPTS% -cp "%CP%" ^
 org.apache.ignite.visor.commands.VisorConsole %ARGS%

:error_finish

if not "%NO_PAUSE%" == "1" pause

goto :eof
