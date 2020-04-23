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

@echo off
Setlocal EnableDelayedExpansion

if "%OS%" == "Windows_NT"  setlocal

:: Check IGNITE_HOME.
pushd "%~dp0"
set IGNITE_HOME=%CD%

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

for /f "tokens=1,2 delims=." %%a in ("%JAVA_VER_STR%.x") do set MAJOR_JAVA_VER=%%a & set MINOR_JAVA_VER=%%b
if %MAJOR_JAVA_VER% == 1 set MAJOR_JAVA_VER=%MINOR_JAVA_VER%

if %MAJOR_JAVA_VER% LSS 8 (
    echo %0, ERROR:
    echo The version of JAVA installed in %JAVA_HOME% is incorrect.
    echo Please point JAVA_HOME variable to installation of JDK 1.8 or later.
    echo You can also download latest JDK at http://java.com/download.
	goto error_finish
)

:run_java

::
:: JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
::
:: ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
::
"%JAVA_HOME%\bin\java.exe" -version 2>&1 | findstr "1\.[7]\." > nul
if %ERRORLEVEL% equ 0 (
    if "%JVM_OPTS%" == "" set JVM_OPTS=-Xms1g -Xmx1g -server -XX:MaxPermSize=256m
) else (
    if "%JVM_OPTS%" == "" set JVM_OPTS=-Xms1g -Xmx1g -server -XX:MaxMetaspaceSize=256m
)

:: https://confluence.atlassian.com/kb/basic-authentication-fails-for-outgoing-proxy-in-java-8u111-909643110.html
set JVM_OPTS=%JVM_OPTS% -Djava.net.useSystemProxies=true -Djdk.http.auth.tunneling.disabledSchemes=

::
:: Final JVM_OPTS for Java 9+ compatibility
::
if "%MAJOR_JAVA_VER%" == "8" (
    set JVM_OPTS= ^
    -XX:+AggressiveOpts ^
    %JVM_OPTS%
)

if %MAJOR_JAVA_VER% GEQ 9 if %MAJOR_JAVA_VER% LSS 11 (
    set JVM_OPTS= ^
    -XX:+AggressiveOpts ^
    --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED ^
    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED ^
    --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED ^
    --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED ^
    --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED ^
    --illegal-access=permit ^
    --add-modules=java.xml.bind ^
    %JVM_OPTS%
)

if "%MAJOR_JAVA_VER%" GEQ "11" (
    set JVM_OPTS= ^
    --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED ^
    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED ^
    --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED ^
    --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED ^
    --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED ^
    --illegal-access=permit ^
    %JVM_OPTS%
)

"%JAVA_HOME%\bin\java.exe" %JVM_OPTS% -cp "*" org.apache.ignite.console.agent.AgentLauncher %*

set JAVA_ERRORLEVEL=%ERRORLEVEL%

:: errorlevel 130 if aborted with Ctrl+c
if %JAVA_ERRORLEVEL%==130 goto eof

:error_finish

if not "%NO_PAUSE%" == "1" pause

goto :eof

