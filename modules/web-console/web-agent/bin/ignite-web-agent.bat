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

:: Check JAVA_HOME.
if defined JAVA_HOME  goto checkJdk
    echo %0, ERROR:
    echo JAVA_HOME environment variable is not found.
    echo Please point JAVA_HOME variable to location of JDK 1.7 or JDK 1.8.
    echo You can also download latest JDK at http://java.com/download.
goto error_finish

:checkJdk
:: Check that JDK is where it should be.
if exist "%JAVA_HOME%\bin\java.exe" goto checkJdkVersion
    echo %0, ERROR:
    echo JAVA is not found in JAVA_HOME=%JAVA_HOME%.
    echo Please point JAVA_HOME variable to installation of JDK 1.7 or JDK 1.8.
    echo You can also download latest JDK at http://java.com/download.
goto error_finish

:checkJdkVersion
"%JAVA_HOME%\bin\java.exe" -version 2>&1 | findstr "1\.[78]\." > nul
if %ERRORLEVEL% equ 0 goto run_java
    echo %0, ERROR:
    echo The version of JAVA installed in %JAVA_HOME% is incorrect.
    echo Please point JAVA_HOME variable to installation of JDK 1.7 or JDK 1.8.
    echo You can also download latest JDK at http://java.com/download.
goto error_finish

:run_java

::
:: JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
::
:: ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
::
"%JAVA_HOME%\bin\java.exe" -version 2>&1 | findstr "1\.[7]\." > nul
if %ERRORLEVEL% equ 0 (
    if "%JVM_OPTS%" == "" set JVM_OPTS=-Xms1g -Xmx1g -server -XX:+AggressiveOpts -XX:MaxPermSize=256m
) else (
    if "%JVM_OPTS%" == "" set JVM_OPTS=-Xms1g -Xmx1g -server -XX:+AggressiveOpts -XX:MaxMetaspaceSize=256m
)

set JVM_OPTS=%JVM_OPTS% -Djava.net.useSystemProxies=true

"%JAVA_HOME%\bin\java.exe" %JVM_OPTS% -cp "*" org.apache.ignite.console.agent.AgentLauncher %*

set JAVA_ERRORLEVEL=%ERRORLEVEL%

:: errorlevel 130 if aborted with Ctrl+c
if %JAVA_ERRORLEVEL%==130 goto eof

:error_finish

if not "%NO_PAUSE%" == "1" pause

goto :eof

