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
:: Starts H2 database server and console for Ignite Schema Import demo.
::

@echo off

if "%OS%" == "Windows_NT"  setlocal

:: Check JAVA_HOME.
if not "%JAVA_HOME%" == "" goto checkJdk
    echo %0, ERROR: JAVA_HOME environment variable is not found.
    echo %0, ERROR: Please create JAVA_HOME variable pointing to location of JDK 1.7 or JDK 1.8.
    echo %0, ERROR: You can also download latest JDK at: http://java.sun.com/getjava
goto error_finish

:checkJdk
:: Check that JDK is where it should be.
if exist "%JAVA_HOME%\bin\java.exe" goto checkJdkVersion
    echo %0, ERROR: The JDK is not found in %JAVA_HOME%.
    echo %0, ERROR: Please modify your script so that JAVA_HOME would point to valid location of JDK.
goto error_finish

:checkJdkVersion
"%JAVA_HOME%\bin\java.exe" -version 2>&1 | findstr "1\.[78]\." > nul
if %ERRORLEVEL% equ 0 goto checkIgniteHome1
    echo %0, ERROR: The version of JAVA installed in %JAVA_HOME% is incorrect.
    echo %0, ERROR: Please install JDK 1.7 or 1.8.
    echo %0, ERROR: You can also download latest JDK at: http://java.sun.com/getjava
goto error_finish

:: Check IGNITE_HOME.
:checkIgniteHome1
if not "%IGNITE_HOME%" == "" goto checkIgniteHome2
    pushd "%~dp0"/../../..
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
if exist "%IGNITE_HOME%\config" goto run
    echo %0, ERROR: Ignite installation folder is not found or IGNITE_HOME environment variable is not valid.
    echo Please create IGNITE_HOME environment variable pointing to location of
    echo Ignite installation folder.
    goto error_finish

:run

:: Starts H2 server and console.
"%JAVA_HOME%\bin\java.exe" -cp "%IGNITE_HOME%\libs\ignite-indexing\h2-1.4.191.jar" org.h2.tools.Console %*

:error_finish