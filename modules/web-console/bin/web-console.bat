::
:: Copyright 2019 GridGain Systems, Inc. and Contributors.
::
:: Licensed under the GridGain Community Edition License (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
::
::     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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

:: Check IGNITE_HOME.
:checkIgniteHome1
pushd "%~dp0"
set IGNITE_HOME=%CD%
popd
goto :checkIgniteHome2

:checkIgniteHome2
set WEB_CONSOLE_AGENT=%IGNITE_HOME%\agent_dists\*

if not exist "%WEB_CONSOLE_AGENT%" (
    echo Web Console installation folder is incorrect.
    echo Please run web-console.bat from Web Console installation folder.
    goto :eof
)

goto run_java

:removeTrailingSlash
set IGNITE_HOME=%IGNITE_HOME:~0,-1%
goto checkIgniteHome2

:run_java

::
:: Final JVM_OPTS for Java 9+ compatibility
::
if %MAJOR_JAVA_VER% == 8 (
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
    --add-modules=java.transaction ^
    --add-modules=java.xml.bind ^
    %JVM_OPTS%
)

if %MAJOR_JAVA_VER% GEQ 11 (
    set JVM_OPTS= ^
    --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED ^
    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED ^
    --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED ^
    --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED ^
    --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED ^
    --illegal-access=permit ^
    %JVM_OPTS%
)

for %%f in (ignite-web-console-*.jar) do set "JAR_FILE=%%f" & goto :run

:run
"%JAVA_HOME%\bin\java.exe" -DIGNITE_HOME="%IGNITE_HOME%" %JVM_OPTS% -jar "%JAR_FILE%"

:finish
