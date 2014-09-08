::
:: Copyright (C) GridGain Systems. All Rights Reserved.
:: _________        _____ __________________        _____
:: __  ____/___________(_)______  /__  ____/______ ____(_)_______
:: _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
:: / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
:: \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
::
:: Version: 6.2.1
::

::
:: Starts Visor Dashboard with GridGain on the classpath.
::

@echo off

if "%OS%" == "Windows_NT"  setlocal

:: Check JAVA_HOME.
if not "%JAVA_HOME%" == "" goto checkJdk
    echo %0, ERROR: JAVA_HOME environment variable is not found.
    echo %0, ERROR: Please create JAVA_HOME variable pointing to location of JDK 1.7 or JDK 1.8.
    echo %0, ERROR: You can also download latest JDK at: http://java.sun.com/getjava
    exit 1

:checkJdk
:: Check that JDK is where it should be.
if exist "%JAVA_HOME%\bin\java.exe" goto checkJdkVersion
    echo %0, ERROR: The JDK is not found in %JAVA_HOME%.
    echo %0, ERROR: Please modify your script so that JAVA_HOME would point to valid location of JDK.
    exit 1

:checkJdkVersion
"%JAVA_HOME%\bin\java.exe" -version 2>&1 | findstr "1\.[78]\." > nul
if %ERRORLEVEL% equ 0 goto checkGridGainHome1
    echo %0, ERROR: The version of JAVA installed in %JAVA_HOME% is incorrect.
    echo %0, ERROR: Please install JDK 1.7 or 1.8.
    echo %0, ERROR: You can also download latest JDK at: http://java.sun.com/getjava
    exit 1

:: Check GRIDGAIN_HOME.
:checkGridGainHome1
if not "%GRIDGAIN_HOME%" == "" goto checkGridGainHome2
    pushd "%~dp0"/..
    set GRIDGAIN_HOME=%CD%
    popd

:checkGridGainHome2
:: Strip double quotes from GRIDGAIN_HOME
set GRIDGAIN_HOME=%GRIDGAIN_HOME:"=%

:: remove all trailing slashes from GRIDGAIN_HOME.
if %GRIDGAIN_HOME:~-1,1% == \ goto removeTrailingSlash
if %GRIDGAIN_HOME:~-1,1% == / goto removeTrailingSlash
goto checkGridGainHome3

:removeTrailingSlash
set GRIDGAIN_HOME=%GRIDGAIN_HOME:~0,-1%
goto checkGridGainHome2

:checkGridGainHome3
if exist "%GRIDGAIN_HOME%\config" goto checkGridGainHome4
    echo %0, ERROR: GridGain installation folder is not found or GRIDGAIN_HOME environment variable is not valid.
    echo Please create GRIDGAIN_HOME environment variable pointing to location of
    echo GridGain installation folder.
    exit 1

:checkGridGainHome4
if /i "%GRIDGAIN_HOME%\bin\" == "%~dp0" goto run
    echo %0, WARN: GRIDGAIN_HOME environment variable may be pointing to wrong folder: %GRIDGAIN_HOME%

:run

::
:: Set GRIDGAIN_LIBS
::
call "%GRIDGAIN_HOME%\bin\include\setenv.bat"


::
:: Remove slf4j, log4j libs from classpath for hadoop edition, because they already exist in hadoop.
::
if exist "%HADOOP_COMMON_HOME%" (
    for /f %%f in ('dir /B %GRIDGAIN_HOME%\bin\include\visorui') do call :concat %%f

    goto cp
)

set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%GRIDGAIN_HOME%\bin\include\visorui\*

:cp
set CP=%GRIDGAIN_LIBS%

::
:: Parse command line parameters.
::
call "%GRIDGAIN_HOME%\bin\include\parseargs.bat" %*
if %ERRORLEVEL% neq 0 (
    echo Arguments parsing failed
    exit /b %ERRORLEVEL%
)

::
:: Set program name.
::
set PROG_NAME=gridgain.bat
if "%OS%" == "Windows_NT" set PROG_NAME=%~nx0%

::
:: JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
::
:: ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
::
if "%JVM_OPTS_VISOR%" == "" set JVM_OPTS_VISOR=-Xms1g -Xmx1g

:: Uncomment to set preference to IPv4 stack.
:: set JVM_OPTS_VISOR=%JVM_OPTS_VISOR% -Djava.net.preferIPv4Stack=true

:: Force to use OpenGL
:: set JVM_OPTS_VISOR=%JVM_OPTS_VISOR% -Dsun.java2d.opengl=True

::
:: Set Visor plugins directory.
::
set VISOR_PLUGINS_DIR=%GRIDGAIN_HOME%\bin\include\visorui\plugins

::
:: Starts Visor Dashboard.
::
"%JAVA_HOME%\bin\java.exe" %JVM_OPTS_VISOR% -DGRIDGAIN_PROG_NAME="%PROG_NAME%" ^
-DGRIDGAIN_PERFORMANCE_SUGGESTIONS_DISABLED=true %QUIET% %JVM_XOPTS% ^
-Dpf4j.pluginsDir="%VISOR_PLUGINS_DIR%" ^
-cp "%CP%" org.gridgain.visor.gui.VisorGuiLauncher

exit %ERRORLEVEL%

goto :eof

:concat
set file=%1

if %file:~-0,5% neq slf4j if %file:~-0,5% neq log4j if %file% neq licenses if %file% neq plugins ^
set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%GRIDGAIN_HOME%\bin\include\visorui\%1

goto :eof
