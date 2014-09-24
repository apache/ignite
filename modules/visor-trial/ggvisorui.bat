::
:: @bat.file.header
:: _________        _____ __________________        _____
:: __  ____/___________(_)______  /__  ____/______ ____(_)_______
:: _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
:: / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
:: \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
::
:: Version: @bat.file.version
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
goto error_finish

:checkJdk
:: Check that JDK is where it should be.
if exist "%JAVA_HOME%\bin\java.exe" goto checkJdkVersion
    echo %0, ERROR: The JDK is not found in %JAVA_HOME%.
    echo %0, ERROR: Please modify your script so that JAVA_HOME would point to valid location of JDK.
goto error_finish

:checkJdkVersion
"%JAVA_HOME%\bin\java.exe" -version 2>&1 | findstr "1\.[78]\." > nul
if %ERRORLEVEL% equ 0 goto checkGridGainHome1
    echo %0, ERROR: The version of JAVA installed in %JAVA_HOME% is incorrect.
    echo %0, ERROR: Please install JDK 1.7 or 1.8.
    echo %0, ERROR: You can also download latest JDK at: http://java.sun.com/getjava
goto error_finish

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
    goto error_finish

:checkGridGainHome4

::
:: Set SCRIPTS_HOME - base path to scripts.
::
set SCRIPTS_HOME=%GRIDGAIN_HOME%\os\bin :: Will be replaced by SCRIPTS_HOME=${GRIDGAIN_HOME_TMP}\bin in release.

if /i "%SCRIPTS_HOME%\" == "%~dp0" goto run
    echo %0, WARN: GRIDGAIN_HOME environment variable may be pointing to wrong folder: %GRIDGAIN_HOME%

:run

::
:: Set GRIDGAIN_LIBS
::
call "%SCRIPTS_HOME%\include\setenv.bat"
call "%SCRIPTS_HOME%\include\target-classpath.bat" :: Will be removed in release.
set CP=%GRIDGAIN_HOME%\bin\include\visorui\*;%GRIDGAIN_LIBS%

::
:: Parse command line parameters.
::
call "%SCRIPTS_HOME%\include\parseargs.bat" %*
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

:error_finish

if %ERRORLEVEL% neq 0 (
    echo Visor exit with error
    exit /b %ERRORLEVEL%
)

goto :eof
