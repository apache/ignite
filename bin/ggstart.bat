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
:: Grid command line loader.
::

@echo off

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
if %ERRORLEVEL% equ 0 goto checkGridGainHome1
    echo %0, ERROR:
    echo The version of JAVA installed in %JAVA_HOME% is incorrect.
    echo Please point JAVA_HOME variable to installation of JDK 1.7 or JDK 1.8.
    echo You can also download latest JDK at http://java.com/download.
goto error_finish

:: Check GRIDGAIN_HOME.
:checkGridGainHome1
if defined GRIDGAIN_HOME goto checkGridGainHome2
    pushd "%~dp0"/../.. :: Will be replaced by pushd "%~dp0"/..
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
set SCRIPTS_HOME=%GRIDGAIN_HOME%\os\bin :: Will be replace by SCRIPTS_HOME=${GRIDGAIN_HOME_TMP}\bin in release.

if /i "%SCRIPTS_HOME%\" == "%~dp0" goto setProgName
    echo %0, WARN: GRIDGAIN_HOME environment variable may be pointing to wrong folder: %GRIDGAIN_HOME%

:setProgName
::
:: Set program name.
::
set PROG_NAME=ggstart.bat
if "%OS%" == "Windows_NT" set PROG_NAME=%~nx0%

:run

::
:: Set GRIDGAIN_LIBS
::
call "%SCRIPTS_HOME%\include\setenv.bat"
call "%SCRIPTS_HOME%\include\target-classpath.bat" :: Will be removed in release.
set CP=%GRIDGAIN_LIBS%

::
:: Parse command line parameters.
::
if not defined DEFAULT_CONFIG set "DEFAULT_CONFIG=config\default-config.xml"
call "%SCRIPTS_HOME%\include\parseargs.bat" %*
if %ERRORLEVEL% neq 0 (
    echo Arguments parsing failed
    exit /b %ERRORLEVEL%
)

::
:: Process 'restart'.
::
set RANDOM_NUMBER_COMMAND="%JAVA_HOME%\bin\java.exe" -cp %CP% org.gridgain.grid.startup.cmdline.GridCommandLineRandomNumberGenerator
for /f "usebackq tokens=*" %%i in (`"%RANDOM_NUMBER_COMMAND%"`) do set RANDOM_NUMBER=%%i

set RESTART_SUCCESS_FILE="%GRIDGAIN_HOME%\work\gridgain_success_%RANDOM_NUMBER%"
set RESTART_SUCCESS_OPT=-DGRIDGAIN_SUCCESS_FILE=%RESTART_SUCCESS_FILE%

::
:: Find available port for JMX
::
:: You can specify GRIDGAIN_JMX_PORT environment variable for overriding automatically found JMX port
::
for /F "tokens=*" %%A in ('""%JAVA_HOME%\bin\java" -cp %CP% org.gridgain.grid.util.portscanner.GridJmxPortFinder"') do (
    set JMX_PORT=%%A
)

::
:: This variable defines necessary parameters for JMX
:: monitoring and management.
::
:: This enables remote unsecure access to JConsole or VisualVM.
::
:: ADD YOUR ADDITIONAL PARAMETERS/OPTIONS HERE
::
if "%JMX_PORT%" == "" (
    echo %0, WARN: Failed to resolve JMX host. JMX will be disabled.
    set JMX_MON=
) else (
    set JMX_MON=-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=%JMX_PORT% ^
    -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false
)

::
:: JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
::
:: ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
::
set BASE_JVM_OPTS=-Xms1g -Xmx1g -server -XX:+AggressiveOpts

if "%JVM_OPTS%" == "" (
    :: Hadoop needs class unloading enabled and large size of perm space.
    if defined GRIDGAIN_HADOOP_CLASSPATH (
        set JVM_OPTS=%BASE_JVM_OPTS% -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m
    ) else (
        set JVM_OPTS=%BASE_JVM_OPTS%
    )
)

::
:: Uncomment the following GC settings if you see spikes in your throughput due to Garbage Collection.
::
:: set JVM_OPTS=%JVM_OPTS% -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseTLAB -XX:NewSize=128m -XX:MaxNewSize=128m
:: set JVM_OPTS=%JVM_OPTS% -XX:MaxTenuringThreshold=0 -XX:SurvivorRatio=1024 -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=60

::
:: Uncomment if you get StackOverflowError.
:: On 64 bit systems this value can be larger, e.g. -Xss16m
::
:: set JVM_OPTS=%JVM_OPTS% -Xss4m

::
:: Uncomment to set preference to IPv4 stack.
::
:: set JVM_OPTS=%JVM_OPTS% -Djava.net.preferIPv4Stack=true

::
:: Assertions are disabled by default since version 3.5.
:: If you want to enable them - set 'ENABLE_ASSERTIONS' flag to '1'.
::
set ENABLE_ASSERTIONS=1

::
:: Set '-ea' options if assertions are enabled.
::
if %ENABLE_ASSERTIONS% == 1 set JVM_OPTS=%JVM_OPTS% -ea

:run_java

::
:: Set main class to start service (grid node by default).
::

if "%MAIN_CLASS%" == "" set MAIN_CLASS=org.gridgain.grid.startup.cmdline.GridCommandLineStartup

::
:: Remote debugging (JPDA).
:: Uncomment and change if remote debugging is required.
:: set JVM_OPTS=-Xdebug -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=n %JVM_OPTS%
::

if "%INTERACTIVE%" == "1" (
    "%JAVA_HOME%\bin\java.exe" %JVM_OPTS% %QUIET% %RESTART_SUCCESS_OPT% %JMX_MON% ^
    -DGRIDGAIN_UPDATE_NOTIFIER=false -DGRIDGAIN_HOME="%GRIDGAIN_HOME%" -DGRIDGAIN_PROG_NAME="%PROG_NAME%" %JVM_XOPTS% ^
    -cp "%CP%" %MAIN_CLASS%
) else (
    "%JAVA_HOME%\bin\java.exe" %JVM_OPTS% %QUIET% %RESTART_SUCCESS_OPT% %JMX_MON% ^
    -DGRIDGAIN_UPDATE_NOTIFIER=false -DGRIDGAIN_HOME="%GRIDGAIN_HOME%" -DGRIDGAIN_PROG_NAME="%PROG_NAME%" %JVM_XOPTS% ^
    -cp "%CP%" %MAIN_CLASS% "%CONFIG%"
)

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

if %ERRORLEVEL% neq 0 (
    echo Visor exit with error
    exit /b %ERRORLEVEL%
)

goto :eof
