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
:: Exports GRIDGAIN_LIBS variable containing classpath for GridGain.
:: Expects GRIDGAIN_HOME to be set.
:: Can be used like:
::      call %GRIDGAIN_HOME%\bin\include\setenv.bat
:: in other scripts to set classpath using exported GRIDGAIN_LIBS variable.
::

@echo off

:: USER_LIBS variable can optionally contain user's JARs/libs.
:: set USER_LIBS=

::
:: Check GRIDGAIN_HOME.
::
if defined GRIDGAIN_HOME goto run
    echo %0, ERROR: GridGain installation folder is not found.
    echo Please create GRIDGAIN_HOME environment variable pointing to location of
    echo GridGain installation folder.
goto :eof

:run
:: The following libraries are required for GridGain.
set GRIDGAIN_LIBS=%GRIDGAIN_HOME%\libs\*

for /D %%F in (%GRIDGAIN_HOME%\libs\*) do if not %%F == "%GRIDGAIN_HOME%\libs\optional" call :concat %%F\*

if exist %GRIDGAIN_HOME%\libs\gridgain-hadoop set HADOOP_EDITION=1

if defined USER_LIBS set GRIDGAIN_LIBS=%USER_LIBS%;%GRIDGAIN_LIBS%

if "%HADOOP_EDITION%" == "1" call "%SCRIPTS_HOME%\include\hadoop-classpath.bat"

set COMMON_HOME_LIB=%HADOOP_COMMON_HOME%\lib

if "%GRIDGAIN_HADOOP_CLASSPATH%" == "" goto :eof

set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%GRIDGAIN_HADOOP_CLASSPATH%

goto :eof

:concat
set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%1
goto :eof
