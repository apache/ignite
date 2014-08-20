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

:: Target class path resolver.
::
:: Can be used like:
::       call "%GRIDGAIN_HOME%\os\bin\include\target-classpath.bat"
:: in other scripts to set classpath using libs from target folder.
::
:: Will be excluded in release.

@echo off

for /D %%F in (%GRIDGAIN_HOME%\os\modules\*) do if not %%F == "%GRIDGAIN_HOME%\os\modules" call :includeToClassPath %%F

for /D %%F in (%GRIDGAIN_HOME%\modules\*) do if not %%F == "%GRIDGAIN_HOME%\modules" call :includeToClassPath %%F

goto :eof

:includeToClassPath
if exist "%1\target\" (
    if exist "%1\target\classes\" call :concat %1\target\classes

    if exist "%1\target\libs\" call :concat %1\target\libs\*
)
goto :eof

:concat
set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%1
goto :eof
