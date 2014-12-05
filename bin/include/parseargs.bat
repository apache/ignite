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

@echo off

::
:: Parses command line parameters into GridGain variables that are common for the launcher scripts:
:: CONFIG
:: INTERACTIVE
:: QUIET
:: NO_PAUSE
:: JVM_XOPTS
::
:: Script setups reasonable defaults (see below) for omitted arguments.
::
:: Scripts accepts following incoming variables:
:: DEFAULT_CONFIG
::
:: Can be used like:
:: call "%GRIDGAIN_HOME%\bin\include\parseargs.bat" %*
:: if %ERRORLEVEL% neq 0 (
::     echo Arguments parsing failed
::     exit /b %ERRORLEVEL%
:: )
:: in other scripts to parse common command lines parameters.

set convertArgsCmd="%JAVA_HOME%\bin\java.exe" -cp %CP% org.apache.ignite.startup.cmdline.CommandLineTransformer %*
for /f "usebackq tokens=*" %%i in (`"%convertArgsCmd%"`) do set reformattedArgs=%%i

for %%i in (%reformattedArgs%) do (
    if "%%i" == "CommandLineTransformerError" exit /b 1
    set %%i
)

if not defined CONFIG set CONFIG=%DEFAULT_CONFIG%

exit /b 0
