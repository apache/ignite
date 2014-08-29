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
:: Grid router command line loader.
::

@echo off

if "%OS%" == "Windows_NT" setlocal

::
:: Set router service environment.
::
set "DEFAULT_CONFIG=config\router\default-router.xml"
set MAIN_CLASS=org.gridgain.client.router.impl.GridRouterCommandLineStartup

::
:: Start router service.
::
call "%~dp0\ggstart.bat" %*
