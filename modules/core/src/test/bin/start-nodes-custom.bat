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

set SCRIPT_DIR=%~dp0

if %SCRIPT_DIR:~-1,1% == \ set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%

:: -np option is mandatory, if it is not provided then we will wait for a user input,
:: as a result ggservice windows service hangs forever
call "%SCRIPT_DIR%\..\..\..\..\..\bin\ggstart.bat" -v -np modules\core\src\test\config\spring-start-nodes-attr.xml
