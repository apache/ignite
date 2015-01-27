@echo OFF

rem Define environment paths.
SET SCRIPT_DIR=%~dp0

SET CONFIG_DIR=%SCRIPT_DIR%\..\resources
SET CLIENTS_MODULE_PATH=%SCRIPT_DIR%\..\..\..
SET BIN_PATH=%SCRIPT_DIR%\..\..\..\..\..\bin

cd %SCRIPT_DIR%\..\..\..\..\..\..
set GG_HOME=%CD%

rem Define this script configuration.
set NODES_COUNT=2

echo GG_HOME: %GG_HOME%
echo JAVA_HOME: %JAVA_HOME%
echo SCRIPT_DIR: %SCRIPT_DIR%

echo Switch to home directory %GG_HOME%
cd %GG_HOME%

set MVN_EXEC=mvn

if defined M2_HOME set MVN_EXEC=%M2_HOME%\bin\%MVN_EXEC%

call %MVN_EXEC% -P+test,-scala,-examples,-release clean package -DskipTests -DskipClientDocs

echo Switch to build script directory %SCRIPT_DIR%
cd %SCRIPT_DIR%

rem Force to create log's directory.
rmdir %GG_HOME%\work\log /S /Q
mkdir %GG_HOME%\work\log

set JVM_OPTS=-DCLIENTS_MODULE_PATH=%CLIENTS_MODULE_PATH%

FOR /L %%G IN (1,1,%NODES_COUNT%) DO start "Node #%%G" /low /MIN cmd /C "%BIN_PATH%\ggstart.bat -v %CONFIG_DIR%\spring-server-node.xml >> %GG_HOME%\work\log\node-%%G.log 2>&1"
FOR /L %%G IN (1,1,%NODES_COUNT%) DO start "SSL Node #%%G" /low /MIN cmd /C "%BIN_PATH%\ggstart.bat -v %CONFIG_DIR%\spring-server-ssl-node.xml >> %GG_HOME%\work\log\node-ssl-%%G.log 2>&1"
FOR /L %%G IN (1,1,%NODES_COUNT%) DO start "Cache Node #%%G" /low /MIN cmd /C "%BIN_PATH%\ggstart.bat -v %CONFIG_DIR%\spring-cache.xml >> %GG_HOME%\work\log\cache-node-%%G.log 2>&1"

echo Wait 60 seconds while nodes started.
ping -n 60 127.0.0.1 > NUL

rem Disable hostname verification for self-signed certificates.
set JVM_OPTS=%JVM_OPTS% -DIGNITE_DISABLE_HOSTNAME_VERIFIER=true

FOR /L %%G IN (1,1,1) DO start "Router #%%G" /low /MIN cmd /C "%BIN_PATH%\ggrouter.bat -v %CONFIG_DIR%\spring-router.xml >> %GG_HOME%\work\log\router-%%G.log 2>&1"
FOR /L %%G IN (1,1,1) DO start "SSL Router #%%G" /low /MIN cmd /C "%BIN_PATH%\ggrouter.bat -v %CONFIG_DIR%\spring-router-ssl.xml >> %GG_HOME%\work\log\router-ssl-%%G.log 2>&1"

echo Wait 10 seconds while routers started.
ping -n 10 127.0.0.1 > NUL

echo.
echo Expects all nodes started.

echo ON
