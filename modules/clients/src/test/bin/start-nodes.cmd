::
::                   GridGain Community Edition Licensing
::                   Copyright 2019 GridGain Systems, Inc.
::
:: Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
:: Restriction; you may not use this file except in compliance with the License. You may obtain a
:: copy of the License at
::
:: http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software distributed under the
:: License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
:: KIND, either express or implied. See the License for the specific language governing permissions
:: and limitations under the License.
::
:: Commons Clause Restriction
::
:: The Software is provided to you by the Licensor under the License, as defined below, subject to
:: the following condition.
::
:: Without limiting other conditions in the License, the grant of rights under the License will not
:: include, and the License does not grant to you, the right to Sell the Software.
:: For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
:: under the License to provide to third parties, for a fee or other consideration (including without
:: limitation fees for hosting or consulting/ support services related to the Software), a product or
:: service whose value derives, entirely or substantially, from the functionality of the Software.
:: Any license notice or attribution required by the License must also include this Commons Clause
:: License Condition notice.
::
:: For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
:: the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
:: Edition software provided with this notice.
::

@echo OFF

rem Define environment paths.
SET SCRIPT_DIR=%~dp0

SET CONFIG_DIR=%SCRIPT_DIR%\..\resources
SET CLIENTS_MODULE_PATH=%SCRIPT_DIR%\..\..\..
SET BIN_PATH=%SCRIPT_DIR%\..\..\..\..\..\bin

cd %SCRIPT_DIR%\..\..\..\..\..\..
set IGNITE_HOME=%CD%

rem Define this script configuration.
set NODES_COUNT=2

echo IGNITE_HOME: %IGNITE_HOME%
echo JAVA_HOME: %JAVA_HOME%
echo SCRIPT_DIR: %SCRIPT_DIR%

echo Switch to home directory %IGNITE_HOME%
cd %IGNITE_HOME%

set MVN_EXEC=mvn

if defined M2_HOME set MVN_EXEC=%M2_HOME%\bin\%MVN_EXEC%

call %MVN_EXEC% -P+test,-scala,-examples,-release clean package -DskipTests -DskipClientDocs

echo Switch to build script directory %SCRIPT_DIR%
cd %SCRIPT_DIR%

rem Force to create log's directory.
rmdir %IGNITE_HOME%\work\log /S /Q
mkdir %IGNITE_HOME%\work\log

set JVM_OPTS=-DCLIENTS_MODULE_PATH=%CLIENTS_MODULE_PATH%

FOR /L %%G IN (1,1,%NODES_COUNT%) DO start "Node #%%G" /low /MIN cmd /C "%BIN_PATH%\ignite.bat -v %CONFIG_DIR%\spring-server-node.xml >> %IGNITE_HOME%\work\log\node-%%G.log 2>&1"
FOR /L %%G IN (1,1,%NODES_COUNT%) DO start "SSL Node #%%G" /low /MIN cmd /C "%BIN_PATH%\ignite.bat -v %CONFIG_DIR%\spring-server-ssl-node.xml >> %IGNITE_HOME%\work\log\node-ssl-%%G.log 2>&1"
FOR /L %%G IN (1,1,%NODES_COUNT%) DO start "Cache Node #%%G" /low /MIN cmd /C "%BIN_PATH%\ignite.bat -v %CONFIG_DIR%\spring-cache.xml >> %IGNITE_HOME%\work\log\cache-node-%%G.log 2>&1"

echo Wait 60 seconds while nodes started.
ping -n 60 127.0.0.1 > NUL

rem Disable hostname verification for self-signed certificates.
set JVM_OPTS=%JVM_OPTS% -DIGNITE_DISABLE_HOSTNAME_VERIFIER=true

FOR /L %%G IN (1,1,1) DO start "Router #%%G" /low /MIN cmd /C "%BIN_PATH%\igniterouter.bat -v %CONFIG_DIR%\spring-router.xml >> %IGNITE_HOME%\work\log\router-%%G.log 2>&1"
FOR /L %%G IN (1,1,1) DO start "SSL Router #%%G" /low /MIN cmd /C "%BIN_PATH%\igniterouter.bat -v %CONFIG_DIR%\spring-router-ssl.xml >> %IGNITE_HOME%\work\log\router-ssl-%%G.log 2>&1"

echo Wait 10 seconds while routers started.
ping -n 10 127.0.0.1 > NUL

echo.
echo Expects all nodes started.

echo ON
