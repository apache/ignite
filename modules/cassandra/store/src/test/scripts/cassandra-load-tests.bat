::
:: Copyright 2019 GridGain Systems, Inc. and Contributors.
:: 
:: Licensed under the GridGain Community Edition License (the "License");
:: you may not use this file except in compliance with the License.
:: You may obtain a copy of the License at
:: 
::     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
:: 
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::
echo off

echo.

set TESTS_CLASSPATH="%~dp0lib\*;%~dp0settings"

call %~dp0jvm-opts.bat %*

call java %JVM_OPTS% -cp "%TESTS_CLASSPATH%" "org.apache.ignite.tests.CassandraDirectPersistenceLoadTest"

if %errorLevel% NEQ 0 (
    echo.
    echo --------------------------------------------------------------------------------
    echo [ERROR] Tests execution failed
    echo --------------------------------------------------------------------------------
    echo.
    exit /b %errorLevel%
)

echo.
echo --------------------------------------------------------------------------------
echo [INFO] Tests execution succeed
echo --------------------------------------------------------------------------------
echo.
