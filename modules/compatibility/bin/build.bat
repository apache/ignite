::
:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to You under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License. You may obtain a copy of the License at
::
::      http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::

@echo off

set SCRIPT_DIR=%~dp0
set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%

if defined M2_HOME (
    set MVN_CMD=%M2_HOME%\bin\mvn.bat
) else (
    set MVN_CMD=mvn
)

for /D %%F in ("%SCRIPT_DIR%"\..\ignite-versions\*) do (
    IF EXIST "%%F\pom.xml" (
        call "%MVN_CMD%" -s "%SCRIPT_DIR%\settings.xml" -f "%%F\pom.xml" clean package

       if ERRORLEVEL 1 exit \b
    )
)
