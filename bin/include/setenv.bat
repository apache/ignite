::
:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to You under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License.  You may obtain a copy of the License at
::
::      http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::

::
:: Exports IGNITE_LIBS variable containing classpath for Ignite.
:: Expects IGNITE_HOME to be set.
:: Can be used like:
::      call %IGNITE_HOME%\bin\include\setenv.bat
:: in other scripts to set classpath using exported IGNITE_LIBS variable.
::

@echo off

:: USER_LIBS variable can optionally contain user's JARs/libs.
:: set USER_LIBS=

::
:: Check IGNITE_HOME.
::
if defined IGNITE_HOME goto run
    echo %0, ERROR: Ignite installation folder is not found.
    echo Please create IGNITE_HOME environment variable pointing to location of
    echo Ignite installation folder.
goto :eof

:run
:: The following libraries are required for Ignite.
set IGNITE_LIBS=%IGNITE_HOME%\libs\*

for /D %%F in (%IGNITE_HOME%\libs\*) do if not "%%F" == "%IGNITE_HOME%\libs\optional" call :concat %%F\*

if exist %IGNITE_HOME%\libs\ignite-hadoop set HADOOP_EDITION=1

if defined USER_LIBS set IGNITE_LIBS=%USER_LIBS%;%IGNITE_LIBS%

if "%HADOOP_EDITION%" == "1" FOR /F "delims=" %%i IN ('%JAVA_HOME%\bin\java.exe -cp %IGNITE_HOME%\libs\ignite-hadoop\* org.apache.ignite.internal.processors.hadoop.HadoopClasspathMain ";"' ) DO set IGNITE_HADOOP_CLASSPATH=%%i

if "%IGNITE_HADOOP_CLASSPATH%" == "" goto :eof

set IGNITE_LIBS=%IGNITE_LIBS%;%IGNITE_HADOOP_CLASSPATH%

goto :eof

:concat
set IGNITE_LIBS=%IGNITE_LIBS%;%1
goto :eof
