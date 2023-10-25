:: Licensed to the Apache Software Foundation (ASF) under one
:: or more contributor license agreements.  See the NOTICE file
:: distributed with this work for additional information
:: regarding copyright ownership.  The ASF licenses this file
:: to you under the Apache License, Version 2.0 (the
:: "License"); you may not use this file except in compliance
:: with the License.  You may obtain a copy of the License at
::
::   http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing,
:: software distributed under the License is distributed on an
:: "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
:: KIND, either express or implied.  See the License for the
:: specific language governing permissions and limitations
:: under the License.

:: Windows launcher script for Gremlin Console

@ECHO OFF
SETLOCAL EnableDelayedExpansion
SET work=%CD%

IF [%work:~-3%]==[bin] CD ..

IF NOT DEFINED IGNITE_HOME (
    SET IGNITE_HOME=%CD%
)

:: location of the JanusGraph lib directory
SET JANUSGRAPH_LIB=%IGNITE_HOME%\libs

:: location of the JanusGraph extensions directory
IF NOT DEFINED JANUSGRAPH_EXT (
    SET JANUSGRAPH_EXT=%IGNITE_HOME%\exts
)

:: Set default message threshold for Log4j Gremlin's console appender
IF NOT DEFINED GREMLIN_LOG_LEVEL (
    SET GREMLIN_LOG_LEVEL=WARN
)


:: set HADOOP_GREMLIN_LIBS by default to the JanusGraph lib
IF NOT DEFINED HADOOP_GREMLIN_LIBS (
    SET HADOOP_GREMLIN_LIBS=%JANUSGRAPH_LIB%
)


:checkJdkVersion
set cmd="%JAVA_HOME%\bin\java.exe"
for /f "tokens=* USEBACKQ" %%f in (`%cmd% -version 2^>^&1`) do (
    set var=%%f
    goto :LoopEscape
)
:LoopEscape

for /f "tokens=1-3  delims= " %%a in ("%var%") do set JAVA_VER_STR=%%c
set JAVA_VER_STR=%JAVA_VER_STR:"=%

for /f "tokens=1,2 delims=." %%a in ("%JAVA_VER_STR%.x") do set MAJOR_JAVA_VER=%%a& set MINOR_JAVA_VER=%%b
if %MAJOR_JAVA_VER% == 1 set MAJOR_JAVA_VER=%MINOR_JAVA_VER%

call "%IGNITE_HOME%\bin\include\jvmdefaults.bat" %MAJOR_JAVA_VER% "%JVM_OPTS%" JVM_OPTS

CD %IGNITE_HOME%

::
:: Set IGNITE_LIBS
::
call "%IGNITE_HOME%\bin\include\setenv.bat"

SET CP=%CLASSPATH%;%IGNITE_LIBS%;exts\*

:: jline.terminal workaround for https://issues.apache.org/jira/browse/GROOVY-6453
:: to debug plugin :install include -Divy.message.logger.level=4 -Dgroovy.grape.report.downloads=true
:: to debug log4j include -Dlog4j.debug=true
IF NOT DEFINED JAVA_OPTIONS (
 SET JAVA_OPTIONS=%JVM_OPTS% -Xms32m -Xmx512m ^
 -Dtinkerpop.ext=%JANUSGRAPH_EXT% ^
 -Dlog4j.configurationFile=file:/%IGNITE_HOME%\config\log4j2-console.xml ^
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED  ^
  --illegal-access=warn ^
 -Dio.netty.tryReflectionSetAccessible=true ^
 -Djline.terminal=none ^
 -Dfile.encoding=utf-8
)

:: Launch the application

IF "%1" == "" GOTO console
IF "%1" == "-e" GOTO script
IF "%1" == "-server" GOTO server
IF "%1" == "-jpda" GOTO debug

:: Start the Gremlin Console

:console

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %CP% org.apache.tinkerpop.gremlin.console.Console %*

GOTO finally

:debug

SET debug=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=19003
java  %JAVA_OPTIONS% %JAVA_ARGS% %debug% -cp %CP% org.apache.tinkerpop.gremlin.console.Console %*

GOTO finally

:: Evaluate a Groovy script file

:script

SET strg=

FOR %%X IN (%*) DO (
CALL :concat %%X %1 %2
)

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %CP% org.apache.tinkerpop.gremlin.groovy.jsr223.ScriptExecutor %strg%

GOTO finally

:: Print the version

:server
echo "use ignite client mode as gremlin-server engine."
call "%IGNITE_HOME%\bin\include\setenv.bat"
java %JAVA_OPTIONS% %JAVA_ARGS% -cp %IGNITE_LIBS% org.apache.tinkerpop.gremlin.server.GremlinServer config\gremlin-server\gremlin-server-ignite.yaml

GOTO finally


:concat

IF %1 == %2 GOTO finally

SET strg=%strg% %1


:finally

ENDLOCAL
