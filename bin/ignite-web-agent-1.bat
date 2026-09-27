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
:: Grid router command line loader.
::

@echo off

if "%OS%" == "Windows_NT" setlocal

set JVM_OPTS=-agentlib:jdwp=transport=dt_socket,address=0.0.0.0:19003,server=y,suspend=n
::set JAVA_HOME=C:\Program Files\Java\jdk-11
::set PATH=C:\Program Files\Java\jdk-11\bin;%PATH%

::
:: Set router service environment.
::
set DEFAULT_CONFIG=--config config/agents/1.properties
set MAIN_CLASS=org.apache.ignite.console.agent.AgentLauncher
SET work=%CD%

IF [%work:~-3%]==[bin] CD ..
::
:: Start router service.
::
call "bin\ignite.bat" %*
