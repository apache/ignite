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

::
:: Set router service environment.
::
set "DEFAULT_CONFIG=config\router\default-router.xml"
set MAIN_CLASS=org.apache.ignite.internal.client.router.impl.GridRouterCommandLineStartup

::
:: Start router service.
::
call "%~dp0\ignite.bat" %*
