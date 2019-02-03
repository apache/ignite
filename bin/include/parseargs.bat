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

@echo off

::
:: Parses command line parameters into Ignite variables that are common for the launcher scripts:
:: CONFIG
:: INTERACTIVE
:: QUIET
:: NO_PAUSE
:: JVM_XOPTS
:: NOJMX
::
:: Script setups reasonable defaults (see below) for omitted arguments.
::
:: Scripts accepts following incoming variables:
:: DEFAULT_CONFIG
::
:: Can be used like:
:: call "%IGNITE_HOME%\bin\include\parseargs.bat" %*
:: if %ERRORLEVEL% neq 0 (
::     echo Arguments parsing failed
::     exit /b %ERRORLEVEL%
:: )
:: in other scripts to parse common command lines parameters.

set convertArgsCmd="!JAVA_HOME!\bin\java.exe" -cp "%CP%" org.apache.ignite.startup.cmdline.CommandLineTransformer %*

for /f "usebackq tokens=*" %%i in (`"!convertArgsCmd!"`) do set reformattedArgs=%%i

for %%i in (%reformattedArgs%) do (
    if "%%i" == "CommandLineTransformerError" exit /b 1
    set %%i
)

if not defined CONFIG set CONFIG=%DEFAULT_CONFIG%

exit /b 0
