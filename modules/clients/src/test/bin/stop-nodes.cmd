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

@echo OFF

echo Kill all grid nodes.
FOR /f "tokens=1,2" %%G IN ('%JAVA_HOME%/bin/jps') DO IF (%%H) EQU (GridCommandLineStartup) taskkill /PID %%G /T /F

echo Kill all grid nodes.
FOR /f "tokens=1,2" %%G IN ('%JAVA_HOME%/bin/jps') DO IF (%%H) EQU (GridRouterCommandLineStartup) taskkill /PID %%G /T /F

echo Wait 1 seconds while nodes stopped.
ping -n 1 127.0.0.1 > NUL

echo Kill all cmd processes.
taskkill /IM cmd.exe /T /F > NUL

echo ON
