::  Licensed to the Apache Software Foundation (ASF) under one or more
::  contributor license agreements.  See the NOTICE file distributed with
::  this work for additional information regarding copyright ownership.
::  The ASF licenses this file to You under the Apache License, Version 2.0
::  (the "License"); you may not use this file except in compliance with
::  the License.  You may obtain a copy of the License at
::
::       http://www.apache.org/licenses/LICENSE-2.0
::
::  Unless required by applicable law or agreed to in writing, software
::  distributed under the License is distributed on an "AS IS" BASIS,
::  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
::  See the License for the specific language governing permissions and
::  limitations under the License.

:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
::                 Hadoop class path resolver.
::  Requires environment variables 'HADOOP_PREFIX' or 'HADOOP_HOME'
::  to be set.
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

:: Turn off script echoing.
@echo off

:: Check if environment passes deprecated HADOOP_HOME.
if not defined HADOOP_PREFIX set HADOOP_PREFIX=%HADOOP_HOME%

:: Exit if we cannot find Hadoop installation directory.
if not defined HADOOP_PREFIX goto :eof

:: Trim quotes.
set HADOOP_PREFIX=%HADOOP_PREFIX:"=%

:: Trim slashes.
if %HADOOP_PREFIX:~-1,1% == \ (
    set HADOOP_PREFIX=%HADOOP_PREFIX:~0,-1%
)

::
:: Setting the rest of Hadoop environment variables.
::

if not defined HADOOP_COMMON_HOME set HADOOP_COMMON_HOME=%HADOOP_PREFIX%\share\hadoop\common
if not defined HADOOP_HDFS_HOME set HADOOP_HDFS_HOME=%HADOOP_PREFIX%\share\hadoop\hdfs
if not defined HADOOP_MAPRED_HOME set HADOOP_MAPRED_HOME=%HADOOP_PREFIX%\share\hadoop\mapreduce

::
:: Libraries included in classpath.
::

set CP=%HADOOP_COMMON_HOME%\lib\*;%HADOOP_MAPRED_HOME%\lib\*;%HADOOP_MAPRED_HOME%\lib\*

:: hadoop-auth-* jar can be located either in home or in home/lib directory, depending on the hadoop version.
for /f %%f in ('dir /B %HADOOP_COMMON_HOME%\hadoop-auth-* ^>nul 2^>^&1') do call :concat %HADOOP_COMMON_HOME%\%%f
for /f %%f in ('dir /B %HADOOP_COMMON_HOME%\lib\hadoop-auth-* ^>nul 2^>^&1') do call :concat %HADOOP_COMMON_HOME%\%%f
for /f %%f in ('dir /B %HADOOP_COMMON_HOME%\hadoop-common-*') do call :concat %HADOOP_COMMON_HOME%\%%f
for /f %%f in ('dir /B %HADOOP_HDFS_HOME%\hadoop-hdfs-*') do call :concat %HADOOP_HDFS_HOME%\%%f
for /f %%f in ('dir /B %HADOOP_MAPRED_HOME%\hadoop-mapreduce-client-common-*') do call :concat %HADOOP_MAPRED_HOME%\%%f
for /f %%f in ('dir /B %HADOOP_MAPRED_HOME%\hadoop-mapreduce-client-core-*') do call :concat %HADOOP_MAPRED_HOME%\%%f

:: Export result.
set IGNITE_HADOOP_CLASSPATH=%CP%

:: Exit.
goto :eof

:: Function that adds jar dependency into classpath.
:concat
    set file=%1
    if %file:~-9,9% neq tests.jar set CP=%CP%;%1
goto :eof
