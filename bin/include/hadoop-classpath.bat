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

:: Turn off script echoing.
@echo off

:: Check if environment passes HADOOP_PREFIX:
if not defined HADOOP_HOME set HADOOP_HOME=%HADOOP_PREFIX%

if not [%HADOOP_HOME%] == [] (
   :: Trim quotes.
   set HADOOP_HOME=%HADOOP_HOME:"=%

   ::
   :: Setting the rest of Hadoop environment variables.
   ::
   if not defined HADOOP_COMMON_HOME set HADOOP_COMMON_HOME=%HADOOP_HOME%\share\hadoop\common
   if not defined HADOOP_HDFS_HOME set HADOOP_HDFS_HOME=%HADOOP_HOME%\share\hadoop\hdfs
   if not defined HADOOP_MAPRED_HOME set HADOOP_MAPRED_HOME=%HADOOP_HOME%\share\hadoop\mapreduce
)

if not exist "%HADOOP_COMMON_HOME%" (  
   echo Hadoop common folder %HADOOP_COMMON_HOME% not found. Please check HADOOP_COMMON_HOME or HADOOP_HOME environment variable.
   exit 1
) 

if not exist "%HADOOP_HDFS_HOME%" (
   echo Hadoop HDFS folder %HADOOP_HDFS_HOME% not found. Please check HADOOP_HDFS_HOME or HADOOP_HOME environment variable.
   exit 1
) 

if not exist "%HADOOP_MAPRED_HOME%" (
   echo Hadoop map-reduce folder %HADOOP_MAPRED_HOME% not found. Please check HADOOP_MAPRED_HOME or HADOOP_HOME environment variable.
   exit 1
) 

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
