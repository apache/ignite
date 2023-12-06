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

set java_version=%~1
set current_value=%~2
set value=""

:: Gets java specific options like add-exports and add-opens
:: First argument is the version of the java
:: Second argument is the current value of the jvm options
:: Third value is the name of the environment variable that jvm options should be set to
if %java_version% == 8 (
    set value= ^
    -XX:+AggressiveOpts ^
    %current_value%
)

if %java_version% GEQ 9 if %java_version% LSS 11 (
    set value= ^
    -XX:+AggressiveOpts ^
    --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED ^
    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED ^
    --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED ^
    --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED ^
    --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED ^
    --illegal-access=permit ^
    --add-modules=java.xml.bind ^
    %current_value%
)

if %java_version% GEQ 11 if %java_version% LSS 15 (
    set value= ^
    --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED ^
    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED ^
    --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED ^
    --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED ^
    --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED ^
    --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED ^
    --illegal-access=permit ^
    %current_value%
)

if %java_version% GEQ 15 (
    set value= ^
    --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED ^
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED ^
    --add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED ^
    --add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED ^
    --add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED ^
    --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED ^
    --add-opens=java.base/java.io=ALL-UNNAMED ^
    --add-opens=java.base/java.nio=ALL-UNNAMED ^
    --add-opens=java.base/java.util=ALL-UNNAMED ^
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED ^
    --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED ^
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED ^
    --add-opens=java.base/java.lang=ALL-UNNAMED ^
    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED ^
    --add-opens=java.base/java.math=ALL-UNNAMED ^
    --add-opens=java.sql/java.sql=ALL-UNNAMED ^
    %current_value%
)

set "%~3=%value%"
