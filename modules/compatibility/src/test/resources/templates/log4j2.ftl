<?xml version="1.0" encoding="UTF-8"?>
<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
<Configuration status="WARN" monitorInterval="30">
    <Properties>
        <Property name="LOG_PATTERN">[%d{ISO8601}][%-5p][%t][%c{1}] %m%n</Property>
        <Property name="APP_LOG_ROOT">${logDir}</Property>
    </Properties>

    <Appenders>
        <Console name="Console" target="SYSTEM_ERR" follow="true">
            <PatternLayout pattern="${r"${LOG_PATTERN}"}"/>
        </Console>

        <Routing name="FILE">
            <Routes pattern="$${r"${sys:appId}"}">
                <Route>
                    <RollingFile name="Rolling-${r"${sys:appId}"}" fileName="${r"${APP_LOG_ROOT}"}/${r"${sys:appId}"}.log"
                                 filePattern="${r"${APP_LOG_ROOT}"}/${r"${sys:appId}"}-%d{yyyy-MM-dd}-%i.log">
                        <PatternLayout pattern="${r"${LOG_PATTERN}"}"/>
                        <Policies>
                            <SizeBasedTriggeringPolicy size="10MB"/>
                        </Policies>
                        <DefaultRolloverStrategy max="10"/>
                    </RollingFile>
                </Route>
            </Routes>
        </Routing>
    </Appenders>

    <Loggers>
        <Logger name="org.springframework" level="WARN"/>
        <Logger name="org.eclipse.jetty" level="WARN"/>
        <Logger name="org.eclipse.jetty.util.log" level="ERROR"/>
        <Logger name="org.eclipse.jetty.util.component" level="ERROR"/>
        <Logger name="com.amazonaws" level="WARN"/>
        <Logger name="org.apache.ignite" level="info"/>
        <Logger name="org.apache.zookeeper" level="info"/>

        <Root level="info">
            <AppenderRef ref="Console" level="ERROR"/>
            <AppenderRef ref="FILE"/>
        </Root>
    </Loggers>
</Configuration>