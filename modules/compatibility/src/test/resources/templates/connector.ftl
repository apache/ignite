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
<#import "ssl.ftl" as ssl>

<#macro connector_configuration config>
    <#if config??>
        <property name="connectorConfiguration">
            <bean class="org.apache.ignite.configuration.ConnectorConfiguration">
                <#if config.idleTimeout??>
                    <property name="idleTimeout" value="${config.idleTimeout?c}"/>
                </#if>

                <#if config.sslEnabled??>
                    <property name="sslEnabled" value="${config.sslEnabled?c}"/>
                </#if>

                <#if config.sslEnabled!false>
                    <#if config.sslFactory??>
                        <property name="sslFactory">
                            <@ssl.ssl params=config.sslFactory />
                        </property>
                    </#if>

                    <#if config.sslClientAuth??>
                        <property name="sslClientAuth" value="${config.sslClientAuth?c}"/>
                    </#if>
                </#if>
            </bean>
        </property>
    </#if>
</#macro>