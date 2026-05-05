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

<#macro client_connector_configuration config>
    <#if config??>
        <property name="clientConnectorConfiguration">
            <bean class="org.apache.ignite.configuration.ClientConnectorConfiguration">
                <property name="port" value="${config.port?c}"/>

                <#if config.threadPoolSize??>
                    <property name="threadPoolSize" value="${config.threadPoolSize?c}"/>
                </#if>

                <#if config.sslEnabled!false>
                    <property name="sslEnabled" value="true"/>
                    <#if config.sslContextFactory??>
                        <property name="sslContextFactory">
                            <@ssl.ssl params=config.sslContextFactory />
                        </property>
                    </#if>
                    <#if config.useIgniteSslContextFactory??>
                        <property name="useIgniteSslContextFactory" value="${config.useIgniteSslContextFactory?c}"/>
                    </#if>
                    <#if config.sslClientAuth??>
                        <property name="sslClientAuth" value="${config.sslClientAuth?c}"/>
                    </#if>
                </#if>

                <#if config.thinClientConfiguration??>
                    <property name="thinClientConfiguration">
                        <bean class="org.apache.ignite.configuration.ThinClientConfiguration">
                            <#if config.thinClientConfiguration.maxActiveComputeTasksPerConnection??>
                                <property name="maxActiveComputeTasksPerConnection" value="${config.thinClientConfiguration.maxActiveComputeTasksPerConnection?c}" />
                            </#if>
                            <#if config.thinClientConfiguration.maxActiveTxPerConnection??>
                                <property name="maxActiveTxPerConnection" value="${config.thinClientConfiguration.maxActiveTxPerConnection?c}" />
                            </#if>
                        </bean>
                    </property>
                </#if>
            </bean>
        </property>
    </#if>
</#macro>