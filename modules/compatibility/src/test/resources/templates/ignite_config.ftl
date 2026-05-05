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
<#import "communication.ftl" as communication>
<#import "discovery.ftl" as discovery>
<#import "cache.ftl" as cache>
<#import "datastorage.ftl" as datastorage>
<#import "ssl.ftl" as ssl>
<#import "connector.ftl" as connector>
<#import "client_connector.ftl" as client_connector>
<#import "transaction.ftl" as transaction>
<#import "checkpoint.ftl" as checkpoint />
<#import "misc.ftl" as misc>

<#macro apply config>
    <bean class="org.apache.ignite.configuration.IgniteConfiguration">
        <#if config.workDirectory??>
            <property name="workDirectory" value="${config.workDirectory}" />
        </#if>

        <#if config.igniteHome??>
            <property name="igniteHome" value="${config.igniteHome}" />
        </#if>

        <property name="gridLogger">
            <bean class="org.apache.ignite.logger.log4j2.Log4J2Logger">
                <constructor-arg type="java.lang.String" value="${logConfigPath}"/>
            </bean>
        </property>

        <property name="clientMode" value="${(config.clientMode!false)?c}"/>
        <property name="nodeId" value="${config.nodeId!""}"/>
        <property name="consistentId" value="${config.igniteInstanceName!""}"/>
        <property name="igniteInstanceName" value="${config.igniteInstanceName!""}"/>
        <property name="peerClassLoadingEnabled" value="${(config.peerClassLoadingEnabled!false)?c}"/>

        <@communication.communication_spi spi=config.communicationSpi />
        <@discovery.discovery_spi spi=config.discoverySpi />
        <@datastorage.data_storage ds=config.dataStorageConfiguration />
        <@cache.cache_configs caches=config.cacheConfiguration />
        <@checkpoint.checkpoint_spi spi_list=config.checkpointSpi />

        <#if config.sslContextFactory??>
            <property name="sslContextFactory">
                <@ssl.ssl params=config.sslContextFactory />
            </property>
        </#if>

        <#if config.connectorConfiguration??>
            <@connector.connector_configuration config=config.connectorConfiguration />
        </#if>

        <#if config.clientConnectorConfiguration??>
            <@client_connector.client_connector_configuration config=config.clientConnectorConfiguration />
        </#if>

        <@transaction.transaction config=config.transactionConfiguration />

        <#if config.binaryConfiguration??>
            <property name="binaryConfiguration">
                <bean class="org.apache.ignite.configuration.BinaryConfiguration">
                    <property name="compactFooter" value="${config.binaryConfiguration.compactFooter?c}"/>
                </bean>
            </property>
        </#if>

        <#if config.includeEventTypes?? && config.includeEventTypes?size gt 0>
            <property name="includeEventTypes" ref="eventTypes"/>
        </#if>

        <#if config.sqlConfiguration??>
            <property name="sqlConfiguration">
                <@misc.bean bean_object=config.sqlConfiguration />
            </property>
        </#if>
    </bean>
</#macro>