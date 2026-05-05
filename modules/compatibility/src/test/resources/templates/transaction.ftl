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
<#import "misc.ftl" as misc>

<#macro transaction config>
    <#if config??>
        <property name="transactionConfiguration">
            <bean class="org.apache.ignite.configuration.TransactionConfiguration">
                <#if config.defaultTxConcurrency??>
                    <property name="defaultTxConcurrency" value="${config.defaultTxConcurrency}"/>
                </#if>

                <#if config.defaultTxIsolation??>
                    <property name="defaultTxIsolation" value="${config.defaultTxIsolation}"/>
                </#if>

                <#if config.defaultTxTimeout??>
                    <property name="defaultTxTimeout" value="${config.defaultTxTimeout?c}"/>
                </#if>

                <#if config.txTimeoutOnPartitionMapExchange??>
                    <property name="txTimeoutOnPartitionMapExchange" value="${config.txTimeoutOnPartitionMapExchange?c}"/>
                </#if>

                <#if config.txManagerFactory??>
                    <property name="txManagerFactory">
                        <@misc.bean bean_object=config.txManagerFactory />
                    </property>
                </#if>

                <#if config.useJtaSynchronization??>
                    <property name="useJtaSynchronization" value="${config.useJtaSynchronization?c}"/>
                </#if>

                <#if config.txAwareQueriesEnabled??>
                    <property name="txAwareQueriesEnabled" value="${config.txAwareQueriesEnabled?c}"/>
                </#if>
            </bean>
        </property>
    </#if>
</#macro>