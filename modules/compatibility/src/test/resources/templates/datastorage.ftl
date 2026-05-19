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

<#macro data_storage ds>
    <#if ds??>
        <property name="dataStorageConfiguration">
            <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
                <#if ds.defaultDataRegionConfiguration??>
                    <property name="defaultDataRegionConfiguration">
                        <@data_region config=ds.defaultDataRegionConfiguration />
                    </property>
                </#if>

                <#if ds.dataRegionConfigurations?? && ds.dataRegionConfigurations?size gt 0>
                    <property name="dataRegionConfigurations">
                        <list>
                            <#list ds.dataRegionConfigurations as region>
                                <@data_region config=region />
                            </#list>
                        </list>
                    </property>
                </#if>

                <#if ds.defaultWarmUpConfiguration??>
                    <property name="defaultWarmUpConfiguration">
                        <@misc.bean bean_object=ds.defaultWarmUpConfiguration />
                    </property>
                </#if>

                <#if ds.storagePath??><property name="storagePath" value="${ds.storagePath}"/></#if>
                <#if ds.walPath??><property name="walPath" value="${ds.walPath}"/></#if>
                <#if ds.walArchivePath??><property name="walArchivePath" value="${ds.walArchivePath}"/></#if>

                <#if ds.walMode??>
                    <property name="walMode" value="${ds.walMode}"/>
                </#if>
            </bean>
        </property>
    </#if>
</#macro>

<#macro data_region config>
    <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
        <#if config.name??><property name="name" value="${config.name}"/></#if>
        <#if config.maxSize??><property name="maxSize" value="${config.maxSize?c}"/></#if>
        <#if config.initialSize??><property name="initialSize" value="${config.initialSize?c}"/></#if>
        <#if config.persistenceEnabled??>
            <property name="persistenceEnabled" value="${config.persistenceEnabled?c}"/>
        </#if>

        <#if config.warmUpConfiguration??>
            <property name="warmUpConfiguration">
                <@misc.bean bean_object=config.warmUpConfiguration />
            </property>
        </#if>
    </bean>
</#macro>