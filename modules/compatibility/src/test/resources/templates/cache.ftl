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

<#macro cache_configs caches>
    <#if caches?? && caches?size gt 0>
        <property name="cacheConfiguration">
            <list>
                <#list caches as cache>
                    <bean class="org.apache.ignite.configuration.CacheConfiguration">
                         <property name="name" value="${cache.name}"/>

                        <#if cache.cacheMode?? && cache.cacheMode == "PARTITIONED">
                            <property name="backups" value="${(cache.backups!0)?c}"/>
                        </#if>

                        <property name="atomicityMode" value="${cache.atomicityMode!"ATOMIC"}"/>

                        <#if cache.statisticsEnabled??>
                            <property name="statisticsEnabled" value="${cache.statisticsEnabled?c}"/>
                        </#if>

                        <#if cache.affinity??>
                            <property name="affinity">
                                <@misc.bean bean_object=cache.affinity />
                            </property>
                        </#if>

                        <#if cache.affinityMapper??>
                            <property name="affinityMapper">
                                <@misc.bean bean_object=cache.affinityMapper />
                            </property>
                        </#if>
                    </bean>
                </#list>
            </list>
        </property>
    </#if>
</#macro>