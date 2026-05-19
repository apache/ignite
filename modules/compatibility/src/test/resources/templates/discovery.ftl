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
<#macro ip_finder spi>
    <#if spi.ipFinder?? && spi.ipFinder.registeredAddresses??>
        <property name="ipFinder">
            <bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder">
                <property name="addresses">
                    <list>
                        <#list spi.ipFinder.registeredAddresses as address>
                            <value>${address?string?replace("/", "")}</value>
                        </#list>
                    </list>
                </property>
            </bean>
        </property>
    </#if>
</#macro>

<#macro discovery_spi spi>
    <#if spi??>
        <property name="discoverySpi">
            <bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi">
                <@ip_finder spi=spi />
            </bean>
        </property>
    </#if>
</#macro>