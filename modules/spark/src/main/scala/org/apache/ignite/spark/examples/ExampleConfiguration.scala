/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spark.examples

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.internal.util.lang.{GridFunc => F}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder

object ExampleConfiguration {
    def configuration(): IgniteConfiguration = {
        val cfg = new IgniteConfiguration()

        val discoSpi = new TcpDiscoverySpi()

        val ipFinder = new TcpDiscoveryVmIpFinder()

        ipFinder.setAddresses(F.asList("127.0.0.1:47500", "127.0.0.1:47501"))

        discoSpi.setIpFinder(ipFinder)

        cfg.setDiscoverySpi(discoSpi)

        cfg
    }
}
