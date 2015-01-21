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

package org.gridgain.visor.commands.cache

import org.gridgain.grid.cache.GridCacheAtomicityMode._
import org.gridgain.grid.cache.GridCacheMode._
import org.gridgain.grid.cache._

import org.apache.ignite.Ignition
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.jetbrains.annotations.Nullable

import org.gridgain.visor._
import org.gridgain.visor.commands.cache.VisorCacheCommand._

import scala.collection.JavaConversions._

/**
 *
 */
class VisorCacheClearCommandSpec extends VisorRuntimeBaseSpec(2) {
    /** IP finder. */
    val ipFinder = new TcpDiscoveryVmIpFinder(true)

    /**
     * Creates grid configuration for provided grid host.
     *
     * @param name Grid name.
     * @return Grid configuration.
     */
    override def config(name: String): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        cfg.setGridName(name)
        cfg.setLocalHost("127.0.0.1")
        cfg.setCacheConfiguration(cacheConfig(null), cacheConfig("cache"))

        val discoSpi = new TcpDiscoverySpi()

        discoSpi.setIpFinder(ipFinder)

        cfg.setDiscoverySpi(discoSpi)

        cfg
    }

    /**
     * @param name Cache name.
     * @return Cache Configuration.
     */
    def cacheConfig(@Nullable name: String): GridCacheConfiguration = {
        val cfg = new GridCacheConfiguration

        cfg.setCacheMode(REPLICATED)
        cfg.setAtomicityMode(TRANSACTIONAL)
        cfg.setName(name)

        cfg
    }

    behavior of "An 'cclear' visor command"

    it should "show correct result for default cache" in {
        Ignition.ignite("node-1").cache[Int, Int](null).putAll(Map(1 -> 1, 2 -> 2, 3 -> 3))

        Ignition.ignite("node-1").cache[Int, Int](null).lock(1, 0)

        VisorCacheClearCommand().clear(Nil, None)

        Ignition.ignite("node-1").cache[Int, Int](null).unlock(1)

        VisorCacheClearCommand().clear(Nil, None)
    }

    it should "show correct result for named cache" in {
        Ignition.ignite("node-1").cache[Int, Int]("cache").putAll(Map(1 -> 1, 2 -> 2, 3 -> 3))

        Ignition.ignite("node-1").cache[Int, Int]("cache").lock(1, 0)

        visor.cache("-clear -c=cache")

        Ignition.ignite("node-1").cache[Int, Int]("cache").unlock(1)

        visor.cache("-clear -c=cache")
    }

    it should "show correct help" in {
        VisorCacheCommand

        visor.help("cache")
    }

    it should "show empty projection error message" in {
        visor.cache("-clear -c=wrong")
    }
}
