/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.visor.commands.cache

import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheAtomicityMode._
import org.apache.ignite.cache.CacheMode._
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.ignite.visor.commands.cache.VisorCacheCommand._
import org.apache.ignite.visor.{VisorRuntimeBaseSpec, visor}
import org.jetbrains.annotations.NotNull

import scala.collection.JavaConversions._

/**
  * Unit test for 'reset' command.
  */
class VisorCacheResetCommandSpec extends VisorRuntimeBaseSpec(2) {
    /** IP finder. */
    val ipFinder = new TcpDiscoveryVmIpFinder(true)

    /**
     * Creates grid configuration for provided grid host.
     *
     * @param name Ignite instance name.
     * @return Grid configuration.
     */
    override def config(name: String): IgniteConfiguration = {
        val cfg = new IgniteConfiguration

        cfg.setIgniteInstanceName(name)
        cfg.setLocalHost("127.0.0.1")
        cfg.setCacheConfiguration(cacheConfig("default"), cacheConfig("cache"))

        val discoSpi = new TcpDiscoverySpi()

        discoSpi.setIpFinder(ipFinder)

        cfg.setDiscoverySpi(discoSpi)

        cfg
    }

    /**
     * @param name Cache name.
     * @return Cache Configuration.
     */
    def cacheConfig(@NotNull name: String): CacheConfiguration[Object, Object] = {
        val cfg = new CacheConfiguration[Object, Object]

        cfg.setCacheMode(REPLICATED)
        cfg.setAtomicityMode(TRANSACTIONAL)
        cfg.setName(name)

        cfg
    }

    describe("A 'reset' visor command") {
        it("should show correct result for default cache") {
            Ignition.ignite("node-1").cache[Int, Int](null).putAll(Map(1 -> 1, 2 -> 2, 3 -> 3))

            val lock = Ignition.ignite("node-1").cache[Int, Int]("default").lock(1)

            lock.lock()

            VisorCacheResetCommand().reset(Nil, None)

            lock.unlock()

            VisorCacheResetCommand().reset(Nil, None)
        }

        it("should show correct result for named cache") {
            Ignition.ignite("node-1").cache[Int, Int]("cache").putAll(Map(1 -> 1, 2 -> 2, 3 -> 3))

            val lock = Ignition.ignite("node-1").cache[Int, Int]("cache").lock(1)

            lock.lock()

            visor.cache("-reset -c=cache")

            lock.unlock()

            visor.cache("-reset -c=cache")
        }

        it("should show correct help") {
            VisorCacheCommand

            visor.help("cache")
        }

        it("should show empty projection error message") {
            visor.cache("-reset -c=wrong")
        }
    }
}
