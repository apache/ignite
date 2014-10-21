/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.cswap

import org.gridgain.grid.cache.{GridCacheConfiguration, GridCacheMode}
import org.gridgain.grid.spi.discovery.tcp.GridTcpDiscoverySpi
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.GridTcpDiscoveryVmIpFinder
import org.gridgain.grid.{GridConfiguration, GridGain => G}
import org.gridgain.visor._
import org.gridgain.visor.commands.cache.VisorCacheCommand._
import org.jetbrains.annotations.Nullable

import scala.collection.JavaConversions._

class VisorCacheSwapCommandSpec extends VisorRuntimeBaseSpec(2) {
    /** IP finder. */
    val ipFinder = new GridTcpDiscoveryVmIpFinder(true)

    /**
     * Creates grid configuration for provided grid host.
     *
     * @param name Grid name.
     * @return Grid configuration.
     */
    override def config(name: String): GridConfiguration = {
        val cfg = new GridConfiguration

        cfg.setGridName(name)
        cfg.setLocalHost("127.0.0.1")
        cfg.setCacheConfiguration(cacheConfig(null), cacheConfig("cache"))

        val discoSpi = new GridTcpDiscoverySpi()

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

        cfg.setName(name)
        cfg.setCacheMode(GridCacheMode.PARTITIONED)
        cfg.setSwapEnabled(true)

        cfg
    }

    behavior of "An 'cswap' visor command"

    it should "show correct result for default cache" in {
        G.grid("node-1").cache[Int, Int](null).putAll(Map(1 -> 1, 2 -> 2, 3 -> 3))

        visor.cache("-swap -c=<default>")
    }

    it should "show correct result for named cache" in {
        G.grid("node-1").cache[Int, Int]("cache").putAll(Map(1 -> 1, 2 -> 2, 3 -> 3))

        visor.cache("-swap -c=cache")
    }

    it should "show empty projection error message" in {
        visor.cache("-swap -c=wrong")
    }
}
