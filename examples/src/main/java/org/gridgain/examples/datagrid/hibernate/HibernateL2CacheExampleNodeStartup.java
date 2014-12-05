/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.hibernate;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Starts up an empty node with example cache configuration.
 */
public class HibernateL2CacheExampleNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        Ignition.start(configuration());
    }

    /**
     * Create Grid configuration with GGFS and enabled IPC.
     *
     * @return Grid configuration.
     * @throws GridException If configuration creation failed.
     */
    public static IgniteConfiguration configuration() throws GridException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("hibernate-grid");
        cfg.setLocalHost("127.0.0.1");
        cfg.setRestEnabled(false);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(
            cacheConfiguration("org.hibernate.cache.spi.UpdateTimestampsCache", ATOMIC),
            cacheConfiguration("org.hibernate.cache.internal.StandardQueryCache", ATOMIC),
            cacheConfiguration("org.gridgain.examples.datagrid.hibernate.User", TRANSACTIONAL),
            cacheConfiguration("org.gridgain.examples.datagrid.hibernate.User.posts", TRANSACTIONAL),
            cacheConfiguration("org.gridgain.examples.datagrid.hibernate.Post", TRANSACTIONAL)
        );

        return cfg;
    }

    /**
     * Create cache configuration.
     *
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private static GridCacheConfiguration cacheConfiguration(String name, GridCacheAtomicityMode atomicityMode) {
        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setName(name);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }
}
