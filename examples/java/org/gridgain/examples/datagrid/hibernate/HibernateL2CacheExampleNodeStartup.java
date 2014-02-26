// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.hibernate;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.partitioned.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.ATOMIC;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.TRANSACTIONAL;
import static org.gridgain.grid.cache.GridCacheMode.PARTITIONED;
import static org.gridgain.grid.cache.GridCacheMode.REPLICATED;
import static org.gridgain.grid.cache.GridCachePartitionedDistributionMode.PARTITIONED_ONLY;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Starts up an empty node with example cache configuration.
 *
 * @author @java.author
 * @version @java.version
 */
public class HibernateL2CacheExampleNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws org.gridgain.grid.GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        GridGain.start(configuration());
    }

    /**
     * Create Grid configuration with GGFS and enabled IPC.
     *
     * @return Grid configuration.
     * @throws GridException If configuration creation failed.
     */
    public static GridConfiguration configuration() throws GridException {
        GridConfiguration cfg = new GridConfiguration();

        cfg.setGridName("hibernate-grid");
        cfg.setLocalHost("127.0.0.1");
        cfg.setRestEnabled(false);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder();

        Collection<String> addrs = new ArrayList<>();

        for (int i = 0; i < 10; i++)
            addrs.add("127.0.0.1:" + (47500 + i));

        ipFinder.setAddresses(addrs);

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
