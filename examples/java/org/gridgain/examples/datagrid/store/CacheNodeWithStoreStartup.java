// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.store;

import org.gridgain.examples.datagrid.store.dummy.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.multicast.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;

import java.util.*;

/**
 * Starts up an empty node with example cache configuration.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheNodeWithStoreStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        GridGain.start(configure());
    }

    /**
     * Configure grid.
     *
     * @return Grid configuration.
     * @throws GridException If failed.
     */
    public static GridConfiguration configure() throws GridException {
        GridConfiguration cfg = new GridConfiguration();

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryMulticastIpFinder();

        Collection<String> addrs = new ArrayList<>();

        String addr = "127.0.0.1";
        int port = 47500;

        for (int i = 0; i < 10; i++)
            addrs.add(addr + ':' + port++);

        ipFinder.setAddresses(addrs);

        discoSpi.setIpFinder(ipFinder);

        GridCacheConfiguration cacheCfg = new GridCacheConfiguration();

        cacheCfg.setStore(new CacheDummyPersonStore());
        // cacheCfg.setStore(new CacheJdbcPersonStore());
        // cacheCfg.setStore(new CacheHibernatePersonStore());

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }
}
