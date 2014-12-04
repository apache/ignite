/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.benchmarks.storevalbytes;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 *
 */
public class GridCacheStoreValueBytesNode {
    /**
     * @return Discovery SPI.
     * @throws Exception If failed.
     */
    static GridTcpDiscoverySpi discovery() throws Exception {
        GridTcpDiscoverySpi disc = new GridTcpDiscoverySpi();

        disc.setLocalAddress("localhost");

        GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder();

        Collection<String> addrs = new ArrayList<>();

        for (int i = 0; i < 10; i++)
            addrs.add("localhost:" + (GridTcpDiscoverySpi.DFLT_PORT + i));

        ipFinder.setAddresses(addrs);

        disc.setIpFinder(ipFinder);

        return disc;
    }

    /**
     * @param size Size.
     * @return Value.
     */
    static String createValue(int size) {
        StringBuilder str = new StringBuilder();

        str.append(new char[size]);

        return str.toString();
    }

    /**
     * @param args Arguments.
     * @param nearOnly Near only flag.
     * @return Configuration.
     * @throws Exception If failed.
     */
    static IgniteConfiguration parseConfiguration(String[] args, boolean nearOnly) throws Exception {
        boolean p2pEnabled = false;

        boolean storeValBytes = false;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            switch (arg) {
                case "-p2p":
                    p2pEnabled = Boolean.parseBoolean(args[++i]);

                    break;

                case "-storeValBytes":
                    storeValBytes = Boolean.parseBoolean(args[++i]);

                    break;
            }
        }

        X.println("Peer class loading enabled: " + p2pEnabled);
        X.println("Store value bytes: " + storeValBytes);

        IgniteConfiguration cfg = new IgniteConfiguration();

        GridTcpCommunicationSpi commSpi = new GridTcpCommunicationSpi();
        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        cfg.setDiscoverySpi(discovery());

        cfg.setPeerClassLoadingEnabled(p2pEnabled);

        GridCacheConfiguration cacheCfg = new GridCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);

        cacheCfg.setStoreValueBytes(storeValBytes);

        cacheCfg.setBackups(1);

        if (nearOnly) {
            cacheCfg.setNearEvictionPolicy(new GridCacheAlwaysEvictionPolicy());

            cacheCfg.setDistributionMode(NEAR_ONLY);
        }

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        Ignition.start(parseConfiguration(args, false));
    }
}
