// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.client.interceptor;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.partitioned.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.indexing.h2.*;

import javax.swing.*;
import java.util.*;

import static org.gridgain.grid.GridDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePartitionedDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Starts up grid node (server) for use with {@link ClientMessageInterceptorExample}.
 * <p>
 * Note that different nodes cannot share the same port for rest services. If you want
 * to start more than one node on the same physical machine you must provide different
 * configurations for each node. Otherwise, this example would not work.
 *
 * @author @java.author
 * @version @java.version
 */
public class ClientMessageInterceptorExampleNodeStartup {
    /**
     * Starts up a node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start(configuration())) {
            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[]{
                    new JLabel("GridGain started."),
                    new JLabel("Press OK to stop GridGain.")
                },
                "GridGain",
                JOptionPane.INFORMATION_MESSAGE
            );
        }
    }

    /**
     * Create Grid configuration with GGFS and enabled IPC.
     *
     * @return Grid configuration.
     * @throws GridException If configuration creation failed.
     */
    public static GridConfiguration configuration() throws GridException {
        GridConfiguration cfg = new GridConfiguration();

        cfg.setLocalHost("127.0.0.1");
        cfg.setDeploymentMode(SHARED);
        cfg.setPeerClassLoadingEnabled(true);

        GridOptimizedMarshaller marsh = new GridOptimizedMarshaller();

        marsh.setRequireSerializable(false);


        GridH2IndexingSpi indexSpi = new GridH2IndexingSpi();

        indexSpi.setDefaultIndexPrimitiveKey(true);
        indexSpi.setDefaultIndexFixedTyping(false);

        cfg.setIndexingSpi(indexSpi);

        cfg.setClientMessageInterceptor(new ClientBigIntegerMessageInterceptor());

        GridCacheConfiguration cacheCfg = new GridCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAtomicityMode(ATOMIC);
        cacheCfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        cacheCfg.setPartitionedDistributionMode(PARTITIONED_ONLY);
        cacheCfg.setAffinity(new GridCachePartitionedAffinity(1));
        cacheCfg.setStartSize(1500000);
        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setPreloadMode(SYNC);

        cfg.setCacheConfiguration(cacheCfg);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder();

        Collection<String> addrs = new ArrayList<>();

        for (int i = 0; i < 10; i++)
            addrs.add("127.0.0.1:" + (47500 + i));

        ipFinder.setAddresses(addrs);

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }
}
