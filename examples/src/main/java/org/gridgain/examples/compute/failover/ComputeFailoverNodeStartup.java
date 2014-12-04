/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.compute.failover;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.checkpoint.sharedfs.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;

import java.util.*;

/**
 * Starts up an empty node with checkpoint-enabled configuration.
 * <p>
 * The difference is that running this class from IDE adds all example classes to classpath
 * but running from command line doesn't.
 */
public class ComputeFailoverNodeStartup {
    /**
     * Start up an empty node with specified configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        GridGain.start(configuration());
    }

    /**
     * Create Grid configuration with configured checkpoints.
     *
     * @return Grid configuration.
     * @throws GridException If configuration creation failed.
     */
    public static IgniteConfiguration configuration() throws GridException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(true);

        // Configure checkpoint SPI.
        GridSharedFsCheckpointSpi checkpointSpi = new GridSharedFsCheckpointSpi();

        checkpointSpi.setDirectoryPaths(Collections.singletonList("work/checkpoint/sharedfs"));

        cfg.setCheckpointSpi(checkpointSpi);

        // Configure discovery SPI.
        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }
}
