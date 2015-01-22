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

package org.gridgain.examples.compute.failover;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.checkpoint.sharedfs.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

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
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        Ignition.start(configuration());
    }

    /**
     * Create Grid configuration with configured checkpoints.
     *
     * @return Grid configuration.
     * @throws IgniteCheckedException If configuration creation failed.
     */
    public static IgniteConfiguration configuration() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(true);

        // Configure checkpoint SPI.
        SharedFsCheckpointSpi checkpointSpi = new SharedFsCheckpointSpi();

        checkpointSpi.setDirectoryPaths(Collections.singletonList("work/checkpoint/sharedfs"));

        cfg.setCheckpointSpi(checkpointSpi);

        // Configure discovery SPI.
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }
}
