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

package org.apache.ignite.spi.discovery.tcp;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Test for {@link TcpDiscoverySpi}.
 */
public class TcpDiscoveryConcurrentStartTest extends GridCommonAbstractTest {
    /** */
    private static final int TOP_SIZE = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

//        TcpDiscoveryMulticastIpFinder finder = new TcpDiscoveryMulticastIpFinder();
//
//        finder.setMulticastGroup(GridTestUtils.getNextMulticastGroup(getClass()));
//        finder.setMulticastPort(GridTestUtils.getNextMulticastPort(getClass()));
//
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryIpFinder() {
            private final List<InetSocketAddress> addrs = new LinkedList<>();

            @Override public void onSpiContextInitialized(IgniteSpiContext spiCtx) throws IgniteSpiException {

            }

            @Override public void onSpiContextDestroyed() {

            }

            @Override
            public void initializeLocalAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {

            }

            @Override public Collection<InetSocketAddress> getRegisteredAddresses() throws IgniteSpiException {
                synchronized (this.addrs){
                    if(addrs.isEmpty()) {
                        this.addrs.add(new InetSocketAddress("127.0.0.1", 47500));
                        this.addrs.add(new InetSocketAddress("127.0.0.1", 47501));
                        this.addrs.add(new InetSocketAddress("127.0.0.1", 47502));
                        this.addrs.add(new InetSocketAddress("127.0.0.1", 47503));
                        this.addrs.add(new InetSocketAddress("127.0.0.1", 47504));
                        this.addrs.add(new InetSocketAddress("127.0.0.1", 47505));
                        this.addrs.add(new InetSocketAddress("127.0.0.1", 47506));
                        this.addrs.add(new InetSocketAddress("127.0.0.1", 47507));
                    }
                }

                return Collections.unmodifiableList(addrs);
            }

            @Override public boolean isShared() {
                return false;
            }

            @Override public void registerAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {

            }

            @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) throws IgniteSpiException {

            }

            @Override public void close() {

            }
        }));

        cfg.setCacheConfiguration();

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentStart() throws Exception {
        for (int i = 0; i < 10; i++) {
            try {
//                startGridsMultiThreaded(TOP_SIZE);
                startGridsMultiThreaded(1);
            }
            finally {
                stopAllGrids();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentStartClients() throws Exception {
        for (int i = 0; i < 10; i++) {
            try {
                startGrid(0);

                final AtomicInteger gridIdx = new AtomicInteger(1);

                GridTestUtils.runMultiThreaded(new Callable<Object>() {
                        @Nullable @Override public Object call() throws Exception {
                            startClientGrid(gridIdx.getAndIncrement());

                            return null;
                        }
                    },
                    TOP_SIZE,
                    "grid-starter-" + getName()
                );

                checkTopology(TOP_SIZE + 1);
            }
            finally {
                stopAllGrids();
            }
        }
    }
}
