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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheResultIsNotNullOnPartitionLossTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of servers to be started. */
    private static final int SERVERS = 10;

    /** Index of node that is goning to be the only client node. */
    private static final int CLIENT_IDX = SERVERS;

    /** Number of cache entries to insert into the test cache. */
    private static final int CACHE_ENTRIES_CNT = 10_000;

    /** True if {@link #getConfiguration(String)} is expected to configure client node on next invocations. */
    private boolean isClient;

    /** Client Ignite instance. */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(0)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
        );

        if (isClient)
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        startGrids(SERVERS);

        isClient = true;

        client = startGrid(CLIENT_IDX);

        try (IgniteDataStreamer<Integer, Integer> dataStreamer = client.dataStreamer(DEFAULT_CACHE_NAME)) {
            dataStreamer.allowOverwrite(true);

            for (int i = 0; i < CACHE_ENTRIES_CNT; i++)
                dataStreamer.addData(i, i);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheResultIsNotNullOnClient() throws Exception {
        testCacheResultIsNotNull0(client);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheResultIsNotNullOnLastServer() throws Exception {
        testCacheResultIsNotNull0(grid(SERVERS - 1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheResultIsNotNullOnServer() throws Exception {
        testCacheResultIsNotNull0(grid(SERVERS - 2));
    }
    /**
     * @throws Exception If failed.
     */
    private void testCacheResultIsNotNull0(IgniteEx ignite) throws Exception {
        AtomicBoolean stopReading = new AtomicBoolean();

        AtomicReference<Throwable> unexpectedThrowable = new AtomicReference<>();

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        CountDownLatch readerThreadStarted = new CountDownLatch(1);

        IgniteInternalFuture<Boolean> nullCacheValFoundFut = GridTestUtils.runAsync(() -> {
            readerThreadStarted.countDown();

            while (!stopReading.get())
                for (int i = 0; i < CACHE_ENTRIES_CNT && !stopReading.get(); i++) {
                    try {
                        if (cache.get(i) == null)
                            return true;
                    }
                    catch (Throwable t) {
                        if (expectedThrowableClass(t)) {
                            try {
                                cache.put(i, i);

                                unexpectedThrowable.set(new RuntimeException("Cache put was successful for entry " + i));
                            }
                            catch (Throwable t2) {
                                if (!expectedThrowableClass(t2))
                                    unexpectedThrowable.set(t2);
                            }
                        }
                        else
                            unexpectedThrowable.set(t);

                        break;
                    }
                }

            return false;
        });

        try {
            readerThreadStarted.await(1, TimeUnit.SECONDS);

            for (int i = 0; i < SERVERS - 1; i++) {
                Thread.sleep(50L);

                grid(i).close();
            }
        }
        finally {
            // Ask reader thread to finish its execution.
            stopReading.set(true);
        }

        assertFalse("Null value was returned by cache.get instead of exception.", nullCacheValFoundFut.get());

        Throwable throwable = unexpectedThrowable.get();
        if (throwable != null) {
            throwable.printStackTrace();

            fail(throwable.getMessage());
        }
    }

    /**
     *
     */
    private boolean expectedThrowableClass(Throwable throwable) {
        return X.hasCause(
            throwable,
            CacheInvalidStateException.class,
            ClusterTopologyCheckedException.class,
            IllegalStateException.class,
            NodeStoppingException.class
        );
    }
}
