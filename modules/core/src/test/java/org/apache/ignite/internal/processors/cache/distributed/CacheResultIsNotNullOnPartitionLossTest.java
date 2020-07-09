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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CacheResultIsNotNullOnPartitionLossTest extends GridCommonAbstractTest {
    /** Number of servers to be started. */
    private static final int SERVERS = 5;

    /** Index of node that is goning to be the only client node. */
    private static final int CLIENT_IDX = SERVERS;

    /** Number of cache entries to insert into the test cache. */
    private static final int CACHE_ENTRIES_CNT = 60;

    /** Client Ignite instance. */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setActiveOnStart(false);
        cfg.setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(0)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setAffinity(new RendezvousAffinityFunction(false, 50))
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        List<Integer> list = IntStream.range(0, SERVERS).boxed().collect(Collectors.toList());

        Collections.shuffle(list);

        for (Integer i : list)
            startGrid(i);

        grid(0).cluster().active(true);
        grid(0).cluster().baselineAutoAdjustEnabled(false);

        client = startClientGrid(CLIENT_IDX);

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
    @Test
    public void testCacheResultIsNotNullOnClient() throws Exception {
        testCacheResultIsNotNull0(client);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheResultIsNotNullOnLastServer() throws Exception {
        testCacheResultIsNotNull0(grid(SERVERS - 1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
                grid(i).close();

                Thread.sleep(400L);
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
            IgniteClientDisconnectedException.class,
            CacheInvalidStateException.class,
            ClusterTopologyCheckedException.class,
            IllegalStateException.class,
            NodeStoppingException.class
        );
    }
}
