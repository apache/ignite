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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@SuppressWarnings("unchecked")
public class DataStreamerFailuresTest extends GridCommonAbstractTest {
    /** Cache name. */
    public static final String CACHE_NAME = "cacheName";

    /** Timeout. */
    private static final int TIMEOUT = 1_000;

    /** Amount of entries. */
    private static final int ENTRY_AMOUNT = 1_000;

    /** Client node index. */
    private static final int CLIENT_NODE_IDX = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (getTestGridName(CLIENT_NODE_IDX).equals(gridName))
            cfg.setClientMode(true);
        else
            cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setName(CACHE_NAME);

        return cacheCfg;
    }

    /**
     * Test fail on receiver, when streamer is called from server node.
     *
     * @throws Exception If fail.
     */
    public void testFromServer() throws Exception {
        try {
            Ignite ignite1 = startGrid(0);
            Ignite ignite2 = startGrid(1);

            checkTopology(2);

            IgniteFuture[] futures = loadData(ignite1);

            checkFailAddData(futures);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test fail on receiver, when streamer invokes from client node.
     * @throws Exception If fail.
     */
    public void testFromClient() throws Exception {
        try {
            Ignite ignite1 = startGrid(0);
            Ignite ignite2 = startGrid(1);
            Ignite client = startGrid(CLIENT_NODE_IDX);

            checkTopology(3);

            IgniteFuture[] futures = loadData(client);

            checkFailAddData(futures);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Data loading failed, because server node broken.
     *
     * @throws Exception If failed.
     */
    public void testServerBrokenPerLoad() throws Exception {
        try {
            Ignite ignite1 = startGrid(0);
            Ignite ignite2 = startGrid(1);
            Ignite client = startGrid(CLIENT_NODE_IDX);

            checkTopology(3);

            boolean thrown = false;

            IgniteDataStreamer ldr = client.dataStreamer(CACHE_NAME);

            try {
                ldr.receiver(new TestDataReceiver());
                ldr.perNodeBufferSize(ENTRY_AMOUNT/6);
                ldr.perNodeParallelOperations(1);
                ((DataStreamerImpl)ldr).maxRemapCount(0);

                for (int i = 0; i < ENTRY_AMOUNT; i++) {
                    if (i > ENTRY_AMOUNT/3)
                        stopAllServers(true);

                    ldr.addData(i, i);
                }
            }
            catch (Throwable e) {
                thrown = true;
            }
            finally {
                try {
                    ldr.close();
                } catch (Throwable e) {
                    thrown = true;
                }
            }

            assertTrue(thrown);
        }
        finally {
            try {
                stopAllGrids();
            } catch (Throwable e){
                //This should not hang.
            }
        }
    }

    /**
     * Test is checking ability of handling exception when flushing.
     *
     * @throws Exception If failed.
     */
    public void testHandledExceptionAndFlushAgain() throws Exception {
        try {
            Ignite ignite1 = startGrid(0);
            Ignite ignite2 = startGrid(1);

            checkTopology(2);

            boolean thrown = false;

            try (IgniteDataStreamer ldr = ignite1.dataStreamer(CACHE_NAME)) {
                ldr.receiver(new TestDataReceiverFailedOnce());
                ldr.perNodeBufferSize(ENTRY_AMOUNT/6);
                ldr.perNodeParallelOperations(1);
                ((DataStreamerImpl)ldr).maxRemapCount(0);

                for (int i = 0; i < ENTRY_AMOUNT; i++) {
                    ldr.addData(i, i);

                    if (i == 1) {
                        try {
                            ldr.flush();
                        } catch (Exception e) {
                            thrown = true;
                        }
                    }
                }
            }

            assertTrue(thrown);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param futures Futures.
     */
    private void checkFailAddData(IgniteFuture[] futures) {
        boolean thrown = false;

        for (int i = 0; i < ENTRY_AMOUNT; i++) {
            try {
                try {
                    futures[i].get(TIMEOUT);
                }
                catch (IgniteFutureTimeoutException e) {
                    fail();
                }
            }
            catch (CacheException e) {
                thrown = true;
            }
        }

        assertTrue(thrown);
    }

    /**
     * @param ignite Ignite.
     * @return Load futures.
     */
    private IgniteFuture[] loadData(Ignite ignite) {
        boolean thrown = false;

        IgniteDataStreamer ldr = ignite.dataStreamer(CACHE_NAME);

        IgniteFuture[] futures = new IgniteFuture[ENTRY_AMOUNT];

        try {
            ldr.receiver(new TestDataReceiver());
            ldr.perNodeBufferSize(ENTRY_AMOUNT / 6);
            ldr.perNodeParallelOperations(1);

            ((DataStreamerImpl)ldr).maxRemapCount(0);

            for (int i = 0; i < ENTRY_AMOUNT; i++)
                futures[i] = ldr.addData(i, i);
        }
        finally {
            try {
                ldr.close();
            }
            catch (CacheException e) {
                thrown = true;
            }
        }

        assertTrue(thrown);

        return futures;
    }

    /**
     * Test receiver for timeout expiration emulation.
     */
    private static class TestDataReceiver implements StreamReceiver {
        /** {@inheritDoc} */
        @Override public void receive(IgniteCache cache, Collection col) {
            throw new IgniteException("Error in TestDataReceiver.");
        }
    }

    /**
     * Test receiver, which failed only once.
     */
    private static class TestDataReceiverFailedOnce implements StreamReceiver {
        /** Not throw. */
        private static AtomicBoolean notThrow = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache cache, Collection col) {
            if (!notThrow.getAndSet(true))
                throw new IgniteException("First fail.");
        }
    }

}
