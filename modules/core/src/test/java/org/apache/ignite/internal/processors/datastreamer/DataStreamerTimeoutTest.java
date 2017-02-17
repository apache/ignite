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
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteDataStreamerTimeoutException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test timeout for Data streamer.
 */
public class DataStreamerTimeoutTest extends GridCommonAbstractTest {
    /** Cache name. */
    public static final String CACHE_NAME = "cacheName";

    /** Timeout. */
    public static final int TIMEOUT = 1_000;

    /** Amount of entries. */
    public static final int ENTRY_AMOUNT = 100;

    /** Fail on. */
    private static volatile int failOn;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

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
     * Test timeout on {@code DataStreamer.addData()} method
     * @throws Exception If fail.
     */
    public void testTimeoutOnCloseMethod() throws Exception {
        failOn = 1;

        Ignite ignite = startGrid(1);

        boolean thrown = false;

        try (IgniteDataStreamer ldr = ignite.dataStreamer(CACHE_NAME)) {
            ldr.timeout(TIMEOUT);
            ldr.receiver(new TestDataReceiver());
            ldr.perNodeBufferSize(ENTRY_AMOUNT);

            for (int i = 0; i < ENTRY_AMOUNT; i++)
                ldr.addData(i, i);
        }
        catch (CacheException | IgniteDataStreamerTimeoutException ignored) {
            thrown = true;
        }
        finally {
            stopAllGrids();
        }

        assertTrue(thrown);
    }

    /**
     * Test timeout on {@code DataStreamer.close()} method
     *
     * @throws Exception If fail.
     */
    public void testTimeoutOnAddData() throws Exception {
        failOn = 1;

        int processed = timeoutOnAddData();

        assertTrue(processed == (failOn + 1) || processed == failOn);

        failOn = ENTRY_AMOUNT / 2;

        processed = timeoutOnAddData();

        assertTrue(processed == (failOn + 1) || processed == failOn);

        failOn = ENTRY_AMOUNT;

        processed = timeoutOnAddData();

        assertTrue(processed == (failOn + 1) || processed == failOn);
    }

    /**
     *
     */
    private int timeoutOnAddData() throws Exception {
        boolean thrown = false;
        int processed = 0;

        try {
            Ignite ignite = startGrid(1);

            try (IgniteDataStreamer ldr = ignite.dataStreamer(CACHE_NAME)) {
                ldr.timeout(TIMEOUT);
                ldr.receiver(new TestDataReceiver());
                ldr.perNodeBufferSize(1);
                ldr.perNodeParallelOperations(1);
                ((DataStreamerImpl)ldr).maxRemapCount(0);

                try {
                    for (int i = 0; i < ENTRY_AMOUNT; i++) {
                        ldr.addData(i, i);

                        processed++;
                    }
                }
                catch (IllegalStateException ignored) {
                    // No-op.
                }
            }
            catch (CacheException | IgniteDataStreamerTimeoutException ignored) {
                thrown = true;
            }
        }
        finally {
            stopAllGrids();
        }

        assertTrue(thrown);

        return processed;
    }

    /**
     * Test receiver for timeout expiration emulation.
     */
    private static class TestDataReceiver implements StreamReceiver {

        /** Count. */
        private final AtomicInteger cnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void receive(IgniteCache cache, Collection col) throws IgniteException {
            try {
                if (cnt.incrementAndGet() == failOn)
                    U.sleep(2 * TIMEOUT);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

}
