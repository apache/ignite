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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;

/** */
public class MutallyDependentCacheUpdateFailureAtNodesStoppageTest extends GridCommonAbstractTest {
    /** */
    public static final int NODE_1_FIRST_KEY = 1;

    /** */
    public static final int NODE_1_SECOND_KEY = 4;

    /** */
    public static final int NODE_2_FIRST_KEY = 2;

    /** */
    public static final int NODE_2_SECOND_KEY = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
                .setCommunicationSpi(new TestRecordingCommunicationSpi())
                .setUserAttributes(singletonMap(IDX_ATTR, getTestIgniteInstanceIndex(igniteInstanceName)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testCacheEntriesProcessingFailureCausedByNodeStop() throws Exception {
        startGridsMultiThreaded(3);

        TestInterceptor.putStartedLatch = new CountDownLatch(2);
        TestInterceptor.putUnblockedLatch = new CountDownLatch(1);

        grid(0).createCache(createTestCacheConfiguration());

        try (
                IgniteClient cli1 = Ignition.startClient(new ClientConfiguration().setClusterDiscoveryEnabled(false)
                    .setAddresses("127.0.0.1:10801"));
                IgniteClient cli2 = Ignition.startClient(new ClientConfiguration().setClusterDiscoveryEnabled(false)
                    .setAddresses("127.0.0.1:10802"))
        ) {
            IgniteInternalFuture<Object> putFut1 = GridTestUtils.runAsync(() -> cli1.cache(DEFAULT_CACHE_NAME)
                .putAll(createKeysForNode(2)));
            IgniteInternalFuture<Object> putFut2 = GridTestUtils.runAsync(() -> cli2.cache(DEFAULT_CACHE_NAME)
                .putAll(createKeysForNode(1)));

            assertTrue(TestInterceptor.putStartedLatch.await(getTestTimeout(), MILLISECONDS));

            IgniteInternalFuture<Object> stopFut1 = GridTestUtils.runAsync(() -> stopGrid(1));
            IgniteInternalFuture<Object> stopFut2 = GridTestUtils.runAsync(() -> stopGrid(2));

            try {
                TestInterceptor.putUnblockedLatch.countDown();

                stopFut1.get(getTestTimeout());
                stopFut2.get(getTestTimeout());

                putFut1.get(getTestTimeout());
                putFut2.get(getTestTimeout());
            }
            catch (Throwable e) {
                assertTrue(e.getMessage().contains("Connection refused"));
            }
        }
    }

    /** */
    private CacheConfiguration<Integer, Integer> createTestCacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>()
                .setName(DEFAULT_CACHE_NAME)
                .setAtomicityMode(ATOMIC)
                .setWriteSynchronizationMode(FULL_SYNC)
                .setBackups(2)
                .setAffinity(new GridCacheModuloAffinityFunction(3, 2))
                .setInterceptor(new TestInterceptor());
    }

    /** */
    private Map<Integer, Integer> createKeysForNode(int nodeIdx) {
        Map<Integer, Integer> data = new TreeMap<>();

        if (nodeIdx == 2) {
            data.put(NODE_2_FIRST_KEY, 2);
            data.put(NODE_2_SECOND_KEY, 5);
        }
        else {
            data.put(NODE_1_FIRST_KEY, 1);
            data.put(NODE_1_SECOND_KEY, 4);
        }

        return data;
    }

    /** */
    private static final class TestInterceptor implements CacheInterceptor<Integer, Integer> {
        /** */
        private static CountDownLatch putStartedLatch;

        /** */
        private static CountDownLatch putUnblockedLatch;

        /** {@inheritDoc} */
        @Override public @Nullable Integer onGet(Integer key, @Nullable Integer val) {
            return val;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer onBeforePut(Cache.Entry<Integer, Integer> entry, Integer newVal) {
            if (entry.getKey() == NODE_1_FIRST_KEY || entry.getKey() == NODE_2_FIRST_KEY) {
                putStartedLatch.countDown();

                try {
                    assertTrue(putUnblockedLatch.await(10000, MILLISECONDS));
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteException(e);
                }
            }
            else
                throw new RuntimeException("Test failure in interceptor");

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<Integer, Integer> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public @Nullable IgniteBiTuple<Boolean, Integer> onBeforeRemove(Cache.Entry<Integer, Integer> entry) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<Integer, Integer> entry) {
            // No-op.
        }
    }
}
