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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;

/** */
public class IgniteCacheAtomicFullSyncPartialUpdateAllTest extends GridCommonAbstractTest {
    /** */
    public static final int NODE_1_FIRST_KEY = 1;

    /** */
    public static final int NODE_1_SECOND_KEY = 4;

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

    /**
     * In the following scenario primary node fails to send {@link GridNearAtomicUpdateResponse) to `near node` during
     * shutdown. As a result entries, belonging to the left primary node, are remmapped to new topology version by the `near node`.
     */
    @Test
    public void testCacheEntriesProcessingFailureCausedByNodeStop() throws Exception {
        startGridsMultiThreaded(3);

        TestInterceptor.putStartedLatch = new CountDownLatch(1);
        TestInterceptor.putUnblockedLatch = new CountDownLatch(1);

        IgniteCache<Integer, Integer> cache = grid(0).createCache(createTestCacheConfiguration(false));

        IgniteInternalFuture<Object> putFut = GridTestUtils.runAsync(() -> cache.putAll(createTestData()));

        assertTrue(TestInterceptor.putStartedLatch.await(getTestTimeout(), MILLISECONDS));

        IgniteEx stoppingNode = grid(1);

        IgniteInternalFuture<Object> stopFut = GridTestUtils.runAsync(() -> stopGrid(1));

        try {
            assertTrue(GridTestUtils.waitForCondition(() ->
                U.<AtomicBoolean>field(stoppingNode.context().cache().cacheGroup(cacheId(DEFAULT_CACHE_NAME)).offheap(), "stopping").get(),
                getTestTimeout()
            ));

            TestInterceptor.putUnblockedLatch.countDown();

            putFut.get(getTestTimeout(), MILLISECONDS);

            assertNotNull(cache.get(0));
            assertNotNull(cache.get(NODE_1_FIRST_KEY));
            assertNotNull(cache.get(NODE_1_SECOND_KEY));
        }
        catch (CachePartialUpdateException e) {
            assertTrue(e.getMessage().contains("Failed to update keys (retry update if possible)"));
        }
        finally {
            stopFut.get(getTestTimeout(), MILLISECONDS);
        }
    }

    /**
     * In the following scenario `near node` does not complete putAll until {@link GridNearAtomicUpdateResponse),
     * contatining cache entry processing errors from the primary node, is received. Even when `near node` receives all
     * responces from backup nodes.
     */
    @Test
    public void testCacheEntriesProcessingFailureCausedByInterceptorException() throws Exception {
        startGridsMultiThreaded(3);

        IgniteCache<Integer, Integer> cache = grid(0).createCache(createTestCacheConfiguration(true));

        CountDownLatch backupResponsesReceivedLatch = new CountDownLatch(3);

        grid(0).context().io().addMessageListener(TOPIC_CACHE, (n, m, p) -> {
            if (m instanceof GridDhtAtomicNearResponse)
                backupResponsesReceivedLatch.countDown();
        });

        spi(grid(1)).blockMessages((n, m) -> m instanceof GridNearAtomicUpdateResponse);

        IgniteInternalFuture<Object> putFut = GridTestUtils.runAsync(() -> cache.putAll(createTestData()));

        spi(grid(1)).waitForBlocked();

        assertTrue(backupResponsesReceivedLatch.await(getTestTimeout(), MILLISECONDS));

        spi(grid(1)).stopBlock();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                putFut.get(getTestTimeout(), MILLISECONDS);

                return null;
            },
            CachePartialUpdateCheckedException.class,
            "Failed to update keys (retry update if possible).: [" + NODE_1_SECOND_KEY + ']'
        );
    }

    /** */
    private CacheConfiguration<Integer, Integer> createTestCacheConfiguration(boolean forceCacheEntryProcessingError) {
        return new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setBackups(2)
            .setAffinity(new GridCacheModuloAffinityFunction(3, 2))
            .setInterceptor(new TestInterceptor(forceCacheEntryProcessingError));
    }

    /** */
    private Map<Integer, Integer> createTestData() {
        Map<Integer, Integer> data = new TreeMap<>();

        data.put(0, 0); // node0 entry
        data.put(NODE_1_FIRST_KEY, 1); // node1 entry
        data.put(NODE_1_SECOND_KEY, 4); // node1 entry

        return data;
    }

    /** */
    public static final class TestInterceptor implements CacheInterceptor<Integer, Integer> {
        /** */
        public static CountDownLatch putStartedLatch;

        /** */
        public static CountDownLatch putUnblockedLatch;

        /** */
        private final boolean forceCacheEntryProcessingError;

        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** */
        public TestInterceptor(boolean forceCacheEntryProcessingError) {
            this.forceCacheEntryProcessingError = forceCacheEntryProcessingError;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer onGet(Integer key, @Nullable Integer val) {
            return val;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer onBeforePut(Cache.Entry<Integer, Integer> entry, Integer newVal) {
            if (ignite.localNode().<Integer>attribute(IDX_ATTR) != 1)
                return newVal;

            if (forceCacheEntryProcessingError) {
                if (entry.getKey() == NODE_1_SECOND_KEY)
                    throw new RuntimeException("expected");
            }
            else if (entry.getKey() == NODE_1_FIRST_KEY) {
                putStartedLatch.countDown();

                try {
                    assertTrue(putUnblockedLatch.await(5000, MILLISECONDS));
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteException(e);
                }
            }

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
