/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager.TOP_VER_BASE_TIME;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests to enuse that there's no race in grid cache version generation
 * between retrieveing records from 3rd party storage and put variations.
 */
public class GridCacheVersionGenerationWithCacheStorageTest extends GridCommonAbstractTest {
    /** Latch in order to slow down exchange. */
    private CountDownLatch latch = new CountDownLatch(1);


    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setCacheStoreFactory(singletonFactory(new TestStore()));
        ccfg.setReadThrough(true);
        ccfg.setBackups(0);
        ccfg.setIndexedTypes(
            Integer.class, Integer.class
        );

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        // In case of exiting waitForCondition because of timeout.
        latch.countDown();

        stopAllGrids();
    }

    /**
     * Verify that there's no race in grid cache version generation between get and put.
     * For more details see
     * {@code GridCacheVersionGenerationWithCacheStorageTest#checkGridCacheVersionsGenerationOrder()}
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheVersionGenerationWithCacheStoreGetPut() throws Exception {
        checkGridCacheVersionsGenerationOrder(
            (IgniteEx ign) -> {
                ign.cache(DEFAULT_CACHE_NAME).get(0);

                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                ign.cache(DEFAULT_CACHE_NAME).put(0, 0);
            },
            Collections.singleton(0)
        );
    }

    /**
     * Verify that there's no race in grid cache version generation between getAll and putAll.
     * For more details see
     * {@code GridCacheVersionGenerationWithCacheStorageTest#checkGridCacheVersionsGenerationOrder()}
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheVersionGenerationWithCacheStoreGetAllPutAll() throws Exception {
        checkGridCacheVersionsGenerationOrder(
            (IgniteEx ign) -> {
                ign.cache(DEFAULT_CACHE_NAME).getAll(IntStream.range(0, 2).boxed().collect(Collectors.toSet()));

                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                ign.cache(DEFAULT_CACHE_NAME).putAll(
                    IntStream.range(0, 2).boxed().collect(Collectors.toMap(Function.identity(), i -> i)));
            },
            IntStream.range(0, 2).boxed().collect(Collectors.toSet()));
    }

    /**
     * Verify that there's no race in grid cache version generation between getAsync and putAsync.
     * For more details see
     * {@code GridCacheVersionGenerationWithCacheStorageTest#checkGridCacheVersionsGenerationOrder()}
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheVersionGenerationWithCacheStoreGetAsyncPutAsync() throws Exception {
        checkGridCacheVersionsGenerationOrder(
            (IgniteEx ign) -> {
                ign.cache(DEFAULT_CACHE_NAME).getAsync(0);

                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                ign.cache(DEFAULT_CACHE_NAME).putAsync(0, 0);
            },
            Collections.singleton(0)
        );
    }

    /**
     * Verify that there's no race in grid cache version generation between getAllAsync and putAllAsync.
     * For more details see
     * {@code GridCacheVersionGenerationWithCacheStorageTest#checkGridCacheVersionsGenerationOrder()}
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheVersionGenerationWithCacheStoreGetAllAsyncPutAllAsync() throws Exception {
        checkGridCacheVersionsGenerationOrder(
            (IgniteEx ign) -> {
                ign.cache(DEFAULT_CACHE_NAME).getAllAsync(IntStream.range(0, 2).boxed().collect(Collectors.toSet()));

                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                ign.cache(DEFAULT_CACHE_NAME).putAllAsync(
                    IntStream.range(0, 2).boxed().collect(Collectors.toMap(Function.identity(), i -> i)));
            },
            IntStream.range(0, 2).boxed().collect(Collectors.toSet())
        );
    }

    /**
     * <or>
     *     <li>Start node.</li>
     *     <li>Start one more node asyncronously</li>
     *     <li>With the help of {@link PartitionsExchangeAware) slow down exchange in order to run some cache operations
     *      after discovery message processing but before exchange topology lock.</li>
     * </or>
     *
     * Ensure that topology version of entry and current topology version* are equals after exchange
     * and operation were finished.
     *
     * @param actions Actions to check: get, put etc.
     * @throws Exception
     */
    private void checkGridCacheVersionsGenerationOrder(Consumer<IgniteEx> actions, Set<Integer> keySetToCheck)
        throws Exception {

        IgniteEx ign = startGrid(0);

        ign.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    latch.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        IgniteInternalFuture<?> newNodeJoinFut = GridTestUtils.runAsync(() -> startGrid(1));

        waitForCondition(() -> (ignite(0).context().discovery().topologyVersion() == 2), 10_000);

        assertEquals(2, ignite(0).context().discovery().topologyVersion());

        assertEquals(1,
            ignite(0).cachex(DEFAULT_CACHE_NAME).context().topology().readyTopologyVersion().topologyVersion());

        actions.accept(ign);

        latch.countDown();

        newNodeJoinFut.get();

        long expTop = (ign.context().cache().cache(DEFAULT_CACHE_NAME).
            context().kernalContext().discovery().gridStartTime() - TOP_VER_BASE_TIME) / 1000 + 1;

        ign.cache(DEFAULT_CACHE_NAME).getEntries(keySetToCheck).stream().
            map(CacheEntry::version).forEach(
                v -> assertEquals(expTop, ((GridCacheVersion)v).topologyVersion()));
    }

    /**
     * Test store.
     */
    private static class TestStore extends CacheStoreAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer load(Integer key) {
            assert key != null;

            return key;
        }
        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry<? extends Integer, ? extends Integer> e) {
            // No-op;
        }
        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op;
        }
    }
}
