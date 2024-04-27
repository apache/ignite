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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * TTL manager self test.
 */
@RunWith(Parameterized.class)
public class GridCacheTtlManagerSelfTest extends GridCommonAbstractTest {
    /** Test cache mode. */
    protected CacheMode cacheMode;

    /** */
    @Parameterized.Parameter
    public boolean pds;

    /** */
    @Parameterized.Parameters(name = "pds={0}")
    public static Collection<?> parameters() {
        return F.asList(false, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(pds)));

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setEagerTtl(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedTtl() throws Exception {
        checkTtl(PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedTtl() throws Exception {
        checkTtl(REPLICATED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    private void checkTtl(CacheMode mode) throws Exception {
        cacheMode = mode;

        final IgniteKernal g = (IgniteKernal)startGrid(0);

        g.cluster().state(ClusterState.ACTIVE);

        try {
            final String key = "key";

            g.cache(DEFAULT_CACHE_NAME).withExpiryPolicy(
                    new TouchedExpiryPolicy(new Duration(MILLISECONDS, 1000))).put(key, 1);

            assertEquals(1, g.cache(DEFAULT_CACHE_NAME).get(key));

            U.sleep(1100);

            GridTestUtils.retryAssert(log, 10, 100, new CAX() {
                @Override public void applyx() {
                    // Check that no more entries left in the map.
                    assertNull(g.cache(DEFAULT_CACHE_NAME).get(key));

                    if (!g.internalCache(DEFAULT_CACHE_NAME).context().deferredDelete())
                        assertNull(g.internalCache(DEFAULT_CACHE_NAME).map().getEntry(g.internalCache(DEFAULT_CACHE_NAME).context(),
                            g.internalCache(DEFAULT_CACHE_NAME).context().toCacheKeyObject(key)));
                }
            });
        }
        finally {
            stopAllGrids();
            cleanPersistenceDir();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedRemove() throws Exception {
        checkRemove(PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedRemove() throws Exception {
        checkRemove(REPLICATED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    private void checkRemove(CacheMode mode) throws Exception {
        cacheMode = mode;

        Map<String, Integer> calls = new ConcurrentHashMap<>();

        BPlusTree.testHndWrapper = (tree, hnd) -> {
            if (tree instanceof PendingEntriesTree) {
                return new PageHandler<Object, BPlusTree.Result>() {
                    @Override public BPlusTree.Result run(
                        int cacheId,
                        long pageId,
                        long page,
                        long pageAddr,
                        PageIO io,
                        Boolean walPlc,
                        Object arg,
                        int lvl,
                        IoStatisticsHolder statHolder
                    ) throws IgniteCheckedException {
                        calls.merge(arg.getClass().getSimpleName(), 1, Integer::sum);

                        return ((PageHandler<Object, BPlusTree.Result>)hnd).run(cacheId, pageId, page, pageAddr, io,
                            walPlc, arg, lvl, statHolder);
                    }

                    @Override public boolean releaseAfterWrite(
                        int cacheId,
                        long pageId,
                        long page,
                        long pageAddr,
                        Object arg,
                        int intArg
                    ) {
                        return ((PageHandler<Object, BPlusTree.Result>)hnd)
                            .releaseAfterWrite(cacheId, pageId, page, pageAddr, arg, intArg);
                    }
                };
            }

            return hnd;
        };

        try (IgniteEx g = startGrid(0)) {
            final String key = "key";

            final int records = 1500;

            g.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Object, Object> cache = g.cache(DEFAULT_CACHE_NAME).withExpiryPolicy(
                new CreatedExpiryPolicy(new Duration(MILLISECONDS, 1000)));

            IntStream.range(0, records).forEach(x -> cache.put(key + x, x));

            assertTrue(GridTestUtils.waitForCondition(
                () -> {
                    try {
                        return g.context().cache().cache(DEFAULT_CACHE_NAME).context().ttl().pendingSize() == 0;
                    }
                    catch (Exception e) {
                        throw new IgniteException(e);
                    }
                }, 5_000L)
            );

            log.info("Invocation counts\n" + calls.keySet().stream()
                .map(k -> k + ": " + calls.get(k))
                .collect(Collectors.joining("\n")));

            assertNotNull(calls.get("RemoveRange"));
            assertNull(calls.get("Remove"));

            IntStream.range(0, records).forEach(x -> assertNull(cache.get(key + x)));
        }
        finally {
            BPlusTree.testHndWrapper = null;
            cleanPersistenceDir();
        }
    }
}
