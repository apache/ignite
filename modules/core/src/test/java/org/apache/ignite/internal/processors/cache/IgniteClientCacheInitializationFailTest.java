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
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.query.ColumnInformation;
import org.apache.ignite.internal.processors.query.DummyQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.TableInformation;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test checks whether cache initialization error on client side
 * doesn't causes hangs and doesn't impact other caches.
 */
public class IgniteClientCacheInitializationFailTest extends GridCommonAbstractTest {
    /** Failed cache name. */
    private static final String CACHE_NAME = "cache";

    /** Atomic cache name. */
    private static final String ATOMIC_CACHE_NAME = "atomic-cache";

    /** Tx cache name. */
    private static final String TX_CACHE_NAME = "tx-cache";

    /** Mvcc tx cache name. */
    private static final String MVCC_TX_CACHE_NAME = "mvcc-tx-cache";

    /** Near atomic cache name. */
    private static final String NEAR_ATOMIC_CACHE_NAME = "near-atomic-cache";

    /** Near tx cache name. */
    private static final String NEAR_TX_CACHE_NAME = "near-tx-cache";

    /** Near mvcc tx cache name. */
    private static final String NEAR_MVCC_TX_CACHE_NAME = "near-mvcc-tx-cache";

    /** Failed caches. */
    private static final Set<String> FAILED_CACHES;

    static {
        Set<String> set = new HashSet<>();

        set.add(ATOMIC_CACHE_NAME);
        set.add(TX_CACHE_NAME);
        set.add(NEAR_ATOMIC_CACHE_NAME);
        set.add(NEAR_TX_CACHE_NAME);
        set.add(MVCC_TX_CACHE_NAME);
        set.add(NEAR_MVCC_TX_CACHE_NAME);

        FAILED_CACHES = Collections.unmodifiableSet(set);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid("server");
        startClientGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.contains("server")) {
            CacheConfiguration<Integer, String> ccfg1 = new CacheConfiguration<>();

            ccfg1.setIndexedTypes(Integer.class, String.class);
            ccfg1.setName(ATOMIC_CACHE_NAME);
            ccfg1.setAtomicityMode(CacheAtomicityMode.ATOMIC);

            CacheConfiguration<Integer, String> ccfg2 = new CacheConfiguration<>();

            ccfg2.setIndexedTypes(Integer.class, String.class);
            ccfg2.setName(TX_CACHE_NAME);
            ccfg2.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            CacheConfiguration<Integer, String> ccfg3 = new CacheConfiguration<>();

            ccfg3.setIndexedTypes(Integer.class, String.class);
            ccfg3.setName(MVCC_TX_CACHE_NAME);
            ccfg3.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

            cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3);
        }
        else
            GridQueryProcessor.idxCls = FailedIndexing.class;

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicCacheInitialization() throws Exception {
        checkCacheInitialization(ATOMIC_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransactionalCacheInitialization() throws Exception {
        checkCacheInitialization(TX_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccTransactionalCacheInitialization() throws Exception {
        checkCacheInitialization(MVCC_TX_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicNearCacheInitialization() throws Exception {
        checkCacheInitialization(NEAR_ATOMIC_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransactionalNearCacheInitialization() throws Exception {
        checkCacheInitialization(NEAR_TX_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    public void testMvccTransactionalNearCacheInitialization() throws Exception {
        checkCacheInitialization(NEAR_MVCC_TX_CACHE_NAME);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void checkCacheInitialization(final String cacheName) throws Exception {
        Ignite client = grid("client");

        checkFailedCache(client, cacheName);

        checkFineCache(client, CACHE_NAME + 1);

        assertNull(((IgniteKernal)client).context().cache().cache(cacheName));

        checkFineCache(client, CACHE_NAME + 2);
    }

    /**
     * @param client Client.
     * @param cacheName Cache name.
     */
    private void checkFineCache(Ignite client, String cacheName) {
        IgniteCache<Integer, String> cache = client.getOrCreateCache(cacheName);

        cache.put(1, "1");

        assertEquals("1", cache.get(1));
    }

    /**
     * @param client Client.
     */
    @SuppressWarnings({"ThrowableNotThrown"})
    private void checkFailedCache(final Ignite client, final String cacheName) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache<Integer, String> cache;

                // Start cache with near enabled.
                if (NEAR_ATOMIC_CACHE_NAME.equals(cacheName) || NEAR_TX_CACHE_NAME.equals(cacheName) ||
                    NEAR_MVCC_TX_CACHE_NAME.equals(cacheName)) {
                    CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<Integer, String>(cacheName)
                        .setNearConfiguration(new NearCacheConfiguration<Integer, String>()).setSqlSchema("test");

                    if (NEAR_TX_CACHE_NAME.equals(cacheName))
                        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
                    else if (NEAR_MVCC_TX_CACHE_NAME.equals(cacheName))
                        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

                    cache = client.getOrCreateCache(ccfg);
                }
                else
                    cache = client.cache(cacheName);

                cache.put(1, "1");

                assertEquals("1", cache.get(1));

                return null;
            }
        }, CacheException.class, null);
    }

    /**
     * To fail on cache start.
     */
    private static class FailedIndexing extends DummyQueryIndexing {
        /** {@inheritDoc} */
        @Override public void registerCache(String cacheName, String schemaName,
            GridCacheContextInfo<?, ?> cacheInfo) throws IgniteCheckedException {
            if (FAILED_CACHES.contains(cacheInfo.name()) && cacheInfo.cacheContext().kernalContext().clientNode())
                throw new IgniteCheckedException("Test query exception " + cacheInfo.name() + " " + new Random().nextInt());
        }

        /** {@inheritDoc} */
        @Override public boolean initCacheContext(GridCacheContext ctx) throws IgniteCheckedException {
            if (FAILED_CACHES.contains(ctx.name()) && ctx.kernalContext().clientNode())
                throw new IgniteCheckedException("Test query exception " + ctx.name() + " " + new Random().nextInt());

            return true;
        }

        /** {@inheritDoc} */
        @Override public Collection<TableInformation> tablesInformation(String schemaNamePtrn, String tblNamePtrn,
            String[] tblTypes) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<ColumnInformation> columnsInformation(String schemaNamePtrn, String tblNamePtrn,
            String colNamePtrn) {
            return null;
        }
    }
}
