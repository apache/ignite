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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests that system transactions do not interact with user transactions.
 */
public class IgniteCacheSystemTransactionsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 4;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (String cacheName : new String[] {DEFAULT_CACHE_NAME, CU.UTILITY_CACHE_NAME}) {
            IgniteKernal kernal = (IgniteKernal)ignite(0);

            GridCacheAdapter<Object, Object> cache = kernal.context().cache().internalCache(cacheName);

            cache.removeAll(F.asList("1", "2", "3"));
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(defaultCacheConfiguration().setAtomicityMode(TRANSACTIONAL));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSystemTxInsideUserTx() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-10473", MvccFeatureChecker.forcedMvcc());

        IgniteKernal ignite = (IgniteKernal)grid(0);

        IgniteCache<Object, Object> jcache = ignite.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            jcache.get("1");
            jcache.put("1", "11");

            IgniteInternalCache<Object, Object> utilityCache = ignite.context().cache().utilityCache();

            utilityCache.getAndPutIfAbsent("2", "2");

            try (GridNearTxLocal itx = utilityCache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                assertEquals(null, utilityCache.get("1"));
                assertEquals("2", utilityCache.get("2"));
                assertEquals(null, utilityCache.get("3"));

                utilityCache.getAndPut("3", "3");

                itx.commit();
            }

            jcache.put("2", "22");

            tx.commit();
        }

        checkTransactionsCommitted();

        checkEntries(DEFAULT_CACHE_NAME, "1", "11", "2", "22", "3", null);
        checkEntries(CU.UTILITY_CACHE_NAME, "1", null, "2", "2", "3", "3");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGridNearTxLocalDuplicateAsyncCommit() throws Exception {
        IgniteKernal ignite = (IgniteKernal)grid(0);

        IgniteInternalCache<Object, Object> utilityCache = ignite.context().cache().utilityCache();

        try (GridNearTxLocal itx = MvccFeatureChecker.forcedMvcc() ?
            utilityCache.txStartEx(PESSIMISTIC, REPEATABLE_READ) :
            utilityCache.txStartEx(OPTIMISTIC, SERIALIZABLE)) {
            utilityCache.put("1", "1");

            itx.commitNearTxLocalAsync();
            itx.commitNearTxLocalAsync().get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkTransactionsCommitted() throws Exception {
        for (int i = 0; i < NODES_CNT; i++) {
            IgniteKernal kernal = (IgniteKernal)grid(i);

            IgniteTxManager tm = kernal.context().cache().context().tm();

            Map map = U.field(tm, "threadMap");

            assertEquals(0, map.size());

            map = U.field(tm, "sysThreadMap");

            assertEquals(0, map.size());

            map = U.field(tm, "idMap");

            assertEquals(0, map.size());
        }
    }

    /**
     * @param cacheName Cache to check.
     * @param vals Key-value pairs.
     * @throws Exception If failed.
     */
    private void checkEntries(String cacheName, Object... vals) throws Exception {
        for (int g = 0; g < NODES_CNT; g++) {
            IgniteKernal kernal = (IgniteKernal)grid(g);

            GridCacheAdapter<Object, Object> cache = kernal.context().cache().internalCache(cacheName);

            for (int i = 0; i < vals.length; i += 2) {
                Object key = vals[i];
                Object val = vals[i + 1];

                GridCacheEntryEx entry = cache.peekEx(key);

                if (entry != null) {
                    assertFalse("Entry is locked [g=" + g + ", cacheName=" + cacheName + ", entry=" + entry + ']',
                        entry.lockedByAny());

                    assertEquals("Invalid entry value [g=" + g + ", cacheName=" + cacheName + ", entry=" + entry + ']',
                        val, entry.rawGet().value(cache.context().cacheObjectContext(), false));
                }
            }
        }
    }
}
