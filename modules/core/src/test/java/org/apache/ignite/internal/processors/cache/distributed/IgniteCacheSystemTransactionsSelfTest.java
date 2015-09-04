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
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests that system transactions do not interact with user transactions.
 */
public class IgniteCacheSystemTransactionsSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setAtomicityMode(TRANSACTIONAL);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (String cacheName : new String[] {null, CU.UTILITY_CACHE_NAME, CU.MARSH_CACHE_NAME}) {
            IgniteKernal kernal = (IgniteKernal)ignite(0);

            GridCacheAdapter<Object, Object> cache = kernal.context().cache().internalCache(cacheName);

            cache.removeAll(F.asList("1", "2", "3"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSystemTxInsideUserTx() throws Exception {
        IgniteKernal ignite = (IgniteKernal)grid(0);

        IgniteCache<Object, Object> jcache = ignite.cache(null);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            jcache.get("1");
            jcache.put("1", "11");

            IgniteInternalCache<Object, Object> utilityCache = ignite.context().cache().utilityCache();

            utilityCache.getAndPutIfAbsent("2", "2");

            try (IgniteInternalTx itx = utilityCache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
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

        checkEntries(null,                  "1", "11", "2", "22", "3", null);
        checkEntries(CU.UTILITY_CACHE_NAME, "1", null, "2", "2",  "3", "3");
    }

    /**
     * @throws Exception If failed.
     */
    public void testMarshallerCacheShouldNotStartTx() throws Exception {
        IgniteKernal ignite = (IgniteKernal)grid(0);

        final GridCacheAdapter<String,String> marshallerCache = (GridCacheAdapter<String, String>)(GridCacheAdapter)
            ignite.context().cache().marshallerCache();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return marshallerCache.txStartEx(PESSIMISTIC, REPEATABLE_READ);
            }
        }, IgniteException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkTransactionsCommitted() throws Exception {
        for (int i = 0; i < gridCount(); i++) {
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
        for (int g = 0; g < gridCount(); g++) {
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