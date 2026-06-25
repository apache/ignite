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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** Tests operation context propagation when a versioned cache entry is locked in a transaction. */
public class CacheOperationContextTransactionalLockTest extends GridCommonAbstractTest {
    /** */
    private static final int KEY = 1;

    /** */
    private static Ignite ignite;

    /** */
    private static IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid(0);

        cache = ignite.createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        cache.put(KEY, KEY);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        ignite = null;
        cache = null;

        super.afterTestsStopped();
    }

    /** Checks the defaults used when there is no operation context. */
    @Test
    public void testDefaultOperationContext() throws Exception {
        assertTxEntryFlags(internalCache(), false, false, false, false);
    }

    /** Checks propagation of the skip-store flag. */
    @Test
    public void testSkipStoreOperationContext() throws Exception {
        assertTxEntryFlags(internalCache().withSkipStore(), true, false, false, false);
    }

    /** Checks propagation of the skip-read-through flag. */
    @Test
    public void testSkipReadThroughOperationContext() throws Exception {
        assertTxEntryFlags(internalCache().withSkipReadThrough(), false, true, false, false);
    }

    /** Checks propagation of the keep-binary-in-interceptor flag. */
    @Test
    public void testKeepBinaryInInterceptorOperationContext() throws Exception {
        assertTxEntryFlags(internalCache().withKeepBinaryInInterceptor(), false, false, true, false);
    }

    /**
     * Locks a versioned entry and checks flags stored in its transaction entry.
     *
     * @param internalCache Internal cache projection used to acquire the lock.
     * @param skipStore Expected skip-store flag.
     * @param skipReadThrough Expected skip-read-through flag.
     * @param keepBinaryInInterceptor Expected keep-binary-in-interceptor flag.
     * @param keepBinary Expected keep-binary flag.
     * @throws Exception If failed.
     */
    private void assertTxEntryFlags(
        IgniteInternalCache<Integer, Integer> internalCache,
        boolean skipStore,
        boolean skipReadThrough,
        boolean keepBinaryInInterceptor,
        boolean keepBinary
    ) throws Exception {
        CacheEntry<Integer, Integer> entry = cache.getEntry(KEY);

        assertNotNull(entry);
        assertNotNull(entry.version());

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            assertTrue(internalCache.lockTxEntry(entry, 0));

            GridCacheContext<Integer, Integer> ctx = internalCache.context();
            IgniteTxKey txKey = ctx.txKey(ctx.toCacheKeyObject(KEY));
            IgniteTxEntry txEntry = internalCache.tx().entry(txKey);

            assertNotNull(txEntry);
            assertEquals(skipStore, txEntry.skipStore());
            assertEquals(skipReadThrough, txEntry.skipReadThrough());
            assertEquals(keepBinaryInInterceptor, txEntry.keepBinaryInInterceptor());
            assertEquals(keepBinary, txEntry.keepBinary());

            tx.commit();
        }
    }

    /** Returns the cache's internal proxy. */
    @SuppressWarnings("unchecked")
    private IgniteInternalCache<Integer, Integer> internalCache() {
        return cache.unwrap(IgniteCacheProxy.class).internalProxy();
    }
}
