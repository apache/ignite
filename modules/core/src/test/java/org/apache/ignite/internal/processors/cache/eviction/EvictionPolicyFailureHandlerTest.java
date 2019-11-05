/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.eviction;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Sorted eviction policy tests.
 */
public class EvictionPolicyFailureHandlerTest extends GridCommonAbstractTest {
    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, GridAbstractTest.log);

    /** Node failure. */
    private AtomicBoolean nodeFailure = new AtomicBoolean(false);

    /**
     *
     */
    private boolean oom = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler((ignite, failureCtx) -> {
            nodeFailure.set(true);

            return false;
        });

        SortedEvictionPolicy<String, String> plc = new SortedEvictionPolicy<String, String>()
            .setMaxSize(3)
            .setBatchSize(10)
            .setMaxMemorySize(10);

        cfg.setGridLogger(log);

        cfg.setCacheConfiguration(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setEvictionPolicy(oom ? new ThrowableEvictionPolicy() : plc)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setOnheapCacheEnabled(true));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /**
     * We expect that localPeek produces an exception, but the entry evict returns false because the transaction locks
     * this entry. After transaction commit, the entry will be evicted.
     */
    @Test
    public void testCacheMapDoesNotContainsWrongEntityAfterTransaction() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        LogListener lsnr = LogListener.matches(s -> s.contains("Eviction manager caught an error"))
            .times(1).build();

        log.registerListener(lsnr);

        IgniteEx node = startGrid(0);

        IgniteEx client = startClientGrid(1);

        GridCacheAdapter<Object, Object> cache = ((IgniteKernal)node).internalCache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        CountDownLatch locPeekFinished = new CountDownLatch(1);
        CountDownLatch txStarted = new CountDownLatch(1);
        CountDownLatch txFinished = new CountDownLatch(1);

        GridTestUtils.runAsync(() -> {
            IgniteCache<Object, Object> cache1 = client.cache(DEFAULT_CACHE_NAME);

            IgniteTransactions transactions = client.transactions();

            try (Transaction tx = transactions.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache1.put(2.1, 2.4);

                txStarted.countDown();

                locPeekFinished.await();

                tx.commit();
            }
            catch (Exception ignore) {
            }

            txFinished.countDown();

        }, "tx-thread");

        txStarted.await();

        try {
            cache.localPeek(2.1, new CachePeekMode[] {CachePeekMode.ONHEAP});
        }
        catch (Exception ignore) {
        }

        locPeekFinished.countDown();

        assertTrue(lsnr.check(10_000));

        txFinished.await();

        assertFalse(cache.map().entrySet(cache.context().cacheId()).stream()
            .anyMatch(e -> new Double(2.1).equals(e.key().value(null, false)))
        );

        assertTrue(node.cluster().active());
    }

    /**
     * We skip {@link RuntimeException}, but {@link Error} should trigger {@link FailureHandler}.
     */
    @Test
    public void testErrorShouldCallErrorHandler() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        oom = true;

        IgniteEx node1 = startGrid(0);

        IgniteEx node2 = startGrid(1);

        GridCacheAdapter<Object, Object> cache = ((IgniteKernal)node2).internalCache(DEFAULT_CACHE_NAME);

        Affinity<Object> affinity = cache.affinity();

        try {
            for (int i = 0; i < 1000; i++) {
                if (affinity.isPrimary(node1.localNode(), i))
                    cache.put(i, 1);
            }
        }
        catch (Throwable ignore) {
        }

        assertTrue(nodeFailure.get());
    }

    /**
     *
     */
    @Test
    public void testFailureHandlerShouldNotCallOnRuntimeException() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        IgniteEx node1 = startGrid(0);

        IgniteEx node2 = startGrid(1);

        GridCacheAdapter<Object, Object> cache = ((IgniteKernal)node2).internalCache(DEFAULT_CACHE_NAME);

        Affinity<Object> affinity = cache.affinity();

        for (int i = 0; i < 1000; i++) {
            if (affinity.isPrimary(node1.localNode(), i))
                cache.put(i, 1);
        }

        for (int i = 0; i < 1000; i++) {
            double d = (double)i;

            if (affinity.isPrimary(node1.localNode(), d))
                cache.put(d, 1);
        }

        assertFalse(cache.map().entrySet(cache.context().cacheId()).stream()
            .anyMatch(e -> Double.class.isInstance(e.key().value(null, false)))
        );

        assertFalse(nodeFailure.get());
    }

    /**
     *
     */
    private static class ThrowableEvictionPolicy implements EvictionPolicy<Integer, Integer>, Serializable {
        /** {@inheritDoc} */
        @Override public synchronized void onEntryAccessed(boolean rmv, EvictableEntry<Integer, Integer> e) {
            throw new OutOfMemoryError("Test");
        }
    }
}
