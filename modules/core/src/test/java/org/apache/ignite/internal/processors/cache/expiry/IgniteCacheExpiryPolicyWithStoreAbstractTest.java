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

package org.apache.ignite.internal.processors.cache.expiry;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListenerFuture;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
public abstract class IgniteCacheExpiryPolicyWithStoreAbstractTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Factory<CacheStore> cacheStoreFactory() {
        return new TestStoreFactory();
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setExpiryPolicyFactory(new TestExpiryPolicyFactory());

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadAll() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        final Integer key = primaryKey(cache);

        storeMap.put(key, 100);

        try {
            CompletionListenerFuture fut = new CompletionListenerFuture();

            cache.loadAll(F.asSet(key), false, fut);

            fut.get();

            checkTtl(key, 500, false);

            assertEquals((Integer)100, cache.localPeek(key, CachePeekMode.ONHEAP));

            U.sleep(600);

            checkExpired(key);

            cache = cache.withExpiryPolicy(new ExpiryPolicy() {
                @Override public Duration getExpiryForCreation() {
                    return new Duration(TimeUnit.MILLISECONDS, 501);
                }

                @Override public Duration getExpiryForAccess() {
                    return new Duration(TimeUnit.MILLISECONDS, 601);
                }

                @Override public Duration getExpiryForUpdate() {
                    return new Duration(TimeUnit.MILLISECONDS, 701);
                }
            });

            fut = new CompletionListenerFuture();

            cache.loadAll(F.asSet(key), false, fut);

            fut.get();

            checkTtl(key, 501, false);
        }
        finally {
            cache.removeAll();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCache() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        final Integer key = primaryKey(cache);

        storeMap.put(key, 100);

        try {
            cache.loadCache(null);

            checkTtl(key, 500, false);

            assertEquals((Integer)100, cache.localPeek(key, CachePeekMode.ONHEAP));

            U.sleep(600);

            checkExpired(key);
        }
        finally {
            cache.removeAll();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadThrough() throws Exception {
        if (atomicityMode() == CacheAtomicityMode.TRANSACTIONAL)
            fail("https://issues.apache.org/jira/browse/IGNITE-821");

        IgniteCache<Integer, Integer> cache = jcache(0);

        final Integer key = primaryKeys(cache, 1, 100_000).get(0);

        storeMap.put(key, 100);

        try {
            Integer res = cache.invoke(key, new EntryProcessor<Integer, Integer, Integer>() {
                @Override public Integer process(MutableEntry<Integer, Integer> e, Object... args) {
                    return e.getValue();
                }
            });

            assertEquals((Integer)100, res);

            checkTtl(key, 500, true);

            assertEquals((Integer)100, cache.localPeek(key, CachePeekMode.ONHEAP));

            U.sleep(600);

            checkExpired(key);
        }
        finally {
            cache.removeAll();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetReadThrough() throws Exception {
        getReadThrough(false, null, null);
        getReadThrough(true, null, null);
    }

    /**
     * @throws Exception If failed.
     */
    protected void getReadThrough(boolean withExcPlc,
        TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation) throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        if (withExcPlc)
            cache = cache.withExpiryPolicy(new ExpiryPolicy() {
                @Override public Duration getExpiryForCreation() {
                    return new Duration(TimeUnit.MILLISECONDS, 501);
                }

                @Override public Duration getExpiryForAccess() {
                    return new Duration(TimeUnit.MILLISECONDS, 601);
                }

                @Override public Duration getExpiryForUpdate() {
                    return new Duration(TimeUnit.MILLISECONDS, 701);
                }
            });

        Integer prim = primaryKeys(cache, 1, 1000).get(0);
        Integer back = backupKeys(cache, 1, 1000).get(0);
        Integer near = nearKeys(cache, 1, 1000).get(0);

        Set<Integer> prims = new HashSet<>(primaryKeys(cache, 10, prim + 1));
        Set<Integer> backs = new HashSet<>(backupKeys(cache, 10, back + 1));
        Set<Integer> nears = new HashSet<>(nearKeys(cache, 10, near + 1));

        Set<Integer> keys = new HashSet<>();

        keys.add(prim);
        keys.add(back);
        keys.add(near);

        keys.addAll(prims);
        keys.addAll(backs);
        keys.addAll(nears);

        for (Integer key : keys)
            storeMap.put(key, key);

        IgniteTransactions transactions = grid(0).transactions();

        Transaction tx = txConcurrency != null ? transactions.txStart(txConcurrency, txIsolation) : null;

        try {
            Collection<Integer> singleKeys = new HashSet<>();

            singleKeys.add(prim);
            singleKeys.add(back);
            singleKeys.add(near);

            assertEquals(3, singleKeys.size());

            for (Integer key : singleKeys)
                assertEquals(key, cache.get(key));

            Map<Integer, Integer> res = new HashMap<>();

            res.putAll(cache.getAll(prims));
            res.putAll(cache.getAll(backs));
            res.putAll(cache.getAll(nears));

            assertEquals(30, res.size());

            for (Map.Entry<Integer, Integer> e : res.entrySet())
                assertEquals(e.getKey(), e.getValue());
        }
        finally {
            if (tx != null)
                tx.rollback();
        }

        for (Integer key : keys)
            checkTtl(key, withExcPlc ? 501 : 500, true);

        U.sleep(600);

        for (Integer key : keys)
            checkExpired(key);
    }

    /**
     * @param key Key.
     */
    private void checkExpired(Integer key) {
        for (int i = 0; i < gridCount(); i++) {
            IgniteCache<Integer, Integer> cache = jcache(i);

            assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));
        }
    }

    /**
     * @param key Key.
     * @param ttl TTL.
     * @param primaryOnly If {@code true} expect entries only on primary node.
     * @throws Exception If failed.
     */
    private void checkTtl(Object key, final long ttl, boolean primaryOnly) throws Exception {
        boolean found = false;

        for (int i = 0; i < gridCount(); i++) {
            IgniteKernal grid = (IgniteKernal)grid(i);

            GridCacheAdapter<Object, Object> cache = grid.context().cache().internalCache();

            GridCacheEntryEx e = cache.peekEx(key);

            if (e == null && cache.context().isNear())
                e = cache.context().near().dht().peekEx(key);

            if (e == null) {
                if (primaryOnly)
                    assertTrue("Not found " + key, !grid.affinity(null).isPrimary(grid.localNode(), key));
                else
                    assertTrue("Not found " + key, !grid.affinity(null).isPrimaryOrBackup(grid.localNode(), key));
            }
            else {
                found = true;

                assertEquals("Unexpected ttl [grid=" + i + ", key=" + key +']', ttl, e.ttl());

                if (ttl > 0)
                    assertTrue(e.expireTime() > 0);
                else
                    assertEquals(0, e.expireTime());
            }
        }

        assertTrue(found);
    }

    /**
     *
     */
    private static class TestExpiryPolicyFactory implements Factory<ExpiryPolicy> {
        /** {@inheritDoc} */
        @Override public ExpiryPolicy create() {
            return new ExpiryPolicy() {
                @Override public Duration getExpiryForCreation() {
                    return new Duration(TimeUnit.MILLISECONDS, 500);
                }

                @Override public Duration getExpiryForAccess() {
                    return new Duration(TimeUnit.MILLISECONDS, 600);
                }

                @Override public Duration getExpiryForUpdate() {
                    return new Duration(TimeUnit.MILLISECONDS, 700);
                }
            };
        }
    }
}