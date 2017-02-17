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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PAX;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public abstract class IgniteCacheExpiryPolicyAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private static final long TTL_FOR_EXPIRE = 500L;

    /** */
    private Factory<? extends ExpiryPolicy> factory;

    /** */
    private boolean nearCache;

    /** */
    private boolean disableEagerTtl;

    /** */
    private Integer lastKey = 0;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        factory = null;

        storeMap.clear();
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        if (nearCache)
            cfg.setNearConfiguration(new NearCacheConfiguration());

        cfg.setExpiryPolicyFactory(factory);

        cfg.setMemoryMode(memoryMode());

        if (memoryMode() == CacheMemoryMode.OFFHEAP_TIERED)
            cfg.setOffHeapMaxMemory(0);

        if (disableEagerTtl)
            cfg.setEagerTtl(false);

        return cfg;
    }

    /**
     * @return Cache memory mode.
     */
    protected CacheMemoryMode memoryMode() {
        return CacheMemoryMode.ONHEAP_TIERED;
    }

    /**
     * @throws Exception if failed.
     */
    public void testCreateUpdate0() throws Exception {
        startGrids(1);

        long ttl = 60L;

        final String key = "key1";

        final IgniteCache<String, String> cache = jcache();

        for (int i = 0; i < 1000; i++) {
            final IgniteCache<String, String> cache0 = cache.withExpiryPolicy(new ModifiedExpiryPolicy(new Duration(TimeUnit.HOURS, ttl)));

            cache0.put(key, key);

            info("PUT DONE");
        }

        int pSize = grid(0).context().cache().internalCache(null).context().ttl().pendingSize();

        assertTrue("Too many pending entries: " + pSize, pSize <= 1);

        cache.remove(key);

        pSize = grid(0).context().cache().internalCache(null).context().ttl().pendingSize();

        assertEquals(0, pSize);
    }

    /**     * @throws Exception If failed.
     */
    public void testZeroOnCreate() throws Exception {
        factory = CreatedExpiryPolicy.factoryOf(Duration.ZERO);

        startGrids();

        for (final Integer key : keys()) {
            log.info("Test zero duration on create, key: " + key);

            zeroOnCreate(key);
        }
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void zeroOnCreate(Integer key) throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        cache.put(key, 1); // Create with zero duration, should not create cache entry.

        checkNoValue(F.asList(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testZeroOnUpdate() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(null, 0L, null));

        startGrids();

        for (final Integer key : keys()) {
            log.info("Test zero duration on update, key: " + key);

            zeroOnUpdate(key);
        }
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void zeroOnUpdate(Integer key) throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        cache.put(key, 1); // Create.

        assertEquals((Integer)1, cache.get(key));

        cache.put(key, 2); // Update should expire entry.

        checkNoValue(F.asList(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testZeroOnAccess() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(null, null, 0L));

        startGrids();

        for (final Integer key : keys()) {
            log.info("Test zero duration on access, key: " + key);

            zeroOnAccess(key);
        }

        final IgniteCache<Integer, Object> cache = jcache(0);

        Integer key = primaryKey(cache);

        IgniteCache<Integer, Object> cache0 = cache.withExpiryPolicy(new TestPolicy(60_000L, 60_000L, 60_000L));

        cache0.put(key, 1);

        cache.get(key); // Access using get.

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !cache.iterator().hasNext();
            }
        }, 1000);

        assertFalse(cache.iterator().hasNext());

        cache0.put(key, 1);

        assertNotNull(cache.iterator().next()); // Access using iterator.

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !cache.iterator().hasNext();
            }
        }, 1000);

        assertFalse(cache.iterator().hasNext());
    }

    /**
     * @throws Exception If failed.
     */
    public void testZeroOnAccessEagerTtlDisabled() throws Exception {
        disableEagerTtl = true;

        testZeroOnAccess();
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void zeroOnAccess(Integer key) throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        cache.put(key, 1); // Create.

        assertEquals((Integer)1, cache.get(key)); // Access should expire entry.

        waitExpired(F.asList(key));

        assertFalse(cache.iterator().hasNext());
    }

    /**
     * @throws Exception If failed.
     */
    public void testEternal() throws Exception {
        factory = EternalExpiryPolicy.factoryOf();

        ExpiryPolicy plc = factory.create();

        assertTrue(plc.getExpiryForCreation().isEternal());
        assertNull(plc.getExpiryForUpdate());
        assertNull(plc.getExpiryForAccess());

        startGrids();

        for (final Integer key : keys()) {
            log.info("Test eternalPolicy, key: " + key);

            eternal(key);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullFactory() throws Exception {
        factory = null; // Should work as eternal.

        startGrids();

        for (final Integer key : keys()) {
            log.info("Test eternalPolicy, key: " + key);

            eternal(key);
        }
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void eternal(Integer key) throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        cache.put(key, 1); // Create.

        checkTtl(key, 0);

        assertEquals((Integer) 1, cache.get(key)); // Get.

        checkTtl(key, 0);

        cache.put(key, 2); // Update.

        checkTtl(key, 0);

        assertTrue(cache.remove(key)); // Remove.

        cache.withExpiryPolicy(new TestPolicy(60_000L, null, null)).put(key, 1); // Create with custom.

        checkTtl(key, 60_000L);

        cache.put(key, 2); // Update with eternal, should not change ttl.

        checkTtl(key, 60_000L);

        cache.withExpiryPolicy(new TestPolicy(null, TTL_FOR_EXPIRE, null)).put(key, 1); // Update with custom.

        checkTtl(key, TTL_FOR_EXPIRE);

        waitExpired(key);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccess() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, 61_000L, 62_000L));

        startGrids();

        for (final Integer key : keys()) {
            log.info("Test access [key=" + key + ']');

            access(key);
        }

        accessGetAll();

        for (final Integer key : keys()) {
            log.info("Test filterAccessRemove access [key=" + key + ']');

            filterAccessRemove(key);
        }

        for (final Integer key : keys()) {
            log.info("Test filterAccessReplace access [key=" + key + ']');

            filterAccessReplace(key);
        }

        if (atomicityMode() == TRANSACTIONAL) {
            TransactionConcurrency[] txModes = {PESSIMISTIC};

            for (TransactionConcurrency txMode : txModes) {
                for (final Integer key : keys()) {
                    log.info("Test txGet [key=" + key + ", txMode=" + txMode + ']');

                    txGet(key, txMode);
                }
            }

            for (TransactionConcurrency txMode : txModes) {
                log.info("Test txGetAll [txMode=" + txMode + ']');

                txGetAll(txMode);
            }
        }

        IgniteCache<Integer, Integer> cache = jcache(0);

        Collection<Integer> putKeys = keys();

        info("Put keys: " + putKeys);

        for (final Integer key : putKeys)
            cache.put(key, key);

        Iterator<Cache.Entry<Integer, Integer>> it = cache.iterator();

        List<Integer> itKeys = new ArrayList<>();

        while (it.hasNext())
            itKeys.add(it.next().getKey());

        info("It keys: " + itKeys);

        assertTrue(itKeys.size() >= putKeys.size());

        for (Integer key : itKeys) {
            info("Checking iterator key: " + key);

            checkTtl(key, 62_000L, true);
        }
    }

    /**
     * @param key Key.
     * @param txMode Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void txGet(Integer key, TransactionConcurrency txMode) throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        cache.put(key, 1);

        checkTtl(key, 60_000L);

        try (Transaction tx = ignite(0).transactions().txStart(txMode, REPEATABLE_READ)) {
            assertEquals((Integer)1, cache.get(key));

            tx.commit();
        }

        checkTtl(key, 62_000L, true);

        try (Transaction tx = ignite(0).transactions().txStart(txMode, REPEATABLE_READ)) {
            assertEquals((Integer)1, cache.withExpiryPolicy(new TestPolicy(100L, 200L, 1000L)).get(key));

            tx.commit();
        }

        checkTtl(key, 1000L, true);
    }

    /**
     * @param txMode Transaction concurrency mode.
     * @throws Exception If failed.
     */
    private void txGetAll(TransactionConcurrency txMode) throws Exception {
        IgniteCache<Integer, Integer> cache = jcache(0);

        Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            vals.put(i, i);

        cache.putAll(vals);

        try (Transaction tx = ignite(0).transactions().txStart(txMode, REPEATABLE_READ)) {
            assertEquals(vals, cache.getAll(vals.keySet()));

            tx.commit();
        }

        for (Integer key : vals.keySet())
            checkTtl(key, 62_000L);

        try (Transaction tx = ignite(0).transactions().txStart(txMode, REPEATABLE_READ)) {
            assertEquals(vals, cache.withExpiryPolicy(new TestPolicy(100L, 200L, 1000L)).getAll(vals.keySet()));

            tx.commit();
        }

        for (Integer key : vals.keySet())
            checkTtl(key, 1000L);
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void access(Integer key) throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        cache.put(key, 1);

        checkTtl(key, 60_000L);

        assertEquals((Integer) 1, cache.get(key));

        checkTtl(key, 62_000L, true);

        IgniteCache<Integer, Integer> cache0 = cache.withExpiryPolicy(new TestPolicy(1100L, 1200L, TTL_FOR_EXPIRE));

        assertEquals((Integer)1, cache0.get(key));

        checkTtl(key, TTL_FOR_EXPIRE, true);

        waitExpired(key);

        cache.put(key, 1);

        checkTtl(key, 60_000L);

        Integer res = cache.invoke(key, new GetEntryProcessor());

        assertEquals((Integer)1, res);

        checkTtl(key, 62_000L, true);
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void filterAccessRemove(Integer key) throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        cache.put(key, 1);

        checkTtl(key, 60_000L);

        assertFalse(cache.remove(key, 2)); // Remove fails, access expiry policy should be used.

        checkTtl(key, 62_000L, true);

        assertFalse(cache.withExpiryPolicy(new TestPolicy(100L, 200L, 1000L)).remove(key, 2));

        checkTtl(key, 1000L, true);
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void filterAccessReplace(Integer key) throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        cache.put(key, 1);

        checkTtl(key, 60_000L);

        assertFalse(cache.replace(key, 2, 3)); // Put fails, access expiry policy should be used.

        checkTtl(key, 62_000L, true);

        assertFalse(cache.withExpiryPolicy(new TestPolicy(100L, 200L, 1000L)).remove(key, 2));

        checkTtl(key, 1000L, true);
    }

    /**
     * @throws Exception If failed.
     */
    private void accessGetAll() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            vals.put(i, i);

        cache.removeAll(vals.keySet());

        cache.putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 60_000L);

        Map<Integer, Integer> vals0 = cache.getAll(vals.keySet());

        assertEquals(vals, vals0);

        for (Integer key : vals.keySet())
            checkTtl(key, 62_000L, true);

        vals0 = cache.withExpiryPolicy(new TestPolicy(1100L, 1200L, 1000L)).getAll(vals.keySet());

        assertEquals(vals, vals0);

        for (Integer key : vals.keySet())
            checkTtl(key, 1000L, true);

        waitExpired(vals.keySet());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateUpdate() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, 61_000L, null));

        startGrids();

        for (final Integer key : keys()) {
            log.info("Test createUpdate [key=" + key + ']');

            createUpdate(key, null);
        }

        for (final Integer key : keys()) {
            log.info("Test createUpdateCustomPolicy [key=" + key + ']');

            createUpdateCustomPolicy(key, null);
        }

        createUpdatePutAll(null);

        if (atomicityMode() == TRANSACTIONAL) {
            TransactionConcurrency[] txModes = new TransactionConcurrency[]{PESSIMISTIC, OPTIMISTIC};

            for (TransactionConcurrency tx : txModes) {
                for (final Integer key : keys()) {
                    log.info("Test createUpdate [key=" + key + ", tx=" + tx + ']');

                    createUpdate(key, tx);
                }

                for (final Integer key : keys()) {
                    log.info("Test createUpdateCustomPolicy [key=" + key + ", tx=" + tx + ']');

                    createUpdateCustomPolicy(key, tx);
                }

                createUpdatePutAll(tx);
            }
        }
    }

    /**
     * @param txConcurrency Not null transaction concurrency mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void createUpdatePutAll(@Nullable TransactionConcurrency txConcurrency) throws Exception {
        Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            vals.put(i, i);

        IgniteCache<Integer, Integer> cache = jcache(0);

        cache.removeAll(vals.keySet());

        Transaction tx = startTx(txConcurrency);

        // Create.
        cache.putAll(vals);

        if (tx != null)
            tx.commit();

        for (Integer key : vals.keySet())
            checkTtl(key, 60_000L);

        tx = startTx(txConcurrency);

        // Update.
        cache.putAll(vals);

        if (tx != null)
            tx.commit();

        for (Integer key : vals.keySet())
            checkTtl(key, 61_000L);

        tx = startTx(txConcurrency);

        // Update with provided TTL.
        cache.withExpiryPolicy(new TestPolicy(null, 1000L, null)).putAll(vals);

        if (tx != null)
            tx.commit();

        for (Integer key : vals.keySet())
            checkTtl(key, 1000L);

        waitExpired(vals.keySet());

        tx = startTx(txConcurrency);

        // Try create again.
        cache.putAll(vals);

        if (tx != null)
            tx.commit();

        for (Integer key : vals.keySet())
            checkTtl(key, 60_000L);

        Map<Integer, Integer> newVals = new HashMap<>(vals);

        newVals.put(100_000, 1);

        // Updates and create.
        cache.putAll(newVals);

        for (Integer key : vals.keySet())
            checkTtl(key, 61_000L);

        checkTtl(100_000, 60_000L);

        cache.removeAll(newVals.keySet());
    }

    /**
     * @param key Key.
     * @param txConcurrency Not null transaction concurrency mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void createUpdateCustomPolicy(Integer key, @Nullable TransactionConcurrency txConcurrency)
        throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        assertNull(cache.get(key));

        Transaction tx = startTx(txConcurrency);

        cache.withExpiryPolicy(new TestPolicy(10_000L, 20_000L, 30_000L)).put(key, 1);

        if (tx != null)
            tx.commit();

        checkTtl(key, 10_000L);

        for (int idx = 0; idx < gridCount(); idx++) {
            assertEquals(1, jcache(idx).get(key)); // Try get.

            checkTtl(key, 10_000L);
        }

        tx = startTx(txConcurrency);

        // Update, returns null duration, should not change TTL.
        cache.withExpiryPolicy(new TestPolicy(20_000L, null, null)).put(key, 2);

        if (tx != null)
            tx.commit();

        checkTtl(key, 10_000L);

        tx = startTx(txConcurrency);

        // Update with provided TTL.
        cache.withExpiryPolicy(new TestPolicy(null, TTL_FOR_EXPIRE, null)).put(key, 2);

        if (tx != null)
            tx.commit();

        checkTtl(key, TTL_FOR_EXPIRE);

        waitExpired(key);

        tx = startTx(txConcurrency);

        // Create, returns null duration, should create with 0 TTL.
        cache.withExpiryPolicy(new TestPolicy(null, 20_000L, 30_000L)).put(key, 1);

        if (tx != null)
            tx.commit();

        checkTtl(key, 0L);
    }

    /**
     * @param key Key.
     * @param txConcurrency Not null transaction concurrency mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void createUpdate(Integer key, @Nullable TransactionConcurrency txConcurrency)
        throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        // Run several times to make sure create after remove works as expected.
        for (int i = 0; i < 3; i++) {
            log.info("Iteration: " + i);

            Transaction tx = startTx(txConcurrency);

            cache.put(key, 1); // Create.

            if (tx != null)
                tx.commit();

            checkTtl(key, 60_000L);

            for (int idx = 0; idx < gridCount(); idx++) {
                assertEquals(1, jcache(idx).get(key)); // Try get.

                checkTtl(key, 60_000L);
            }

            tx = startTx(txConcurrency);

            cache.put(key, 2); // Update.

            if (tx != null)
                tx.commit();

            checkTtl(key, 61_000L);

            for (int idx = 0; idx < gridCount(); idx++) {
                assertEquals(2, jcache(idx).get(key)); // Try get.

                checkTtl(key, 61_000L);
            }

            tx = startTx(txConcurrency);

            assertTrue(cache.remove(key));

            if (tx != null)
                tx.commit();

            for (int idx = 0; idx < gridCount(); idx++)
                assertNull(jcache(idx).get(key));
        }
    }

    /**
     * @param txMode Transaction concurrency mode.
     * @return Transaction.
     */
    @Nullable private Transaction startTx(@Nullable TransactionConcurrency txMode) {
        return txMode == null ? null : ignite(0).transactions().txStart(txMode, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearCreateUpdate() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-518");

        if (cacheMode() != PARTITIONED)
            return;

        nearCache = true;

        testCreateUpdate();

        nearReaderUpdate();

        nearPutAll();
    }

    /**
     * @throws Exception If failed.
     */
    private void nearReaderUpdate() throws Exception {
        log.info("Test near reader update.");

        Integer key = nearKeys(jcache(0), 1, 500_000).get(0);

        IgniteCache<Integer, Integer> cache0 = jcache(0);

        assertNotNull(jcache(0).getConfiguration(CacheConfiguration.class).getNearConfiguration());

        cache0.put(key, 1);

        checkTtl(key, 60_000L);

        IgniteCache<Integer, Integer> cache1 = jcache(1);

        if (atomicityMode() == ATOMIC && atomicWriteOrderMode() == CLOCK)
            Thread.sleep(100);

        // Update from another node.
        cache1.put(key, 2);

        checkTtl(key, 61_000L);

        if (atomicityMode() == ATOMIC && atomicWriteOrderMode() == CLOCK)
            Thread.sleep(100);

        // Update from another node with provided TTL.
        cache1.withExpiryPolicy(new TestPolicy(null, TTL_FOR_EXPIRE, null)).put(key, 3);

        checkTtl(key, TTL_FOR_EXPIRE);

        waitExpired(key);

        // Try create again.
        cache0.put(key, 1);

        checkTtl(key, 60_000L);

        if (atomicityMode() == ATOMIC && atomicWriteOrderMode() == CLOCK)
            Thread.sleep(100);

        // Update from near node with provided TTL.
        cache0.withExpiryPolicy(new TestPolicy(null, TTL_FOR_EXPIRE + 1, null)).put(key, 2);

        checkTtl(key, TTL_FOR_EXPIRE + 1);

        waitExpired(key);
    }

    /**
     * @throws Exception If failed.
     */
    private void nearPutAll() throws Exception {
        Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            vals.put(i, i);

        IgniteCache<Integer, Integer> cache0 = jcache(0);

        cache0.removeAll(vals.keySet());

        cache0.putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 60_000L);

        if (atomicityMode() == ATOMIC && atomicWriteOrderMode() == CLOCK)
            Thread.sleep(100);

        IgniteCache<Integer, Integer> cache1 = jcache(1);

        // Update from another node.
        cache1.putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 61_000L);

        if (atomicityMode() == ATOMIC && atomicWriteOrderMode() == CLOCK)
            Thread.sleep(100);

        // Update from another node with provided TTL.
        cache1.withExpiryPolicy(new TestPolicy(null, 1000L, null)).putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 1000L);

        waitExpired(vals.keySet());

        // Try create again.
        cache0.putAll(vals);

        if (atomicityMode() == ATOMIC && atomicWriteOrderMode() == CLOCK)
            Thread.sleep(100);

        // Update from near node with provided TTL.
        cache1.withExpiryPolicy(new TestPolicy(null, 1101L, null)).putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 1101L);

        waitExpired(vals.keySet());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearAccess() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-518");

        if (cacheMode() != PARTITIONED)
            return;

        nearCache = true;

        testAccess();

        Integer key = primaryKeys(jcache(0), 1, 500_000).get(0);

        IgniteCache<Integer, Integer> cache0 = jcache(0);

        cache0.put(key, 1);

        checkTtl(key, 60_000L);

        assertEquals(1, jcache(1).get(key));

        checkTtl(key, 62_000L, true);

        assertEquals(1, jcache(2).withExpiryPolicy(new TestPolicy(1100L, 1200L, TTL_FOR_EXPIRE)).get(key));

        checkTtl(key, TTL_FOR_EXPIRE, true);

        waitExpired(key);

        // Test reader update on get.

        key = nearKeys(jcache(0), 1, 600_000).get(0);

        cache0.put(key, 1);

        checkTtl(key, 60_000L);

        IgniteCache<Object, Object> cache =
            grid(0).affinity(null).isPrimary(grid(1).localNode(), key) ? jcache(1) : jcache(2);

        assertEquals(1, cache.get(key));

        checkTtl(key, 62_000L, true);
    }

    /**
     * @return Test keys.
     * @throws Exception If failed.
     */
    private Collection<Integer> keys() throws Exception {
        IgniteCache<Integer, Object> cache = jcache(0);

        List<Integer> keys = new ArrayList<>();

        keys.add(primaryKeys(cache, 1, lastKey).get(0));

        if (gridCount() > 1) {
            keys.add(backupKeys(cache, 1, lastKey).get(0));

            if (cache.getConfiguration(CacheConfiguration.class).getCacheMode() != REPLICATED)
                keys.add(nearKeys(cache, 1, lastKey).get(0));
        }

        lastKey = Collections.max(keys) + 1;

        return keys;
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void waitExpired(Integer key) throws Exception {
        waitExpired(Collections.singleton(key));
    }

    /**
     * @param keys Keys.
     * @throws Exception If failed.
     */
    private void waitExpired(final Collection<Integer> keys) throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int i = 0; i < gridCount(); i++) {
                    for (Integer key : keys) {
                        Object val = jcache(i).localPeek(key, CachePeekMode.ONHEAP);

                        if (val != null) {
                            // log.info("Value [grid=" + i + ", val=" + val + ']');

                            return false;
                        }
                    }
                }

                return false;
            }
        }, 3000);

        checkNoValue(keys);
    }

    /**
     * @param keys Keys.
     * @throws Exception If failed.
     */
    private void checkNoValue(Collection<Integer> keys) throws Exception {
        IgniteCache<Integer, Object> cache = jcache(0);

        for (int i = 0; i < gridCount(); i++) {
            ClusterNode node = grid(i).cluster().localNode();

            for (Integer key : keys) {
                Object val = jcache(i).localPeek(key, CachePeekMode.ONHEAP, CachePeekMode.OFFHEAP);

                if (val != null) {
                    log.info("Unexpected value [grid=" + i +
                        ", primary=" + affinity(cache).isPrimary(node, key) +
                        ", backup=" + affinity(cache).isBackup(node, key) + ']');
                }

                assertNull("Unexpected non-null value for grid " + i, val);
            }
        }

        storeMap.clear();

        for (int i = 0; i < gridCount(); i++) {
            for (Integer key : keys)
                assertNull("Unexpected non-null value for grid " + i, jcache(i).get(key));
        }
    }

    /**
     * @param key Key.
     * @param ttl TTL.
     * @throws Exception If failed.
     */
    private void checkTtl(Object key, long ttl) throws Exception {
        checkTtl(key, ttl, false);
    }

    /**
     * @param key Key.
     * @param ttl TTL.
     * @param wait If {@code true} waits for ttl update.
     * @throws Exception If failed.
     */
    private void checkTtl(Object key, final long ttl, boolean wait) throws Exception {
        boolean found = false;

        for (int i = 0; i < gridCount(); i++) {
            IgniteKernal grid = (IgniteKernal)grid(i);

            GridCacheAdapter<Object, Object> cache = grid.context().cache().internalCache();

            if (cache.context().isNear())
                cache = cache.context().near().dht();

            while (true) {
                try {
                    GridCacheEntryEx e = memoryMode() == CacheMemoryMode.ONHEAP_TIERED ?
                        cache.peekEx(key) : cache.entryEx(key);

                    if (e != null && e.deleted()) {
                        assertEquals(0, e.ttl());

                        assertFalse("Invalid entry [e=" + e + ", node=" + i + ']',
                            cache.affinity().isPrimaryOrBackup(grid.localNode(), key));

                        continue;
                    }

                    if (e == null)
                        assertTrue("Not found " + key, !cache.affinity().isPrimaryOrBackup(grid.localNode(), key));
                    else {
                        e.unswap();

                        found = true;

                        if (wait)
                            waitTtl(cache, key, ttl);

                        boolean primary = cache.affinity().isPrimary(grid.localNode(), key);
                        boolean backup = cache.affinity().isBackup(grid.localNode(), key);

                        assertEquals("Unexpected ttl [grid=" + i + ", nodeId=" + grid.getLocalNodeId() +
                            ", key=" + key + ", e=" + e + ", primary=" + primary + ", backup=" + backup + ']', ttl, e.ttl());

                        if (ttl > 0)
                            assertTrue(e.expireTime() > 0);
                        else
                            assertEquals(0, e.expireTime());
                    }

                    break;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    info("RETRY");
                    // Retry.
                }
                catch (GridDhtInvalidPartitionException ignore) {
                    // No need to check.
                    break;
                }
            }
        }

        assertTrue(found);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param ttl TTL to wait.
     * @throws IgniteInterruptedCheckedException If wait has been interrupted.
     */
    private void waitTtl(final GridCacheAdapter<Object, Object> cache, final Object key, final long ttl)
        throws IgniteInterruptedCheckedException {
        GridTestUtils.waitForCondition(new PAX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                GridCacheEntryEx entry;

                while (true) {
                    try {
                        entry = memoryMode() == CacheMemoryMode.ONHEAP_TIERED ?
                                cache.peekEx(key) : cache.entryEx(key);

                        assert entry != null;

                        entry.unswap();

                        return entry.ttl() == ttl;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        // Retry.
                    }
                    catch (GridDhtInvalidPartitionException ignore) {
                        return true;
                    }
                }
            }
        }, 3000);
    }

    /**
     *
     */
    private static class GetEntryProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e, Object... args) {
            return e.getValue();
        }
    }

    /**
     *
     */
    private static class TestPolicy implements ExpiryPolicy, Serializable {
        /** */
        private Long create;

        /** */
        private Long access;

        /** */
        private Long update;

        /**
         * @param create TTL for creation.
         * @param access TTL for access.
         * @param update TTL for update.
         */
        TestPolicy(@Nullable Long create,
            @Nullable Long update,
            @Nullable Long access) {
            this.create = create;
            this.update = update;
            this.access = access;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForCreation() {
            return create != null ? new Duration(TimeUnit.MILLISECONDS, create) : null;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForAccess() {
            return access != null ? new Duration(TimeUnit.MILLISECONDS, access) : null;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForUpdate() {
            return update != null ? new Duration(TimeUnit.MILLISECONDS, update) : null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestPolicy.class, this);
        }
    }
}
