/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.expiry;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import javax.cache.expiry.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 *
 */
public abstract class IgniteCacheExpiryPolicyAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private Factory<? extends ExpiryPolicy> factory;

    /** */
    private boolean nearCache;

    /** */
    private Integer lastKey = 0;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testEternal() throws Exception {
        factory = EternalExpiryPolicy.factoryOf();

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
        factory = null;

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

        /*
        cache.withExpiryPolicy(new TestPolicy(60_000L, null, null)).put(key, 1); // Create with custom.

        checkTtl(key, 60_000L);

        cache.put(key, 2); // Update.

        checkTtl(key, 0);

        cache.withExpiryPolicy(new TestPolicy(null, 1000L, null)).put(key, 1);

        checkTtl(key, 1000L);

        waitExpired(key);
        */
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

        GridCacheTxConcurrency[] txModes = {PESSIMISTIC};

        if (atomicityMode() == TRANSACTIONAL) {
            for (GridCacheTxConcurrency tx : txModes) {
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
    private void createUpdatePutAll(@Nullable GridCacheTxConcurrency txConcurrency) throws Exception {
        Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            vals.put(i, i);

        IgniteCache<Integer, Integer> cache = jcache(0);

        cache.removeAll(vals.keySet());

        GridCacheTx tx = startTx(txConcurrency);

        // Create.
        cache.putAll(vals);

        if (tx != null)
            tx.commit();

        for (Integer key : vals.keySet())
            checkTtl(key, 60_000);

        tx = startTx(txConcurrency);

        // Update.
        cache.putAll(vals);

        if (tx != null)
            tx.commit();

        for (Integer key : vals.keySet())
            checkTtl(key, 61_000);

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
    private void createUpdateCustomPolicy(Integer key, @Nullable GridCacheTxConcurrency txConcurrency)
        throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        assertNull(cache.get(key));

        GridCacheTx tx = startTx(txConcurrency);

        cache.withExpiryPolicy(new TestPolicy(10_000L, 20_000L, 30_000L)).put(key, 1);

        if (tx != null)
            tx.commit();

        checkTtl(key, 10_000L);

        for (int idx = 0; idx < gridCount(); idx++) {
            assertEquals(1, cache(idx).get(key)); // Try get.

            checkTtl(key, 10_000);
        }

        tx = startTx(txConcurrency);

        // Update, returns null duration, should not change TTL.
        cache.withExpiryPolicy(new TestPolicy(20_000L, null, null)).put(key, 2);

        if (tx != null)
            tx.commit();

        checkTtl(key, 10_000L);

        tx = startTx(txConcurrency);

        // Update with provided TTL.
        cache.withExpiryPolicy(new TestPolicy(null, 1000L, null)).put(key, 2);

        if (tx != null)
            tx.commit();

        checkTtl(key, 1000L);

        waitExpired(key);

        tx = startTx(txConcurrency);

        // Create, returns null duration, should create with 0 TTL.
        cache.withExpiryPolicy(new TestPolicy(null, 20_000L, 30_000L)).put(key, 1);

        if (tx != null)
            tx.commit();

        checkTtl(key, 0L);
    }

    public void _testPrimary() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, 61_000L, null));

        nearCache = true;

        boolean inTx = true;

        startGrids();

        IgniteCache<Integer, Integer> cache = jcache(0);

        GridCache<Integer, Object> cache0 = cache(0);

        Integer key = primaryKey(cache0);

        log.info("Create: " + key);

        GridCacheTx tx = inTx ? grid(0).transactions().txStart(OPTIMISTIC, READ_COMMITTED) : null;

        cache.put(key, 1);

        if (tx != null)
            tx.commit();

        checkTtl(key, 60_000);

        tx = inTx ? grid(0).transactions().txStart(OPTIMISTIC, READ_COMMITTED) : null;

        log.info("Update: " + key);

        cache.put(key, 2);

        if (tx != null)
            tx.commit();

        checkTtl(key, 61_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void _test1() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, 61_000L, null));

        nearCache = false;

        boolean inTx = true;

        startGrids();

        Collection<Integer> keys = keys();

        IgniteCache<Integer, Integer> cache = jcache(0);

        for (final Integer key : keys) {
            log.info("Test key1: " + key);

            GridCacheTx tx = inTx ? grid(0).transactions().txStart(OPTIMISTIC, READ_COMMITTED) : null;

            cache.put(key, 1);

            if (tx != null)
                tx.commit();

            log.info("Test key2: " + key);

            tx = inTx ? grid(0).transactions().txStart(OPTIMISTIC, READ_COMMITTED) : null;

            cache.put(key, 2);

            if (tx != null)
                tx.commit();

            log.info("Done");
        }
    }

    /**
     * @param key Key.
     * @param txConcurrency Not null transaction concurrency mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void createUpdate(Integer key, @Nullable GridCacheTxConcurrency txConcurrency)
        throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        // Run several times to make sure create after remove works as expected.
        for (int i = 0; i < 3; i++) {
            log.info("Iteration: " + i);

            GridCacheTx tx = startTx(txConcurrency);

            cache.put(key, 1); // Create.

            if (tx != null)
                tx.commit();

            checkTtl(key, 60_000);

            for (int idx = 0; idx < gridCount(); idx++) {
                assertEquals(1, cache(idx).get(key)); // Try get.

                checkTtl(key, 60_000);
            }

            tx = startTx(txConcurrency);

            cache.put(key, 2); // Update.

            if (tx != null)
                tx.commit();

            checkTtl(key, 61_000);

            for (int idx = 0; idx < gridCount(); idx++) {
                assertEquals(2, cache(idx).get(key)); // Try get.

                checkTtl(key, 61_000);
            }

            tx = startTx(txConcurrency);

            assertTrue(cache.remove(key));

            if (tx != null)
                tx.commit();

            for (int idx = 0; idx < gridCount(); idx++)
                assertNull(cache(idx).get(key));
        }
    }

    /**
     * @param txConcurrency Transaction concurrency mode.
     * @return Transaction.
     */
    @Nullable private GridCacheTx startTx(@Nullable GridCacheTxConcurrency txConcurrency) {
        return txConcurrency == null ? null : ignite(0).transactions().txStart(txConcurrency, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearCreateUpdate() throws Exception {
        if (cacheMode() != PARTITIONED)
            return;

        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, 61_000L, null));

        nearCache = true;

        startGrids();

        Integer key = nearKey(cache(0));

        IgniteCache<Integer, Integer> jcache0 = jcache(0);

        jcache0.put(key, 1);

        checkTtl(key, 60_000);

        IgniteCache<Integer, Integer> jcache1 = jcache(1);

        // Update from another node.
        jcache1.put(key, 2);

        checkTtl(key, 61_000L);

        // Update from another node with provided TTL.
        jcache1.withExpiryPolicy(new TestPolicy(null, 1000L, null)).put(key, 3);

        checkTtl(key, 1000);

        waitExpired(key);

        // Try create again.
        jcache0.put(key, 1);

        checkTtl(key, 60_000);

        // Update from near node with provided TTL.
        jcache0.withExpiryPolicy(new TestPolicy(null, 1100L, null)).put(key, 2);

        checkTtl(key, 1100);

        waitExpired(key);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearPutAll() throws Exception {
        if (cacheMode() != PARTITIONED)
            return;

        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, 61_000L, null));

        nearCache = true;

        startGrids();

        Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            vals.put(i, i);

        IgniteCache<Integer, Integer> jcache0 = jcache(0);

        jcache0.putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 60_000);

        IgniteCache<Integer, Integer> jcache1 = jcache(1);

        // Update from another node.
        jcache1.putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 61_000);

        // Update from another node with provided TTL.
        jcache1.withExpiryPolicy(new TestPolicy(null, 1000L, null)).putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 1000);

        waitExpired(vals.keySet());

        // Try create again.
        jcache0.putAll(vals);

        // Update from near node with provided TTL.
        jcache1.withExpiryPolicy(new TestPolicy(null, 1101L, null)).putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 1101L);

        waitExpired(vals.keySet());
    }

    /**
     * @return Test keys.
     * @throws Exception If failed.
     */
    private Collection<Integer> keys() throws Exception {
        GridCache<Integer, Object> cache = cache(0);

        ArrayList<Integer> keys = new ArrayList<>();

        keys.add(primaryKeys(cache, 1, lastKey).get(0));

        if (gridCount() > 1) {
            keys.add(backupKeys(cache, 1, lastKey).get(0));

            if (cache.configuration().getCacheMode() != REPLICATED)
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
                        Object val = jcache(i).localPeek(key);

                        if (val != null) {
                            // log.info("Value [grid=" + i + ", val=" + val + ']');

                            return false;
                        }
                    }
                }

                return false;
            }
        }, 3000);

        GridCache<Integer, Object> cache = cache(0);

        for (int i = 0; i < gridCount(); i++) {
            ClusterNode node = grid(i).cluster().localNode();

            for (Integer key : keys) {
                Object val = jcache(i).localPeek(key);

                if (val != null) {
                    log.info("Unexpected value [grid=" + i +
                        ", primary=" + cache.affinity().isPrimary(node, key) +
                        ", backup=" + cache.affinity().isBackup(node, key) + ']');
                }

                assertNull("Unexpected non-null value for grid " + i, val);
            }
        }

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
        boolean found = false;

        for (int i = 0; i < gridCount(); i++) {
            GridKernal grid = (GridKernal)grid(i);

            GridCacheAdapter<Object, Object> cache = grid.context().cache().internalCache();

            GridCacheEntryEx<Object, Object> e = cache.peekEx(key);

            if (e == null && cache.context().isNear())
                e = cache.context().near().dht().peekEx(key);

            if (e == null)
                assertTrue(!cache.affinity().isPrimaryOrBackup(grid.localNode(), key));
            else {
                found = true;

                assertEquals("Unexpected ttl for grid " + i, ttl, e.ttl());

                if (ttl > 0)
                    assertTrue(e.expireTime() > 0);
                else
                    assertEquals(0, e.expireTime());
            }
        }

        assertTrue(found);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        if (nearCache && gridName.equals(getTestGridName(0)))
            cfg.setDistributionMode(NEAR_PARTITIONED);

        cfg.setExpiryPolicyFactory(factory);

        return cfg;
    }

    /**
     *
     */
    private class TestPolicy implements ExpiryPolicy {
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
