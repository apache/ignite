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
import org.gridgain.grid.util.typedef.*;
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

    /** */
    private static final long TTL_FOR_EXPIRE = 500L;

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

        assertEquals((Integer)1, cache.withExpiryPolicy(new TestPolicy(1100L, 1200L, TTL_FOR_EXPIRE)).get(key));

        checkTtl(key, TTL_FOR_EXPIRE, true);

        waitExpired(key);
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
            GridCacheTxConcurrency[] txModes;

            if (cacheMode() == LOCAL)
                txModes= new GridCacheTxConcurrency[]{PESSIMISTIC};
            else
                txModes= new GridCacheTxConcurrency[]{PESSIMISTIC, OPTIMISTIC};

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

            checkTtl(key, 60_000L);

            for (int idx = 0; idx < gridCount(); idx++) {
                assertEquals(1, cache(idx).get(key)); // Try get.

                checkTtl(key, 60_000L);
            }

            tx = startTx(txConcurrency);

            cache.put(key, 2); // Update.

            if (tx != null)
                tx.commit();

            checkTtl(key, 61_000L);

            for (int idx = 0; idx < gridCount(); idx++) {
                assertEquals(2, cache(idx).get(key)); // Try get.

                checkTtl(key, 61_000L);
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
        return txConcurrency == null ? null : ignite(0).transactions().txStart(txConcurrency, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearCreateUpdate() throws Exception {
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

        Integer key = nearKeys(cache(0), 1, 500_000).get(0);

        IgniteCache<Integer, Integer> cache0 = jcache(0);

        assertEquals(NEAR_PARTITIONED, cache(0).configuration().getDistributionMode());

        cache0.put(key, 1);

        checkTtl(key, 60_000L);

        IgniteCache<Integer, Integer> cache1 = jcache(1);

        // Update from another node.
        cache1.put(key, 2);

        checkTtl(key, 61_000L);

        // Update from another node with provided TTL.
        cache1.withExpiryPolicy(new TestPolicy(null, TTL_FOR_EXPIRE, null)).put(key, 3);

        checkTtl(key, TTL_FOR_EXPIRE);

        waitExpired(key);

        // Try create again.
        cache0.put(key, 1);

        checkTtl(key, 60_000L);

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

        IgniteCache<Integer, Integer> cache1 = jcache(1);

        // Update from another node.
        cache1.putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 61_000L);

        // Update from another node with provided TTL.
        cache1.withExpiryPolicy(new TestPolicy(null, 1000L, null)).putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 1000L);

        waitExpired(vals.keySet());

        // Try create again.
        cache0.putAll(vals);

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
        if (cacheMode() != PARTITIONED)
            return;

        nearCache = true;

        testAccess();

        Integer key = primaryKeys(cache(0), 1, 500_000).get(0);

        IgniteCache<Integer, Integer> cache0 = jcache(0);

        cache0.put(key, 1);

        checkTtl(key, 60_000L);

        assertEquals(1, jcache(1).get(key));

        checkTtl(key, 62_000L, true);

        assertEquals(1, jcache(2).withExpiryPolicy(new TestPolicy(1100L, 1200L, TTL_FOR_EXPIRE)).get(key));

        checkTtl(key, TTL_FOR_EXPIRE, true);

        waitExpired(key);

        // Test reader update on get.

        key = nearKeys(cache(0), 1, 600_000).get(0);

        cache0.put(key, 1);

        checkTtl(key, 60_000L);

        IgniteCache<Object, Object> cache =
            cache(0).affinity().isPrimary(grid(1).localNode(), key) ? jcache(1) : jcache(2);

        assertEquals(1, cache.get(key));

        checkTtl(key, 62_000L, true);
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
            GridKernal grid = (GridKernal)grid(i);

            GridCacheAdapter<Object, Object> cache = grid.context().cache().internalCache();

            GridCacheEntryEx<Object, Object> e = cache.peekEx(key);

            if (e == null && cache.context().isNear())
                e = cache.context().near().dht().peekEx(key);

            if (e == null)
                assertTrue(!cache.affinity().isPrimaryOrBackup(grid.localNode(), key));
            else {
                found = true;

                if (wait) {
                    final GridCacheEntryEx<Object, Object> e0 = e;

                    GridTestUtils.waitForCondition(new PA() {
                        @Override public boolean apply() {
                            try {
                                return e0.ttl() == ttl;
                            }
                            catch (Exception e) {
                                fail("Unexpected error: " + e);

                                return true;
                            }
                        }
                    }, 3000);
                }

                assertEquals("Unexpected ttl [grid=" + i + ", key=" + key +']', ttl, e.ttl());

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
