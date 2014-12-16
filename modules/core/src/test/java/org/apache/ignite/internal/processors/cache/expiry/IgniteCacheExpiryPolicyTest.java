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

/**
 *
 */
public class IgniteCacheExpiryPolicyTest extends IgniteCacheTest {
    /** */
    private Factory<? extends ExpiryPolicy> factory;

    /** */
    private boolean nearCache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    public void testPrimary() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, 61_000L, null));

        nearCache = false;

        boolean inTx = false;

        startGrids();

        IgniteCache<Integer, Integer> cache = jcache(0);

        GridCache<Integer, Object> cache0 = cache(0);

        Integer key = primaryKey(cache0);

        log.info("Create: " + key);

        GridCacheTx tx = inTx ? grid(0).transactions().txStart() : null;

        cache.put(key, 1);

        if (tx != null)
            tx.commit();

        checkTtl(key, 60_000);

        tx = inTx ? grid(0).transactions().txStart() : null;

        log.info("Update: " + key);

        cache.put(key, 2);

        if (tx != null)
            tx.commit();

        checkTtl(key, 61_000);
    }

    public void testBackup() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, 61_000L, null));

        nearCache = false;

        boolean inTx = false;

        startGrids();

        IgniteCache<Integer, Integer> cache = jcache(0);

        GridCache<Integer, Object> cache0 = cache(0);

        Integer key = backupKey(cache0);

        log.info("Create: " + key);

        GridCacheTx tx = inTx ? grid(0).transactions().txStart() : null;

        cache.put(key, 1);

        if (tx != null)
            tx.commit();

        checkTtl(key, 60_000);

        tx = inTx ? grid(0).transactions().txStart() : null;

        log.info("Update: " + key);

        cache.put(key, 2);

        if (tx != null)
            tx.commit();

        checkTtl(key, 61_000);
    }

    public void testNear() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, 61_000L, null));

        nearCache = false;

        boolean inTx = true;

        startGrids();

        IgniteCache<Integer, Integer> cache = jcache(0);

        GridCache<Integer, Object> cache0 = cache(0);

        Integer key = nearKey(cache0);

        log.info("Create: " + key);

        GridCacheTx tx = inTx ? grid(0).transactions().txStart() : null;

        cache.put(key, 1);

        if (tx != null)
            tx.commit();

        checkTtl(key, 60_000);

        tx = inTx ? grid(0).transactions().txStart() : null;

        log.info("Update: " + key);

        cache.put(key, 2);

        if (tx != null)
            tx.commit();

        checkTtl(key, 61_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void test1() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, null, null));

        nearCache = false;

        boolean inTx = true;

        startGrids();

        Collection<Integer> keys = keys();

        IgniteCache<Integer, Integer> cache = jcache(0);

        for (final Integer key : keys) {
            log.info("Test key1: " + key);

            GridCacheTx tx = inTx ? grid(0).transactions().txStart() : null;

            cache.put(key, 1);

            if (tx != null)
                tx.commit();
        }

        for (final Integer key : keys) {
            log.info("Test key2: " + key);

            GridCacheTx tx = inTx ? grid(0).transactions().txStart() : null;

            cache.put(key, 2);

            if (tx != null)
                tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreated() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, null, null));

        startGrids();

        Collection<Integer> keys = keys();

        IgniteCache<Integer, Integer> cache = jcache(0);

        for (final Integer key : keys) {
            log.info("Test key: " + key);

            cache.put(key, 1);

            checkTtl(key, 60_000);

            for (int i = 0; i < gridCount(); i++) {
                assertEquals((Integer)1, cache.get(key));

                checkTtl(key, 60_000);
            }

            cache.withExpiryPolicy(new TestPolicy(1000L, null, null)).put(key, 2); // Update, should not change TTL.

            checkTtl(key, 60_000);

            assertEquals((Integer)2, cache.get(key));

            assertTrue(cache.remove(key));

            cache.withExpiryPolicy(new TestPolicy(1000L, null, null)).put(key, 3); // Create with provided TTL.

            checkTtl(key, 1000);

            waitExpired(key);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearPut() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, null, null));

        nearCache = true;

        startGrids();

        GridCache<Integer, Object> cache0 = cache(0);

        Integer key = nearKey(cache0);

        IgniteCache<Integer, Integer> jcache0 = jcache(0);

        jcache0.put(key, 1);

        checkTtl(key, 60_000);

        IgniteCache<Integer, Integer> jcache1 = jcache(1);

        // Update from another node with provided TTL.
        jcache1.withExpiryPolicy(new TestPolicy(null, 1000L, null)).put(key, 2);

        checkTtl(key, 1000);

        waitExpired(key);

        jcache1.remove(key);

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
        factory = new FactoryBuilder.SingletonFactory<>(new TestPolicy(60_000L, null, null));

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

        // Update from another node with provided TTL.
        jcache1.withExpiryPolicy(new TestPolicy(null, 1000L, null)).putAll(vals);

        for (Integer key : vals.keySet())
            checkTtl(key, 1000);

        waitExpired(vals.keySet());

        jcache0.removeAll(vals.keySet());

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

        Collection<Integer> keys = new ArrayList<>();

        keys.add(primaryKey(cache));

        if (gridCount() > 1) {
            keys.add(backupKey(cache));

            if (cache.configuration().getCacheMode() != REPLICATED)
                keys.add(nearKey(cache));
        }

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
        for (int i = 0; i < gridCount(); i++) {
            GridKernal grid = (GridKernal)grid(i);

            GridCacheAdapter<Object, Object> cache = grid.context().cache().internalCache();

            GridCacheEntryEx<Object, Object> e = cache.peekEx(key);

            if (e == null && cache.context().isNear())
                e = cache.context().near().dht().peekEx(key);

            if (e == null)
                assertTrue(!cache.affinity().isPrimaryOrBackup(grid.localNode(), key));
            else
                assertEquals("Unexpected ttl for grid " + i, ttl, e.ttl());
        }
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        assert factory != null;

        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);

        //cfg.setAtomicityMode(ATOMIC);

        cfg.setBackups(1);

        if (nearCache && gridName.equals(getTestGridName(0)))
            cfg.setDistributionMode(NEAR_PARTITIONED);
        else
            cfg.setDistributionMode(PARTITIONED_ONLY);

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
