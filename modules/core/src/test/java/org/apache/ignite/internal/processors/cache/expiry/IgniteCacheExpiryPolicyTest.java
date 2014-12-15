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
        return 2;
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
     * @return Test keys.
     * @throws Exception If failed.
     */
    private Collection<Integer> keys() throws Exception {
        GridCache<Integer, Object> cache = cache(0);

        Collection<Integer> keys = new ArrayList<>();

        keys.add(primaryKey(cache));

        if (gridCount() > 1) {
            keys.add(backupKey(cache));

            if (cache.configuration().getDistributionMode() == NEAR_PARTITIONED)
                keys.add(nearKey(cache));
        }

        return keys;
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void waitExpired(final Integer key) throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int i = 0; i < gridCount(); i++) {
                    Object val = jcache(i).localPeek(key);

                    log.info("Value [grid=" + i + ", val=" + val + ']');

                    if (val != null)
                        return false;
                }

                return false;
            }
        }, 3000);

        GridCache<Integer, Object> cache = cache(0);

        for (int i = 0; i < gridCount(); i++) {
            ClusterNode node = grid(i).cluster().localNode();

            Object val = jcache(i).localPeek(key);

            log.info("Value [grid=" + i +
                ", primary=" + cache.affinity().isPrimary(node, key) +
                ", backup=" + cache.affinity().isBackup(node, key) + ']');

            assertNull("Unexpected non-null value for grid " + i, val);
        }

        for (int i = 0; i < gridCount(); i++)
            assertNull("Unexpected non-null value for grid " + i, jcache(i).get(key));
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

            if (e == null) {
                assertTrue(i > 0);

                assertTrue(!cache.affinity().isPrimaryOrBackup(grid.localNode(), key));
            }
            else
                assertEquals("Unexpected ttl for grid " + i, ttl, e.ttl());
        }
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        assert factory != null;

        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(ATOMIC);
        cfg.setBackups(1);

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
           @Nullable Long access,
           @Nullable Long update) {
            this.create = create;
            this.access = access;
            this.update = update;
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
