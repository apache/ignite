/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.expiry;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;

import javax.cache.configuration.*;
import javax.cache.expiry.*;
import java.util.concurrent.*;

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

    @Override
    protected int gridCount() {
        return 2;
    }

    /**
     *
     */
    private class TestCreatedPolicy implements ExpiryPolicy {
        /** */
        private final Duration duration;

        /**
         * @param ttl TTL for creation.
         */
        TestCreatedPolicy(long ttl) {
            this.duration = new Duration(TimeUnit.MILLISECONDS, ttl);
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForCreation() {
            return duration;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForAccess() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Duration getExpiryForUpdate() {
            return null;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreated() throws Exception {
        factory = new FactoryBuilder.SingletonFactory<>(new TestCreatedPolicy(1000));

        startGrids();

        final Integer key = 1;

        IgniteCache<Integer, Integer> cache = jcache(0);

        cache.put(1, 1);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int i = 0; i < gridCount(); i++) {
                    Object val = cache(i).peek(key);

                    log.info("Value [grid=" + i + ", val=" + val + ']');

                    if (val != null)
                        return false;
                }

                return false;
            }
        }, 2000);

        for (int i = 0; i < gridCount(); i++)
            assertNull("Unexpected non-null value for grid " + i, cache.get(key));
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        assert factory != null;

        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheMode(GridCacheMode.PARTITIONED);
        cfg.setAtomicityMode(GridCacheAtomicityMode.ATOMIC);

        cfg.setExpiryPolicyFactory(factory);

        return cfg;
    }
}
