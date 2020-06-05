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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.configuration.DataPageEvictionMode.RANDOM_2_LRU;
import static org.apache.ignite.configuration.DataPageEvictionMode.RANDOM_LRU;

/**
 *
 */
@SuppressWarnings("unchecked")
public class CacheMvccConfigurationValidationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testMvccModeMismatchForGroup1() throws Exception {
        final Ignite node = startGrid(0);

        node.createCache(new CacheConfiguration("cache1").setGroupName("grp1").setAtomicityMode(ATOMIC));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() {
                node.createCache(
                    new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

                return null;
            }
        }, CacheException.class, null);

        node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(ATOMIC));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testMvccModeMismatchForGroup2() throws Exception {
        final Ignite node = startGrid(0);

        node.createCache(
            new CacheConfiguration("cache1").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() {
                node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(ATOMIC));

                return null;
            }
        }, CacheException.class, null);

        node.createCache(
            new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL_SNAPSHOT));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testMvccLocalCacheDisabled() throws Exception {
        final Ignite node1 = startGrid(1);
        final Ignite node2 = startGrid(2);

        IgniteCache cache1 = node1.createCache(new CacheConfiguration("cache1")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        cache1.put(1,1);
        cache1.put(2,2);
        cache1.put(2,2);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() {
                node1.createCache(new CacheConfiguration("cache2").setCacheMode(CacheMode.LOCAL)
                    .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

                return null;
            }
        }, CacheException.class, null);

        IgniteCache cache3 = node2.createCache(new CacheConfiguration("cache3")
            .setAtomicityMode(TRANSACTIONAL));

        cache3.put(1, 1);
        cache3.put(2, 2);
        cache3.put(3, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testNodeRestartWithCacheModeChangedTxToMvcc() throws Exception {
        cleanPersistenceDir();

        //Enable persistence.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        regionCfg.setPersistenceEnabled(true);
        storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        IgniteConfiguration cfg = getConfiguration("testGrid");
        cfg.setDataStorageConfiguration(storageCfg);
        cfg.setConsistentId(cfg.getIgniteInstanceName());

        Ignite node = startGrid(cfg);

        node.cluster().active(true);

        CacheConfiguration ccfg1 = new CacheConfiguration("test1").setAtomicityMode(TRANSACTIONAL);

        IgniteCache cache = node.createCache(ccfg1);

        cache.put(1, 1);
        cache.put(1, 2);
        cache.put(2, 2);

        stopGrid(cfg.getIgniteInstanceName());

        CacheConfiguration ccfg2 = new CacheConfiguration().setName(ccfg1.getName())
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

        IgniteConfiguration cfg2 = getConfiguration("testGrid")
            .setConsistentId(cfg.getIgniteInstanceName())
            .setCacheConfiguration(ccfg2)
            .setDataStorageConfiguration(storageCfg);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(cfg2);

                return null;
            }
        }, IgniteCheckedException.class, "Cannot start cache. Statically configured atomicity mode differs from");
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testNodeRestartWithCacheModeChangedMvccToTx() throws Exception {
        cleanPersistenceDir();

        //Enable persistence.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        regionCfg.setPersistenceEnabled(true);
        regionCfg.setPageEvictionMode(RANDOM_LRU);
        storageCfg.setDefaultDataRegionConfiguration(regionCfg);
        IgniteConfiguration cfg = getConfiguration("testGrid");
        cfg.setDataStorageConfiguration(storageCfg);
        cfg.setConsistentId(cfg.getIgniteInstanceName());

        Ignite node = startGrid(cfg);

        node.cluster().active(true);

        CacheConfiguration ccfg1 = new CacheConfiguration("test1").setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

        IgniteCache cache = node.createCache(ccfg1);

        cache.put(1, 1);
        cache.put(1, 2);
        cache.put(2, 2);

        stopGrid(cfg.getIgniteInstanceName());

        CacheConfiguration ccfg2 = new CacheConfiguration().setName(ccfg1.getName())
            .setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg2 = getConfiguration("testGrid")
            .setConsistentId(cfg.getIgniteInstanceName())
            .setCacheConfiguration(ccfg2)
            .setDataStorageConfiguration(storageCfg);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(cfg2);

                return null;
            }
        }, IgniteCheckedException.class, "Cannot start cache. Statically configured atomicity mode");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccInMemoryEvictionDisabled() throws Exception {
        final String memRegName = "in-memory-evictions";

        // Enable in-memory eviction.
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();
        regionCfg.setPersistenceEnabled(false);
        regionCfg.setPageEvictionMode(RANDOM_2_LRU);
        regionCfg.setName(memRegName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.setDefaultDataRegionConfiguration(regionCfg);

        IgniteConfiguration cfg = getConfiguration("testGrid");
        cfg.setDataStorageConfiguration(storageCfg);

        Ignite node = startGrid(cfg);

        CacheConfiguration ccfg1 = new CacheConfiguration("test1")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setDataRegionName(memRegName);

        try {
            node.createCache(ccfg1);

            fail("In memory evictions should be disabled for MVCC caches.");
        }
        catch (Exception e) {
            assertTrue(X.getFullStackTrace(e).contains("Data pages evictions cannot be used with TRANSACTIONAL_SNAPSHOT"));
        }
    }

    /**
     * Test TRANSACTIONAL_SNAPSHOT and near cache.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testTransactionalSnapshotLimitations() throws Exception {
        assertCannotStart(
            mvccCacheConfig().setCacheMode(LOCAL),
            "LOCAL cache mode cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode"
        );

        assertCannotStart(
            mvccCacheConfig().setRebalanceMode(CacheRebalanceMode.NONE),
            "Rebalance mode NONE cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode"
        );

        assertCannotStart(
            mvccCacheConfig().setNearConfiguration(new NearCacheConfiguration<>()),
            "near cache cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode"
        );

        assertCannotStart(
            mvccCacheConfig().setReadThrough(true),
            "readThrough cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode"
        );

        assertCannotStart(
            mvccCacheConfig().setWriteThrough(true),
            "writeThrough cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode"
        );

        assertCannotStart(
            mvccCacheConfig().setWriteBehindEnabled(true),
            "writeBehindEnabled cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode"
        );

        assertCannotStart(
            mvccCacheConfig().setExpiryPolicyFactory(new TestExpiryPolicyFactory()),
            "expiry policy cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode"
        );

        assertCannotStart(
            mvccCacheConfig().setInterceptor(new TestCacheInterceptor()),
            "interceptor cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode"
        );
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself and it contains passed message.
     * <p>
     * Note that this method follows includes {@link Throwable#getSuppressed()}
     * into check.
     *
     * @param t Throwable to check (if {@code null}, {@code false} is returned).
     * @param cls Cause class to check (if {@code null}, {@code false} is returned).
     * @param msg Message to check.
     * @return {@code True} if one of the causing exception is an instance of passed in classes
     *      and it contains the passed message, {@code false} otherwise.
     */
    private boolean hasCauseWithMessage(@Nullable Throwable t, Class<?> cls, String msg) {
        if (t == null)
            return false;

        assert cls != null;

        for (Throwable th = t; th != null; th = th.getCause()) {
            if (cls.isAssignableFrom(th.getClass()) && th.getMessage() != null && th.getMessage().contains(msg))
                return true;

            for (Throwable n : th.getSuppressed()) {
                if (hasCauseWithMessage(n, cls, msg))
                    return true;
            }

            if (th.getCause() == th)
                break;
        }

        return false;
    }

    /**
     * Make sure cache cannot be started with the given configuration.
     *
     * @param ccfg Cache configuration.
     * @param msg Message.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void assertCannotStart(CacheConfiguration ccfg, String msg) throws Exception {
        Ignite node = startGrid(0);

        try {
            try {
                node.getOrCreateCache(ccfg);

                fail("Cache should not start.");
            }
            catch (Exception e) {
                if (msg != null) {
                    assert e.getMessage() != null : "Error message is null";
                    assertTrue(hasCauseWithMessage(e, IgniteCheckedException.class, msg));
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @return MVCC-enabled cache configuration.
     */
    private static CacheConfiguration mvccCacheConfig() {
        return new CacheConfiguration().setName(DEFAULT_CACHE_NAME + UUID.randomUUID())
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);
    }

    /**
     * Test expiry policy.
     */
    private static class TestExpiryPolicyFactory implements Factory<ExpiryPolicy>, Serializable {
        /** {@inheritDoc} */
        @Override public ExpiryPolicy create() {
            return null;
        }
    }

    /**
     * Test cache interceptor.
     */
    private static class TestCacheInterceptor implements CacheInterceptor, Serializable {
        /** {@inheritDoc} */
        @Nullable
        @Override public Object onGet(Object key, @Nullable Object val) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object onBeforePut(Cache.Entry entry, Object newVal) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple onBeforeRemove(Cache.Entry entry) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry entry) {
            // No-op.
        }
    }
}
