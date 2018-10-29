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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 *
 */
@SuppressWarnings("unchecked")
public class CacheMvccConfigurationValidationTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
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
    public void testNodeRestartWithCacheModeChangedMvccToTx() throws Exception {
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
     * Test TRANSACTIONAL_SNAPSHOT and near cache.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testTransactionalSnapshotLimitations() throws Exception {
        assertCannotStart(
            mvccCacheConfig().setCacheMode(LOCAL),
            "LOCAL cache mode cannot be used with TRANSACTIONAL_SNAPSHOT atomicity mode"
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
                    assert e.getMessage().contains(msg) : "Wrong error message: " + e.getMessage();
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
