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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreReadFromBackupTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.configvariations.ConfigVariations;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

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
    public void testMvccModeMismatchForGroup1() throws Exception {
        final Ignite node = startGrid(0);

        node.createCache(new CacheConfiguration("cache1").setGroupName("grp1").setAtomicityMode(ATOMIC));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

                return null;
            }
        }, CacheException.class, null);

        node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(ATOMIC));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccModeMismatchForGroup2() throws Exception {
        final Ignite node = startGrid(0);

        node.createCache(new CacheConfiguration("cache1").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(ATOMIC));

                return null;
            }
        }, CacheException.class, null);

        node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL_SNAPSHOT));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccLocalCacheDisabled() throws Exception {
        final Ignite node1 = startGrid(1);
        final Ignite node2 = startGrid(2);

        IgniteCache cache1 = node1.createCache(new CacheConfiguration("cache1")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        cache1.put(1,1);
        cache1.put(2,2);
        cache1.put(2,2);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
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
    public void testMvccExpiredPolicyCacheDisabled() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8640");

        final Ignite node1 = startGrid(1);
        final Ignite node2 = startGrid(2);

        IgniteCache cache1 = node1.createCache(new CacheConfiguration("cache1")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        cache1.put(1,1);
        cache1.put(2,2);
        cache1.put(2,2);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node1.createCache(new CacheConfiguration("cache2")
                    .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 1)))
                    .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

                return null;
            }
        }, CacheException.class, null);

        IgniteCache cache3 = node2.createCache(new CacheConfiguration("cache3")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        cache3.put(1, 1);
        cache3.put(2, 2);
        cache3.put(3, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccThirdPartyStoreCacheDisabled() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8640");

        final Ignite node1 = startGrid(1);
        final Ignite node2 = startGrid(2);

        IgniteCache cache1 = node1.createCache(new CacheConfiguration("cache1")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        cache1.put(1,1);
        cache1.put(2,2);
        cache1.put(2,2);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node1.createCache(new CacheConfiguration("cache2")
                    .setCacheStoreFactory(FactoryBuilder.factoryOf(CacheStoreReadFromBackupTest.TestStore.class))
                    .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

                return null;
            }
        }, CacheException.class, null);

        IgniteCache cache3 = node2.createCache(new CacheConfiguration("cache3")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        cache3.put(1, 1);
        cache3.put(2, 2);
        cache3.put(3, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccInterceptorCacheDisabled() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8640");

        final Ignite node1 = startGrid(1);
        final Ignite node2 = startGrid(2);

        IgniteCache cache1 = node1.createCache(new CacheConfiguration("cache1")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        cache1.put(1,1);
        cache1.put(2,2);
        cache1.put(2,2);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node1.createCache(new CacheConfiguration("cache2")
                    .setInterceptor(new ConfigVariations.NoopInterceptor())
                    .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

                return null;
            }
        }, CacheException.class, null);

        IgniteCache cache3 = node2.createCache(new CacheConfiguration("cache3")
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT));

        cache3.put(1, 1);
        cache3.put(2, 2);
        cache3.put(3, 3);
    }

    /**
     * @throws Exception If failed.
     */
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
     * @throws Exception If failed.
     */
    public void testTxCacheWithCacheStore() throws Exception {
        checkTransactionalModeConflict("cacheStoreFactory", new TestFactory(),
            "Transactional cache may not have a third party cache store when MVCC is enabled.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCacheWithExpiryPolicy() throws Exception {
        checkTransactionalModeConflict("expiryPolicyFactory0", CreatedExpiryPolicy.factoryOf(Duration.FIVE_MINUTES),
            "Transactional cache may not have expiry policy when MVCC is enabled.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxCacheWithInterceptor() throws Exception {
        checkTransactionalModeConflict("interceptor", new CacheInterceptorAdapter(),
            "Transactional cache may not have an interceptor when MVCC is enabled.");
    }

    /**
     * Check that setting specified property conflicts with transactional cache atomicity mode.
     * @param propName Property name.
     * @param obj Property value.
     * @param errMsg Expected error message.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void checkTransactionalModeConflict(String propName, Object obj, String errMsg)
        throws Exception {
        final String setterName = "set" + propName.substring(0, 1).toUpperCase() + propName.substring(1);

        try (final Ignite node = startGrid(0)) {
            final CacheConfiguration cfg = new TestConfiguration("cache");

            cfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

            U.invoke(TestConfiguration.class, cfg, setterName, obj);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @SuppressWarnings("unchecked")
                @Override public Void call() {
                    node.getOrCreateCache(cfg);

                    return null;
                }
            }, IgniteCheckedException.class, errMsg);
        }
    }

    /**
     * Dummy class to overcome ambiguous method name "setExpiryPolicyFactory".
     */
    private final static class TestConfiguration extends CacheConfiguration {
        /**
         *
         */
        TestConfiguration(String cacheName) {
            super(cacheName);
        }

        /**
         *
         */
        @SuppressWarnings("unused")
        public void setExpiryPolicyFactory0(Factory<ExpiryPolicy> plcFactory) {
            super.setExpiryPolicyFactory(plcFactory);
        }
    }

    /**
     *
     */
    private static class TestFactory implements Factory<CacheStore> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return null;
        }
    }
}
