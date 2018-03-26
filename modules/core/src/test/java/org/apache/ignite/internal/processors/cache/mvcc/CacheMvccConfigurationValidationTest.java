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
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class CacheMvccConfigurationValidationTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMvccEnabled(true);

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
                node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL));

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

        node.createCache(new CacheConfiguration("cache1").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(ATOMIC));

                return null;
            }
        }, CacheException.class, null);

        node.createCache(new CacheConfiguration("cache2").setGroupName("grp1").setAtomicityMode(TRANSACTIONAL));
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

            cfg.setAtomicityMode(TRANSACTIONAL);

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
