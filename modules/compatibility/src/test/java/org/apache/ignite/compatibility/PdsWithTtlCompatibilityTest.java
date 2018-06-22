/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.compatibility.persistence.IgnitePersistenceCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.migration.UpgradePendingTreeToPerPartitionTask;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test PendingTree upgrading to per-partition basis. Test fill cache with persistence enabled and with ExpirePolicy
 * configured on ignite-2.1 version and check if entries will be correctly expired when a new version node started.
 *
 * Note: Test for ignite-2.3 version will always fails due to entry ttl update fails with assertion on checkpoint lock
 * check.
 */
public class PdsWithTtlCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** */
    static final String TEST_CACHE_NAME = PdsWithTtlCompatibilityTest.class.getSimpleName();

    /** */
    static final int DURATION_SEC = 10;

    /** */
    private static final int ENTRIES_CNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(32L * 1024 * 1024)
                        .setPersistenceEnabled(true)
                        .setCheckpointPageBufferSize(16L * 1024 * 1024)
                ).setWalMode(WALMode.LOG_ONLY));

        return cfg;
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    public void testNodeStartByOldVersionPersistenceData_2_1() throws Exception {
        doTestStartupWithOldVersion("2.1.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param igniteVer 3-digits version of ignite
     * @throws Exception If failed.
     */
    protected void doTestStartupWithOldVersion(String igniteVer) throws Exception {
        try {
            startGrid(1, igniteVer, new ConfigurationClosure(), new PostStartupClosure());

            stopAllGrids();

            IgniteEx ignite = startGrid(0);

            assertEquals(1, ignite.context().discovery().topologyVersion());

            ignite.active(true);

            validateResultingCacheData(ignite, ignite.cache(TEST_CACHE_NAME));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cache to be filled by different keys and values. Results may be validated in {@link
     * #validateResultingCacheData(Ignite, IgniteCache)}.
     */
    public static void saveCacheData(Cache<Object, Object> cache) {
        for (int i = 0; i < ENTRIES_CNT; i++)
            cache.put(i, "data-" + i);

        //Touch
        for (int i = 0; i < ENTRIES_CNT; i++)
            assertNotNull(cache.get(i));
    }

    /**
     * Asserts cache contained all expected values as it was saved before.
     *
     * @param cache cache should be filled using {@link #saveCacheData(Cache)}.
     */
    public static void validateResultingCacheData(Ignite ignite,
        IgniteCache<Object, Object> cache) throws IgniteInterruptedCheckedException {

        final long expireTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(DURATION_SEC + 1);

        final IgniteFuture<Collection<Boolean>> future = ignite.compute().broadcastAsync(new UpgradePendingTreeToPerPartitionTask());

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return future.isDone() && expireTime < System.currentTimeMillis();
            }
        }, TimeUnit.SECONDS.toMillis(DURATION_SEC + 2));

        for (Boolean res : future.get())
            assertTrue(res);

        for (int i = 0; i < ENTRIES_CNT; i++)
            assertNull(cache.get(i));
    }

    /** */
    public static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            cfg.setPeerClassLoadingEnabled(false);

            cfg.setMemoryConfiguration(new MemoryConfiguration().setDefaultMemoryPolicySize(256L * 1024 * 1024));
            cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration().setWalMode(WALMode.LOG_ONLY)
                .setCheckpointingPageBufferSize(16L * 1024 * 1024));
        }
    }

    /** */
    public static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>();
            cacheCfg.setName(TEST_CACHE_NAME);
            cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg.setBackups(1);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, DURATION_SEC)));
            cacheCfg.setEagerTtl(true);
            cacheCfg.setGroupName("myGroup");

            IgniteCache<Object, Object> cache = ignite.createCache(cacheCfg);

            saveCacheData(cache);

            ignite.active(false);
        }
    }
}
