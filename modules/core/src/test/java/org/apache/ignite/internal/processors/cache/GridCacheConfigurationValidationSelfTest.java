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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.UUID;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Attribute validation self test.
 */
public class GridCacheConfigurationValidationSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String NON_DFLT_CACHE_NAME = "non-dflt-cache";

    /** */
    private static final String WRONG_PRELOAD_MODE_IGNITE_INSTANCE_NAME = "preloadModeCheckFails";

    /** */
    private static final String WRONG_CACHE_MODE_IGNITE_INSTANCE_NAME = "cacheModeCheckFails";

    /** */
    private static final String WRONG_AFFINITY_IGNITE_INSTANCE_NAME = "cacheAffinityCheckFails";

    /** */
    private static final String WRONG_AFFINITY_MAPPER_IGNITE_INSTANCE_NAME = "cacheAffinityMapperCheckFails";

    /** */
    private static final String DUP_CACHES_IGNITE_INSTANCE_NAME = "duplicateCachesCheckFails";

    /** */
    private static final String DUP_DFLT_CACHES_IGNITE_INSTANCE_NAME = "duplicateDefaultCachesCheckFails";

    /** */
    private static final String RESERVED_FOR_DATASTRUCTURES_CACHE_NAME_IGNITE_INSTANCE_NAME =
        "reservedForDsCacheNameCheckFails";

    /** */
    private static final String RESERVED_FOR_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME =
            "reservedForDsCacheGroupNameCheckFails";

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * Constructs test.
     */
    public GridCacheConfigurationValidationSelfTest() {
        super(/* don't start grid */ false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        // Default cache config.
        CacheConfiguration dfltCacheCfg = defaultCacheConfiguration();

        dfltCacheCfg.setCacheMode(PARTITIONED);
        dfltCacheCfg.setRebalanceMode(ASYNC);
        dfltCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dfltCacheCfg.setAffinity(new RendezvousAffinityFunction());
        dfltCacheCfg.setIndexedTypes(Integer.class, String.class);

        // Non-default cache configuration.
        CacheConfiguration namedCacheCfg = defaultCacheConfiguration();

        namedCacheCfg.setCacheMode(PARTITIONED);
        namedCacheCfg.setRebalanceMode(ASYNC);
        namedCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        namedCacheCfg.setAffinity(new RendezvousAffinityFunction());

        // Modify cache config according to test parameters.
        if (igniteInstanceName.contains(WRONG_PRELOAD_MODE_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setRebalanceMode(SYNC);
        else if (igniteInstanceName.contains(WRONG_CACHE_MODE_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setCacheMode(REPLICATED);
        else if (igniteInstanceName.contains(WRONG_AFFINITY_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setAffinity(new TestRendezvousAffinityFunction());
        else if (igniteInstanceName.contains(WRONG_AFFINITY_MAPPER_IGNITE_INSTANCE_NAME))
            dfltCacheCfg.setAffinityMapper(new TestCacheDefaultAffinityKeyMapper());

        if (igniteInstanceName.contains(DUP_CACHES_IGNITE_INSTANCE_NAME))
            cfg.setCacheConfiguration(namedCacheCfg, namedCacheCfg);
        else if (igniteInstanceName.contains(DUP_DFLT_CACHES_IGNITE_INSTANCE_NAME))
            cfg.setCacheConfiguration(dfltCacheCfg, dfltCacheCfg);
        else
            // Normal configuration.
            cfg.setCacheConfiguration(dfltCacheCfg, namedCacheCfg);

        if (igniteInstanceName.contains(RESERVED_FOR_DATASTRUCTURES_CACHE_NAME_IGNITE_INSTANCE_NAME))
            namedCacheCfg.setName(DataStructuresProcessor.ATOMICS_CACHE_NAME + "@abc");
        else
            namedCacheCfg.setName(NON_DFLT_CACHE_NAME);

        if (igniteInstanceName.contains(RESERVED_FOR_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME))
            namedCacheCfg.setGroupName("default-ds-group");

        return cfg;
    }

    /**
     * This test method does not require remote nodes.
     */
    public void testDuplicateCacheConfigurations() {
        // This grid should not start.
        startInvalidGrid(DUP_CACHES_IGNITE_INSTANCE_NAME);

        // This grid should not start.
        startInvalidGrid(DUP_DFLT_CACHES_IGNITE_INSTANCE_NAME);
    }

    /**
     * @throws Exception If fails.
     */
    public void testCacheAttributesValidation() throws Exception {
        try {
            startGrid(0);

            // This grid should not start.
            startInvalidGrid(WRONG_PRELOAD_MODE_IGNITE_INSTANCE_NAME);

            // This grid should not start.
            startInvalidGrid(WRONG_CACHE_MODE_IGNITE_INSTANCE_NAME);

            // This grid should not start.
            startInvalidGrid(WRONG_AFFINITY_IGNITE_INSTANCE_NAME);

            // This grid should not start.
            startInvalidGrid(WRONG_AFFINITY_MAPPER_IGNITE_INSTANCE_NAME);

            // This grid should not start.
            startInvalidGrid(RESERVED_FOR_DATASTRUCTURES_CACHE_NAME_IGNITE_INSTANCE_NAME);

            // This grid should not start.
            startInvalidGrid(RESERVED_FOR_DATASTRUCTURES_CACHE_GROUP_NAME_IGNITE_INSTANCE_NAME);

            // This grid will start normally.
            startGrid(1);
        }
        finally {
            stopAllGrids();
        }
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
        @Nullable @Override public Object onGet(Object key, @Nullable Object val) {
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
     * Starts grid that will fail to start due to invalid configuration.
     *
     * @param name Name of the grid which will have invalid configuration.
     */
    private void startInvalidGrid(String name) {
        try {
            startGrid(name);

            assert false : "Exception should have been thrown.";
        }
        catch (Exception e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     *
     */
    private static class TestRendezvousAffinityFunction extends RendezvousAffinityFunction {
        // No-op. Just to have another class name.

        /**
         * Empty constructor required by Externalizable.
         */
        public TestRendezvousAffinityFunction() {
            // No-op.
        }
    }

    /**
     *
     */
    private static class TestCacheDefaultAffinityKeyMapper extends GridCacheDefaultAffinityKeyMapper {
        // No-op. Just to have another class name.
    }
}