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

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Affinity routing tests.
 */
public class GridCacheAffinityRoutingSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final String NON_DFLT_CACHE_NAME = "myCache";

    /** */
    private static final int KEY_CNT = 50;

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * Constructs test.
     */
    public GridCacheAffinityRoutingSelfTest() {
        super(/* don't start grid */ false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        if (!gridName.equals(getTestGridName(GRID_CNT))) {
            // Default cache configuration.
            CacheConfiguration dfltCacheCfg = defaultCacheConfiguration();

            dfltCacheCfg.setCacheMode(PARTITIONED);
            dfltCacheCfg.setBackups(1);
            dfltCacheCfg.setWriteSynchronizationMode(FULL_SYNC);

            // Non-default cache configuration.
            CacheConfiguration namedCacheCfg = defaultCacheConfiguration();

            namedCacheCfg.setCacheMode(PARTITIONED);
            namedCacheCfg.setBackups(1);
            namedCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
            namedCacheCfg.setName(NON_DFLT_CACHE_NAME);

            cfg.setCacheConfiguration(dfltCacheCfg, namedCacheCfg);
        }
        else {
            // No cache should be configured for extra node.
            cfg.setCacheConfiguration();
        }

        cfg.setMarshaller(new OptimizedMarshaller(false));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        assert G.allGrids().size() == GRID_CNT;

        for (int i = 0; i < KEY_CNT; i++) {
            grid(0).cache(null).put(i, i);

            grid(0).cache(NON_DFLT_CACHE_NAME).put(i, i);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        for (int i = 0; i < GRID_CNT; i++)
            stopGrid(i);

        assert G.allGrids().isEmpty();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAffinityRun() throws Exception {
        for (int i = 0; i < KEY_CNT; i++)
            grid(0).compute().affinityRun(NON_DFLT_CACHE_NAME, i, new CheckRunnable(i, i));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAffinityRunComplexKey() throws Exception {
        for (int i = 0; i < KEY_CNT; i++) {
            AffinityTestKey key = new AffinityTestKey(i);

            grid(0).compute().affinityRun(NON_DFLT_CACHE_NAME, i, new CheckRunnable(i, key));
            grid(0).compute().affinityRun(NON_DFLT_CACHE_NAME, key, new CheckRunnable(i, key));
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAffinityCall() throws Exception {
        for (int i = 0; i < KEY_CNT; i++)
            grid(0).compute().affinityCall(NON_DFLT_CACHE_NAME, i, new CheckCallable(i, i));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAffinityCallComplexKey() throws Exception {
        for (int i = 0; i < KEY_CNT; i++) {
            final AffinityTestKey key = new AffinityTestKey(i);

            grid(0).compute().affinityCall(NON_DFLT_CACHE_NAME, i, new CheckCallable(i, key));
            grid(0).compute().affinityCall(NON_DFLT_CACHE_NAME, key, new CheckCallable(i, key));
        }
    }

    /**
     * Test key.
     */
    private static class AffinityTestKey {
        /** Affinity key. */
        @AffinityKeyMapped
        private final int affKey;

        /**
         * @param affKey Affinity key.
         */
        private AffinityTestKey(int affKey) {
            this.affKey = affKey;
        }

        /**
         * @return Affinity key.
         */
        public int affinityKey() {
            return affKey;
        }
    }

    /**
     * Test runnable.
     */
    private static class CheckRunnable extends CAX {
        /** Affinity key. */
        private final Object affKey;

        /** Key. */
        private final Object key;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param affKey Affinity key.
         * @param key Key.
         */
        private CheckRunnable(Object affKey, Object key) {
            this.affKey = affKey;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public void applyx() throws IgniteCheckedException {
            assert ignite.cluster().localNode().id().equals(ignite.cluster().mapKeyToNode(null, affKey).id());
            assert ignite.cluster().localNode().id().equals(ignite.cluster().mapKeyToNode(null, key).id());
        }
    }

    /**
     * Test callable.
     */
    private static class CheckCallable implements IgniteCallable<Object> {
        /** Affinity key. */
        private final Object affKey;

        /** Key. */
        private final Object key;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /**
         * @param affKey Affinity key.
         * @param key Key.
         */
        private CheckCallable(Object affKey, Object key) {
            this.affKey = affKey;
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws IgniteCheckedException {
            assert ignite.cluster().localNode().id().equals(ignite.cluster().mapKeyToNode(null, affKey).id());
            assert ignite.cluster().localNode().id().equals(ignite.cluster().mapKeyToNode(null, key).id());

            return null;
        }
    }
}
