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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
public abstract class IgniteCollectionAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String COL_CACHE_NAME = "TEST_COLLECTION_CACHE";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        // TODO IGNITE-180: remove cache configuration when dynamic cache start is implemented.
        TestCollectionConfiguration colCfg = collectionConfiguration();

        assertNotNull(colCfg);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(COL_CACHE_NAME);
        ccfg.setCacheMode(colCfg.getCacheMode());
        ccfg.setAtomicityMode(colCfg.getAtomicityMode());
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setBackups(colCfg.getBackups());
        ccfg.setMemoryMode(colCfg.getMemoryMode());
        ccfg.setDistributionMode(colCfg.getDistributionMode());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @param collocated Collocated flag.
     * @return Collection configuration.
     */
    protected final CollectionConfiguration config(boolean collocated) {
        CollectionConfiguration cfg = new CollectionConfiguration();

        cfg.setCacheName(COL_CACHE_NAME);
        cfg.setCollocated(collocated);

        return cfg;
    }

    /**
     * @return Collection configuration.
     */
    protected TestCollectionConfiguration collectionConfiguration() {
        TestCollectionConfiguration colCfg = new TestCollectionConfiguration();

        colCfg.setCacheMode(collectionCacheMode());
        colCfg.setAtomicityMode(collectionCacheAtomicityMode());
        colCfg.setDistributionMode(PARTITIONED_ONLY);

        if (colCfg.getCacheMode() == PARTITIONED)
            colCfg.setBackups(1);

        return colCfg;
    }

    /**
     * @return Number of nodes to start.
     */
    protected abstract int gridCount();

    /**
     * @return Collection cache mode.
     */
    protected abstract CacheMode collectionCacheMode();

    /**
     * @return Collection cache atomicity mode.
     */
    protected abstract CacheAtomicityMode collectionCacheAtomicityMode();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * TODO IGNITE-180: move properties to CollectionConfiguration.
     */
    public static class TestCollectionConfiguration {
        /** Default backups number. */
        public static final int DFLT_BACKUPS = 0;

        /** Default cache mode. */
        public static final CacheMode DFLT_CACHE_MODE = PARTITIONED;

        /** Default atomicity mode. */
        public static final CacheAtomicityMode DFLT_ATOMICITY_MODE = ATOMIC;

        /** Default memory mode. */
        public static final CacheMemoryMode DFLT_MEMORY_MODE = ONHEAP_TIERED;

        /** Default distribution mode. */
        public static final CacheDistributionMode DFLT_DISTRIBUTION_MODE = PARTITIONED_ONLY;

        /** Default off-heap storage size is {@code -1} which means that off-heap storage is disabled. */
        public static final long DFLT_OFFHEAP_MEMORY = -1;

        /** Off-heap memory size. */
        private long offHeapMaxMem = DFLT_OFFHEAP_MEMORY;

        /** Cache mode. */
        private CacheMode cacheMode = DFLT_CACHE_MODE;

        /** Cache distribution mode. */
        private CacheDistributionMode distro = DFLT_DISTRIBUTION_MODE;

        /** Number of backups. */
        private int backups = DFLT_BACKUPS;

        /** Atomicity mode. */
        private CacheAtomicityMode atomicityMode = DFLT_ATOMICITY_MODE;

        /** Memory mode. */
        private CacheMemoryMode memMode = DFLT_MEMORY_MODE;

        /**
         * @return Number of cache backups.
         */
        public int getBackups() {
            return backups;
        }

        /**
         * @param backups Number of cache backups.
         */
        public void setBackups(int backups) {
            this.backups = backups;
        }

        /**
         * @return Cache mode.
         */
        public CacheMode getCacheMode() {
            return cacheMode;
        }

        /**
         * @param cacheMode Cache mode.
         */
        public void setCacheMode(CacheMode cacheMode) {
            this.cacheMode = cacheMode;
        }

        /**
         * @return Cache atomicity mode.
         */
        public CacheAtomicityMode getAtomicityMode() {
            return atomicityMode;
        }

        /**
         * @param atomicityMode Cache atomicity mode.
         */
        public void setAtomicityMode(CacheAtomicityMode atomicityMode) {
            this.atomicityMode = atomicityMode;
        }

        /**
         * @return Cache memory mode.
         */
        public CacheMemoryMode getMemoryMode() {
            return memMode;
        }

        /**
         * @param memMode Cache memory mode.
         */
        public void setMemoryMode(CacheMemoryMode memMode) {
            this.memMode = memMode;
        }

        /**
         * @return Cache distribution mode.
         */
        public CacheDistributionMode getDistributionMode() {
            return distro;
        }

        /**
         * @param distro Cache distribution mode.
         */
        public void setDistributionMode(CacheDistributionMode distro) {
            this.distro = distro;
        }

        /**
         * @param offHeapMaxMem Maximum memory in bytes available to off-heap memory space.
         */
        public void setOffHeapMaxMemory(long offHeapMaxMem) {
            this.offHeapMaxMem = offHeapMaxMem;
        }

        /**
         * @return Maximum memory in bytes available to off-heap memory space.
         */
        public long getOffHeapMaxMemory() {
            return offHeapMaxMem;
        }
    }
}
