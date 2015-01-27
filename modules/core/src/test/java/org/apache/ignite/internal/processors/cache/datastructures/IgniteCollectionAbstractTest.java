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
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CachePreloadMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
public abstract class IgniteCollectionAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        // TODO IGNITE-29: remove cache configuration when dynamic cache start is implemented.
        IgniteCollectionConfiguration colCfg = collectionConfiguration();

        assertNotNull(colCfg);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName("TEST_COLLECTION_CACHE");
        ccfg.setCacheMode(colCfg.getCacheMode());
        ccfg.setAtomicityMode(colCfg.getAtomicityMode());
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setBackups(colCfg.getBackups());
        ccfg.setMemoryMode(colCfg.getMemoryMode());
        ccfg.setDistributionMode(colCfg.getDistributionMode());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setPreloadMode(SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Collection configuration.
     */
    IgniteCollectionConfiguration collectionConfiguration() {
        IgniteCollectionConfiguration colCfg = new IgniteCollectionConfiguration();

        colCfg.setCacheMode(collectionCacheMode());
        colCfg.setAtomicityMode(collectionCacheAtomicityMode());
        colCfg.setDistributionMode(PARTITIONED_ONLY);

        return colCfg;
    }

    /**
     * @return Collection configuration with {@link IgniteCollectionConfiguration#isCollocated()} flag set.
     */
    protected IgniteCollectionConfiguration collocatedCollectionConfiguration() {
        IgniteCollectionConfiguration colCfg = collectionConfiguration();

        colCfg.setCollocated(true);

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
}
