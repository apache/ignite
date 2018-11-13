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

import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.datastructures.GridCacheQueueAdapter;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetImpl;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetProxy;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public abstract class IgniteCollectionAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /**
     * @param collocated Collocated flag.
     * @return Collection configuration.
     */
    protected final CollectionConfiguration config(boolean collocated) {
        CollectionConfiguration cfg = collectionConfiguration();

        cfg.setCollocated(collocated);

        return cfg;
    }

    /**
     * @return Collection configuration.
     */
    protected CollectionConfiguration collectionConfiguration() {
        CollectionConfiguration colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(collectionCacheMode());
        colCfg.setAtomicityMode(collectionCacheAtomicityMode());
        colCfg.setOffHeapMaxMemory(collectionOffHeapMaxMemory());

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

    /**
     * @return Collection cache off-heap max memory.
     */
    protected long collectionOffHeapMaxMemory() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /**
     * @param queue Ignite queue.
     * @return Cache configuration.
     */
    protected CacheConfiguration getQueueCache(IgniteQueue queue) {
        GridCacheQueueAdapter delegate = GridTestUtils.getFieldValue(queue, "delegate");

        GridCacheAdapter cache = GridTestUtils.getFieldValue(delegate, GridCacheQueueAdapter.class, "cache");

        return cache.configuration();
    }

    /**
     * @param queue Ignite queue.
     * @return Cache context.
     */
    protected static GridCacheContext cctx(IgniteQueue queue) {
        return GridTestUtils.getFieldValue(queue, "cctx");
    }

    /**
     * @param set Ignite set.
     * @return Cache context.
     */
    protected static GridCacheContext cctx(IgniteSet set) {
        if (set instanceof GridCacheSetProxy)
            return GridTestUtils.getFieldValue(set, GridCacheSetProxy.class, "cctx");
        else
            return GridTestUtils.getFieldValue(set, GridCacheSetImpl.class, "ctx");
    }

    /**
     * @param set Ignite set.
     * @return {@code True} If a separated cache is used to store items.
     */
    protected boolean separated(IgniteSet set) {
        if (set instanceof GridCacheSetProxy)
            set = ((GridCacheSetProxy)set).delegate();

        return GridTestUtils.getFieldValue(set, "separated");
    }
}