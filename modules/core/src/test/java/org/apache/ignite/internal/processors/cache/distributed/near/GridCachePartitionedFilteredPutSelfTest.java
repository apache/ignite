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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;

/**
 * Test filtered put.
 */
public class GridCachePartitionedFilteredPutSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(discoverySpi());
        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Discovery SPI;
     */
    private DiscoverySpi discoverySpi() {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setDistributionMode(NEAR_PARTITIONED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFilteredPutCheckNear() throws Exception {
        doFilteredPut();

        GridCache<Integer, Integer> c = cache();

        assert c.entrySet().isEmpty() : "Actual size: " + c.entrySet().size();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFilteredPutCheckDht() throws Exception {
        doFilteredPut();

        GridCache<Integer, Integer> c =
            ((GridNearCacheAdapter<Integer, Integer>)cache().<Integer, Integer>cache()).dht();

        assert c.entrySet().isEmpty() : "Actual size: " + c.entrySet().size();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAndRollbackCheckNear() throws Exception {
        doPutAndRollback();

        GridCache<Integer, Integer> c = cache();

        assert c.entrySet().isEmpty() : "Actual size: " + c.entrySet().size();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAndRollbackCheckDht() throws Exception {
        doPutAndRollback();

        GridCache<Integer, Integer> c =
            ((GridNearCacheAdapter<Integer, Integer>)cache().<Integer, Integer>cache()).dht();

        assert c.entrySet().isEmpty() : "Actual size: " + c.entrySet().size();
    }

    /**
     * @throws Exception If failed.
     */
    private void doFilteredPut() throws Exception {
        GridCache<Integer, Integer> c = cache();

        try (IgniteTx tx = c.txStart()) {
            assert !c.putx(1, 1, F.<Integer, Integer>cacheHasPeekValue());

            tx.commit();
        }

        assert c.isEmpty();
        assert c.peek(1) == null;
        assert c.get(1) == null;
    }

    /**
     * @throws Exception If failed.
     */
    private void doPutAndRollback() throws Exception {
        GridCache<Integer, Integer> c = cache();

        try (IgniteTx tx = c.txStart()) {
            assert c.putx(1, 1);

            tx.rollback();
        }

        assert c.isEmpty();
        assert c.peek(1) == null;
        assert c.get(1) == null;
    }
}
