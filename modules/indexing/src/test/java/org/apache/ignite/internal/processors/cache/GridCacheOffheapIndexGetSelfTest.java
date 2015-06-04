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
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.configuration.DeploymentMode.*;

/**
 * Tests off heap storage when both offheaped and swapped entries exists.
 */
public class GridCacheOffheapIndexGetSelfTest extends GridCommonAbstractTest {
    /** */
    private static final long OFFHEAP_MEM = 10L * 1024L;

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setNetworkTimeout(2000);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setOffHeapMaxMemory(OFFHEAP_MEM);
        cacheCfg.setEvictSynchronized(true);
        cacheCfg.setEvictSynchronizedKeyBufferSize(1);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setMemoryMode(OFFHEAP_TIERED);
        cacheCfg.setEvictionPolicy(null);
        cacheCfg.setIndexedTypes(Long.class, Long.class);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setDeploymentMode(SHARED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cache(null).clear();
    }

    /**
     * Tests behavior on offheaped entries.
     *
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        IgniteCache<Long, Long> cache = grid(0).cache(null);

        for (long i = 0; i < 100; i++)
            cache.put(i, i);

        for (long i = 0; i < 100; i++)
            assertEquals((Long)i, cache.get(i));

        SqlQuery<Long, Long> qry = new SqlQuery<>(Long.class, "_val >= 90");

        List<Cache.Entry<Long, Long>> res = cache.query(qry).getAll();

        assertEquals(10, res.size());

        for (Cache.Entry<Long, Long> e : res) {
            assertNotNull(e.getKey());
            assertNotNull(e.getValue());
        }
    }
}
