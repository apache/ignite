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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class PageEvictionMultinodeTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Offheap size for memory policy. */
    private static final int SIZE = 256 * 1024 * 1024;

    /** Page size. */
    private static final int PAGE_SIZE = 2048;

    /** Number of entries. */
    private static final int ENTRIES = 400_000;

    /** Empty pages pool size. */
    private static final int EMPTY_PAGES_POOL_SIZE = 100;

    /** Eviction threshold. */
    private static final double EVICTION_THRESHOLD = 0.9;

    /** Cache modes. */
    private static final CacheMode[] CACHE_MODES = {CacheMode.PARTITIONED, CacheMode.REPLICATED};

    /** Atomicity modes. */
    private static final CacheAtomicityMode[] ATOMICITY_MODES = {
        CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

    /** Write modes. */
    private static final CacheWriteSynchronizationMode[] WRITE_MODES = {CacheWriteSynchronizationMode.PRIMARY_SYNC,
        CacheWriteSynchronizationMode.FULL_SYNC, CacheWriteSynchronizationMode.FULL_ASYNC};
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration plc = new MemoryPolicyConfiguration();

        plc.setDefault(true);
        plc.setSize(SIZE);
        plc.setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);
        plc.setEmptyPagesPoolSize(EMPTY_PAGES_POOL_SIZE);
        plc.setEvictionThreshold(EVICTION_THRESHOLD);

        dbCfg.setMemoryPolicies(plc);
        dbCfg.setPageSize(PAGE_SIZE);

        cfg.setMemoryConfiguration(dbCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(4, false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }

    /**
     *
     */
    public void testPageEviction() throws Exception {
        for (int i = 0; i < CACHE_MODES.length; i++) {
            for (int j = 0; j < ATOMICITY_MODES.length; j++) {
                for (int k = 0; k < WRITE_MODES.length; k++) {
                    if (i + j + Math.min(k, 1) <= 1) {
                        CacheConfiguration<Object, Object> cfg = cacheConfig(
                            "cache" + i + j + k, null, CACHE_MODES[i], ATOMICITY_MODES[j], WRITE_MODES[k]);

                        createCacheAndTestEvcition(cfg);
                    }
                }
            }
        }
    }

    /**
     * @param cfg Config.
     */
    private void createCacheAndTestEvcition(CacheConfiguration<Object, Object> cfg) throws Exception {
        IgniteCache<Object, Object> cache = ignite(0).getOrCreateCache(cfg);

        for (int i = 0; i < ENTRIES; i++) {

            ThreadLocalRandom r = ThreadLocalRandom.current();

            if (r.nextInt() % 5 == 0)
                cache.put(i, new TestObject(PAGE_SIZE / 4 - 50 + r.nextInt(5000))); // Fragmented object.
            else
                cache.put(i, new TestObject(r.nextInt(PAGE_SIZE / 4 - 50))); // Fits in one page.

            if (i % (ENTRIES / 10) == 0)
                System.out.println(">>> Entries put: " + i);
        }

        int resultingSize = cache.size(CachePeekMode.PRIMARY);

        System.out.println(">>> Resulting size: " + resultingSize);

        // More than half of entries evicted, no OutOfMemory occurred, success.
        assertTrue(resultingSize < ENTRIES / 2);

        ignite(0).destroyCache(cfg.getName());
    }



    /**
     * @param name Name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param writeSynchronizationMode Write synchronization mode.
     * @param memoryPlcName Memory policy name.
     */
    private static CacheConfiguration<Object, Object> cacheConfig(
        String name,
        String memoryPlcName,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        CacheWriteSynchronizationMode writeSynchronizationMode
    ) {
        CacheConfiguration<Object, Object> cacheConfiguration = new CacheConfiguration<>()
            .setName(name)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setCacheMode(cacheMode)
            .setAtomicityMode(atomicityMode)
            .setMemoryPolicyName(memoryPlcName)
            .setWriteSynchronizationMode(writeSynchronizationMode);

        if (cacheMode == CacheMode.PARTITIONED)
            cacheConfiguration.setBackups(1);

        return cacheConfiguration;
    }







}
