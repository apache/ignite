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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests that {@link CacheRebalanceMode#SYNC} caches are evicted at first.
 *
 * Note that this test is based on logging, for more correct version check the same class in actual master branch.
 * Correct version could be merged here only when https://ggsystems.atlassian.net/browse/GG-17431 has been backported.
 */
public class PartitionEvictionOrderTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private ListeningTestLogger testLog = new ListeningTestLogger(log);

    /**
     * Flag for condition that async partition eviction is not happened during sync partitions eviction
     */
    private Boolean asyncCacheEvicted = false;

    /** */
    boolean sysCacheEvictStarted;

    /** */
    boolean sysCacheEvictQueued;

    /** */
    boolean sysCacheEvictEnded;

    /** */
    boolean logParsed = true;

    /**
     * Flag for condition that first sync partition has been polled from eviction queue at first.
     */
    boolean firstSysPartEvictedAtFirst;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        CacheConfiguration<Long, Long> atomicCcfg = new CacheConfiguration<Long, Long>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setCacheMode(REPLICATED);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setGridLogger(testLog);

        cfg.setCacheConfiguration(atomicCcfg);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests that {@link CacheRebalanceMode#SYNC} caches are evicted at first.
     *
     * The main idea is to check logs for special phrase that identifies eviction that has been started.
     * We check that if eviction of sync cache has started, no async cache eviction is possible until
     * all sync cache partitions are evicted.
     */
    public void testSyncCachesEvictedAtFirst() throws Exception {
        withSystemProperty(IgniteSystemProperties.IGNITE_EVICTION_PERMITS, "1");

        withSystemProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "500_000");

        withSystemProperty("SHOW_EVICTION_PROGRESS_FREQ", "0");

        setRootLoggerDebugLevel();

        IgniteEx node0 = startGrid(0);

        node0.cluster().active(true);

        IgniteEx node1 = startGrid(1);

        node0.cluster().setBaselineTopology(node1.cluster().topologyVersion());

        GridCacheAdapter<Object, Object> utilCache0 = grid(0).context().cache().internalCache(CU.UTILITY_CACHE_NAME);

        IgniteCache<Object, Object> cache = node0.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1000; i++) {
            utilCache0.put(i, i);

            cache.put(i, i);
        }

        awaitPartitionMapExchange();

        stopGrid(0);

        GridCacheAdapter<Object, Object> utilCache1 = grid(1).context().cache().internalCache(CU.UTILITY_CACHE_NAME);

        IgniteInternalCache<Object, Object> cache2 = grid(1).context().cache().cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 2000; i++) {
            try {
                cache2.put(i, i + 1);

                utilCache1.put(i, i + 1);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
        }

        TestLogListener listener = new TestLogListener();

        testLog.registerListener(listener);

        startGrid(0);

        awaitPartitionMapExchange(true, true, null);

        assertTrue(logParsed);

        assertTrue(sysCacheEvictStarted);

        assertTrue(firstSysPartEvictedAtFirst);

        assertTrue(sysCacheEvictEnded);

        assertFalse(asyncCacheEvicted);
    }

    /**
     * Listens to rows that starts with "Group eviction in progress [grpName="
     */
    class TestLogListener extends LogListener {
        /** */
        EvictionLogParams lastDfltGroupParams;

        int sysEvictProgressMessages;

        /** {@inheritDoc} */
        @Override public boolean check() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // no-op
        }

        /** {@inheritDoc} */
        @Override public void accept(String s) {
            if (s.contains("Partition has been scheduled for eviction [grp=" + CU.UTILITY_CACHE_NAME))
                sysCacheEvictQueued = true;


            if (s.contains("Group eviction in progress [grpName=" + CU.UTILITY_CACHE_NAME)) {
                EvictionLogParams sysCacheParam = new EvictionLogParams(s);

                sysEvictProgressMessages++;

                if (sysCacheParam.partsEvictInProgress > 0)
                    sysCacheEvictStarted = true;

                //Here we check that the next polled partition from the eviction queue after the first sync partition is scheduled
                // is sync partition
                if (sysCacheEvictQueued) {
                    if (sysEvictProgressMessages == 1)
                        return;
                    else
                        if (sysEvictProgressMessages == 2 && sysCacheParam.partsEvictInProgress > 0)
                            firstSysPartEvictedAtFirst = true;
                }

                if (sysCacheEvictStarted &&
                    (sysCacheParam.remainingPartsToEvict == 0 ||
                        (sysCacheParam.remainingPartsToEvict == 1 && sysCacheParam.partsEvictInProgress == 1)))
                    sysCacheEvictEnded = true;
            }

            if (s.contains("Group eviction in progress [grpName=" + DEFAULT_CACHE_NAME)) {
                EvictionLogParams curDfltGroupParams = new EvictionLogParams(s);

                if (lastDfltGroupParams != null && sysCacheEvictStarted && !sysCacheEvictEnded)
                    // Check that async cache partition has not been evicted during sync cache partitions eviction
                    if(lastDfltGroupParams.remainingPartsToEvict > curDfltGroupParams.remainingPartsToEvict)
                        asyncCacheEvicted = true;

                lastDfltGroupParams = curDfltGroupParams;
            }
        }
    }

    /**
     * Class for storing params from eviction log row, that starts with "Group eviction in progress [grpName="
     */
    private class EvictionLogParams {
        /** */
        int remainingPartsToEvict;

        /** */
        int partsEvictInProgress;

        /**
         * Parse params from eviction log row, that starts with "Group eviction in progress [grpName="
         *
         * @param s Log row.
         */
        EvictionLogParams(String s) {
            Pattern remPartsPattern = Pattern.compile("remainingPartsToEvict=([0-9]+)");

            Matcher matcher = remPartsPattern.matcher(s);

            if (matcher.find())
                remainingPartsToEvict = Integer.parseInt(matcher.group(1));
            else
                logParsed = false;

            Pattern partsInProgPattern = Pattern.compile("partsEvictInProgress=([0-9]+)");

            matcher = partsInProgPattern.matcher(s);

            if (matcher.find())
                partsEvictInProgress = Integer.parseInt(matcher.group(1));
            else
                logParsed = false;
        }
    }
}
