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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test counts reservations on partitions during cross cache backup tx commit and expects them to be the same.
 * This is achieved by mocking partition on backup node and counting reservations.
 */
public class TxCrossCacheRemoteMultiplePartitionReservationTest extends GridCommonAbstractTest {
    /** Cache 1. */
    private static final String CACHE1 = DEFAULT_CACHE_NAME;

    /** Cache 2. */
    private static final String CACHE2 = DEFAULT_CACHE_NAME + "2";

    /** */
    private static final int MB = 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setCacheConfiguration(cacheConfiguration(CACHE1), cacheConfiguration(CACHE2));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setPageSize(1024).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().
                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        return cfg;
    }

    /**
     * @param name Name.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(String name) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(name);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /** */
    @Test
    public void testRemoteCommitPartitionReservations() throws Exception {
        try {
            IgniteEx crd = startGrids(2);

            awaitPartitionMapExchange();

            IgniteEx client = startClientGrid("client");
            IgniteCache<Object, Object> cache1 = client.cache(CACHE1);
            IgniteCache<Object, Object> cache2 = client.cache(CACHE2);

            List<Integer> evictingIds = evictingPartitionsAfterJoin(crd, crd.cache(CACHE1), 10);

            int[] backupParts = crd.affinity(CACHE1).backupPartitions(crd.localNode());

            Arrays.sort(backupParts);

            int evictingBackupPartId = -1;

            for (int id : evictingIds) {
                if (Arrays.binarySearch(backupParts, id) >= 0) {
                    evictingBackupPartId = id;

                    break;
                }
            }

            assertTrue(evictingBackupPartId != -1);

            startGrid(2);

            awaitPartitionMapExchange(true, true, null);

            // Mock partition after re-create.
            final int finalEvictingBackupPartId = evictingBackupPartId;

            Map<Integer, AtomicInteger> reserveCntrs = new ConcurrentHashMap<>();

            GridDhtPartitionTopologyImpl.PartitionFactory factory = new GridDhtPartitionTopologyImpl.PartitionFactory() {
                @Override public GridDhtLocalPartition create(GridCacheSharedContext ctx, CacheGroupContext grp, int id) {
                    return id != finalEvictingBackupPartId ? new GridDhtLocalPartition(ctx, grp, id, false) :
                        new GridDhtLocalPartition(ctx, grp, id, false) {
                            @Override public boolean reserve() {
                                reserveCntrs.computeIfAbsent(grp.groupId(), integer -> new AtomicInteger()).incrementAndGet();

                                return super.reserve();
                            }
                        };
                }
            };

            Stream.of(CACHE1, CACHE2).map(cache -> (GridDhtPartitionTopologyImpl)crd.cachex(cache).context().topology()).
                forEach(topology -> topology.partitionFactory(factory));

            stopGrid(2);

            awaitPartitionMapExchange(true, true, null);

            reserveCntrs.values().forEach(cntr -> cntr.set(0));

            // By this moment a backup partition with id=evictingBackupPartId is mocked and will count reservations on
            // backup commits.
            try (Transaction tx = client.transactions().txStart()) {
                cache1.put(evictingBackupPartId, 0);
                cache2.put(evictingBackupPartId, 0);

                tx.commit();
            }

            assertEquals("Expecting same reservations count for all caches [cntrs=" + reserveCntrs.toString() + ']',
                1, reserveCntrs.values().stream().map(AtomicInteger::get).distinct().count());
        }
        finally {
            stopAllGrids();
        }
    }
}
