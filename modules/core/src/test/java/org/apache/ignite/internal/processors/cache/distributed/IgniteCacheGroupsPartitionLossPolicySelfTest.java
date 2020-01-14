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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheGroupsPartitionLossPolicySelfTest extends GridCommonAbstractTest {
    /** */
    private boolean client;

    /** */
    private PartitionLossPolicy partLossPlc;

    /** */
    private static final String GROUP_NAME = "group";

    /** */
    private static final String CACHE_1 = "cache1";

    /** */
    private static final String CACHE_2 = "cache2";

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(client);

        CacheConfiguration ccfg1 = new CacheConfiguration(CACHE_1)
            .setGroupName(GROUP_NAME)
            .setCacheMode(PARTITIONED)
            .setBackups(0)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setPartitionLossPolicy(partLossPlc)
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        CacheConfiguration ccfg2 = new CacheConfiguration(ccfg1)
            .setName(CACHE_2);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadOnlySafe() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_ONLY_SAFE;

        checkLostPartition(false, true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadOnlyAll() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_ONLY_ALL;

        checkLostPartition(false, false);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafe() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteAll() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_ALL;

        checkLostPartition(true, false);
    }

    /**
     * @throws Exception if failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-5078")
    @Test
    public void testIgnore() throws Exception {
        prepareTopology();

        String cacheName = ThreadLocalRandom.current().nextBoolean() ? CACHE_1 : CACHE_2;

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

            Collection<Integer> lost = cache.lostPartitions();

            assertTrue("[grid=" + ig.name() + ", lost=" + lost.toString() + ']', lost.isEmpty());

            int parts = ig.affinity(cacheName).partitions();

            for (int i = 0; i < parts; i++) {
                cache.get(i);

                cache.put(i, i);
            }
        }
    }

    /**
     * @param canWrite {@code True} if writes are allowed.
     * @param safe {@code True} if lost partition should trigger exception.
     * @throws Exception if failed.
     */
    private void checkLostPartition(boolean canWrite, boolean safe) throws Exception {
        assert partLossPlc != null;

        String cacheName = ThreadLocalRandom.current().nextBoolean() ? CACHE_1 : CACHE_2;

        int part = prepareTopology();

        for (Ignite ig : G.allGrids()) {
            info("Checking node: " + ig.cluster().localNode().id());

            verifyCacheOps(cacheName, canWrite, safe, part, ig);

            IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

            // Check we can read and write to lost partition in recovery mode.
            IgniteCache<Integer, Integer> recoverCache = cache.withPartitionRecover();

            for (int lostPart : recoverCache.lostPartitions()) {
                recoverCache.get(lostPart);
                recoverCache.put(lostPart, lostPart);
            }

            // Check that writing in recover mode does not clear partition state.
            verifyCacheOps(cacheName, canWrite, safe, part, ig);
        }

        // Check that partition state does not change after we start a new node.
        IgniteEx grd = startGrid(3);

        info("Newly started node: " + grd.cluster().localNode().id());

        for (Ignite ig : G.allGrids())
            verifyCacheOps(cacheName, canWrite, safe, part, ig);

        ignite(0).resetLostPartitions(F.asList(CACHE_1, CACHE_2));

        awaitPartitionMapExchange(true, true, null);

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

            assertTrue(cache.lostPartitions().isEmpty());

            int parts = ig.affinity(cacheName).partitions();

            for (int i = 0; i < parts; i++) {
                cache.get(i);

                cache.put(i, i);
            }
        }
    }

    /**
     *
     * @param canWrite {@code True} if writes are allowed.
     * @param safe {@code True} if lost partition should trigger exception.
     * @param part Lost partition ID.
     * @param ig Ignite instance.
     */
    private void verifyCacheOps(String cacheName, boolean canWrite, boolean safe, int part, Ignite ig) {
        IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

        Collection<Integer> lost = cache.lostPartitions();

        assertTrue("Failed to find expected lost partition [exp=" + part + ", lost=" + lost + ']',
            lost.contains(part));

        int parts = ig.affinity(cacheName).partitions();

        // Check read.
        for (int i = 0; i < parts; i++) {
            try {
                Integer actual = cache.get(i);

                if (cache.lostPartitions().contains(i)) {
                    if (safe)
                        fail("Reading from a lost partition should have failed: " + i);
                    // else we could have read anything.
                }
                else
                    assertEquals((Integer)i, actual);
            }
            catch (CacheException e) {
                assertTrue("Read exception should only be triggered in safe mode: " + e, safe);
                assertTrue("Read exception should only be triggered for a lost partition " +
                    "[ex=" + e + ", part=" + i + ']', cache.lostPartitions().contains(i));
            }
        }

        // Check write.
        for (int i = 0; i < parts; i++) {
            try {
                cache.put(i, i);

                assertTrue("Write in read-only mode should be forbidden: " + i, canWrite);

                if (cache.lostPartitions().contains(i))
                    assertFalse("Writing to a lost partition should have failed: " + i, safe);
            }
            catch (CacheException e) {
                if (canWrite) {
                    assertTrue("Write exception should only be triggered in safe mode: " + e, safe);
                    assertTrue("Write exception should only be triggered for a lost partition: " + e,
                        cache.lostPartitions().contains(i));
                }
                // else expected exception regardless of partition.
            }
        }
    }

    /**
     * @return Lost partition ID.
     * @throws Exception If failed.
     */
    private int prepareTopology() throws Exception {
        startGrids(4);

        final String cacheName = ThreadLocalRandom.current().nextBoolean() ? CACHE_1 : CACHE_2;

        Affinity<Object> aff = ignite(0).affinity(cacheName);

        for (int i = 0; i < aff.partitions(); i++) {
            ignite(0).cache(CACHE_1).put(i, i);
            ignite(0).cache(CACHE_2).put(i, i);
        }

        client = true;

        startGrid(4);

        client = false;

        for (int i = 0; i < 5; i++)
            info(">>> Node [idx=" + i + ", nodeId=" + ignite(i).cluster().localNode().id() + ']');

        awaitPartitionMapExchange();

        ClusterNode killNode = ignite(3).cluster().localNode();

        int part = -1;

        for (int i = 0; i < aff.partitions(); i++) {
            if (aff.isPrimary(killNode, i)) {
                part = i;

                break;
            }
        }

        if (part == -1)
            throw new IllegalStateException("No partition on node: " + killNode);

        final CountDownLatch[] partLost = new CountDownLatch[3];

        // Check events.
        for (int i = 0; i < 3; i++) {
            final CountDownLatch latch = new CountDownLatch(1);
            partLost[i] = latch;

            final int part0 = part;

            grid(i).events().localListen(new P1<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt.type() == EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;

                    CacheRebalancingEvent cacheEvt = (CacheRebalancingEvent)evt;

                    if (cacheEvt.partition() == part0 && F.eq(cacheName, cacheEvt.cacheName())) {
                        latch.countDown();

                        // Auto-unsubscribe.
                        return false;
                    }

                    return true;
                }
            }, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);
        }

        ignite(3).close();

        for (CountDownLatch latch : partLost)
            assertTrue("Failed to wait for partition LOST event", latch.await(10, TimeUnit.SECONDS));

        return part;
    }
}
