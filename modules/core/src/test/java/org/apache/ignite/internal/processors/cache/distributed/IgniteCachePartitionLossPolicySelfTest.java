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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.TestTcpCommunicationSpi;

import javax.cache.CacheException;
import java.util.Collection;
import java.util.UUID;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCachePartitionLossPolicySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private PartitionLossPolicy partLossPlc;

    /** */
    private static final String CACHE_NAME = "partitioned";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(disco);

        if (gridName.matches(".*\\d")) {
            String idStr = UUID.randomUUID().toString();

            char[] chars = idStr.toCharArray();

            chars[chars.length - 3] = '0';
            chars[chars.length - 2] = '0';
            chars[chars.length - 1] = gridName.charAt(gridName.length() - 1);

            cfg.setNodeId(UUID.fromString(new String(chars)));
        }

        cfg.setCommunicationSpi(new TestTcpCommunicationSpi());
        cfg.setClientMode(client);

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(CACHE_NAME);

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(0);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setPartitionLossPolicy(partLossPlc);
        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadOnlySafe() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_ONLY_SAFE;

        checkLostPartition(false, true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadOnlyAll() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_ONLY_ALL;

        checkLostPartition(false, false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafe() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteAll() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_ALL;

        checkLostPartition(true, false);
    }

    /**
     * @param canWrite {@code True} if writes are allowed.
     * @param safe {@code True} if lost partition should trigger exception.
     * @throws Exception if failed.
     */
    private void checkLostPartition(boolean canWrite, boolean safe) throws Exception {
        assert partLossPlc != null;

        int part = prepareTopology();

        for (Ignite ig : G.allGrids()) {
            info("Checking node: " + ig.cluster().localNode().id());

            IgniteCache<Integer, Integer> cache = ig.cache(CACHE_NAME);

            verifyCacheOps(canWrite, safe, part, ig);

            // Check we can read and write to lost partition in recovery mode.
            IgniteCache<Integer, Integer> recoverCache = cache.withPartitionRecover();

            for (int lostPart : recoverCache.lostPartitions()) {
                recoverCache.get(lostPart);
                recoverCache.put(lostPart, lostPart);
            }

            // Check that writing in recover mode does not clear partition state.
            verifyCacheOps(canWrite, safe, part, ig);
        }

        // Check that partition state does not change after we start a new node.
        startGrid(3);

        for (Ignite ig : G.allGrids())
            verifyCacheOps(canWrite, safe, part, ig);

        ignite(0).cache(CACHE_NAME).resetLostPartitions();

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Integer, Integer> cache = ig.cache(CACHE_NAME);

            assertTrue(cache.lostPartitions().isEmpty());

            int parts = ig.affinity(CACHE_NAME).partitions();

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
    private void verifyCacheOps(boolean canWrite, boolean safe, int part, Ignite ig) {
        IgniteCache<Integer, Integer> cache = ig.cache(CACHE_NAME);

        Collection<Integer> lost = cache.lostPartitions();

        assertTrue("Failed to find expected lost partition [exp=" + part + ", lost=" + lost + ']',
            lost.contains(part));

        int parts = ig.affinity(CACHE_NAME).partitions();

        // Check read.
        for (int i = 0; i < parts; i++) {
            try {
                assertEquals((Integer)i, cache.get(i));

                if (cache.lostPartitions().contains(i) && safe)
                    fail("Reading from a lost partition should have failed: " + i);
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

        Affinity<Object> aff = ignite(0).affinity(CACHE_NAME);

        for (int i = 0; i < aff.partitions(); i++)
            ignite(0).cache(CACHE_NAME).put(i, i);

        client = true;

        startGrid(4);

        client = false;

        for (int i = 0; i < 5; i++)
            info(">>> Node [idx=" + i + ", nodeId=" + ignite(i).cluster().localNode().id() + ']');

        awaitPartitionMapExchange();

        U.sleep(5_000);

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

        ignite(3).close();

        U.sleep(1_000);

        return part;
    }
}
