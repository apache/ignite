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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestDelayingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

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

    /** */
    private int backups = 0;

    /** */
    private final AtomicBoolean delayPartExchange = new AtomicBoolean(false);

    /** */
    private final TopologyChanger killSingleNode = new TopologyChanger(false, Arrays.asList(3), Arrays.asList(0, 1, 2, 4),0);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setCommunicationSpi(new TestDelayingCommunicationSpi() {
            @Override protected boolean delayMessage(Message msg, GridIoMessage ioMsg) {
                return delayPartExchange.get() && (msg instanceof GridDhtPartitionsFullMessage || msg instanceof GridDhtPartitionsAbstractMessage);
            }

            @Override protected int delayMillis() {
                return 250;
            }
        });

        cfg.setClientMode(client);

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(CACHE_NAME);

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(backups);
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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        delayPartExchange.set(false);

        backups = 0;
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadOnlySafe() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_ONLY_SAFE;

        checkLostPartition(false, true, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadOnlyAll() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_ONLY_ALL;

        checkLostPartition(false, false, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafe() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteAll() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_ALL;

        checkLostPartition(true, false, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeAfterKillTwoNodes() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, new TopologyChanger(false, Arrays.asList(3, 2), Arrays.asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeAfterKillTwoNodesWithDelay() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, new TopologyChanger(false, Arrays.asList(3, 2), Arrays.asList(0, 1, 4), 20));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeWithBackupsAfterKillThreeNodes() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        checkLostPartition(true, true, new TopologyChanger(true, Arrays.asList(3, 2, 1), Arrays.asList(0, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeAfterKillCrd() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, new TopologyChanger(true, Arrays.asList(3, 0), Arrays.asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeWithBackups() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        checkLostPartition(true, true, new TopologyChanger(true, Arrays.asList(3, 2), Arrays.asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeWithBackupsAfterKillCrd() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        checkLostPartition(true, true, new TopologyChanger(true, Arrays.asList(3, 0), Arrays.asList(1, 2, 4), 0));
    }

    /**
     * @param topChanger topology changer.
     * @throws Exception if failed.
     */
    public void testIgnore(TopologyChanger topChanger) throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5078");

        topChanger.changeTopology();

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Integer, Integer> cache = ig.cache(CACHE_NAME);

            Collection<Integer> lost = cache.lostPartitions();

            assertTrue("[grid=" + ig.name() + ", lost=" + lost.toString() + ']', lost.isEmpty());

            int parts = ig.affinity(CACHE_NAME).partitions();

            for (int i = 0; i < parts; i++) {
                cache.get(i);

                cache.put(i, i);
            }
        }
    }

    /**
     * @param canWrite {@code True} if writes are allowed.
     * @param safe {@code True} if lost partition should trigger exception.
     * @param topChanger topology changer.
     * @throws Exception if failed.
     */
    private void checkLostPartition(boolean canWrite, boolean safe, TopologyChanger topChanger) throws Exception {
        assert partLossPlc != null;

        int part = topChanger.changeTopology().get(0);

        // Wait for all grids (servers and client) have same topology version
        // to make sure that all nodes received map with lost partition.
        GridTestUtils.waitForCondition(() -> {
            AffinityTopologyVersion last = null;
            for (Ignite ig : G.allGrids()) {
                AffinityTopologyVersion ver = ((IgniteEx) ig).context().cache().context().exchange().readyAffinityVersion();

                if (last != null && !last.equals(ver))
                    return false;

                last = ver;
            }

            return true;
        }, 10000);

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
        IgniteEx grd = startGrid(3);

        info("Newly started node: " + grd.cluster().localNode().id());

        for (Ignite ig : G.allGrids())
            verifyCacheOps(canWrite, safe, part, ig);

        ignite(4).resetLostPartitions(Collections.singletonList(CACHE_NAME));

        awaitPartitionMapExchange(true, true, null);

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
                Integer actual = cache.get(i);

                if (cache.lostPartitions().contains(i)) {
                    if (safe)
                        fail("Reading from a lost partition should have failed: " + i + " " + ig.name());
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
     * @param nodes List of nodes to find partition.
     * @return List of partitions that aren't primary or backup for specified nodes.
     */
    protected List<Integer> noPrimaryOrBackupPartition(List<Integer> nodes) {
        Affinity<Object> aff = ignite(4).affinity(CACHE_NAME);

        List<Integer> parts = new ArrayList<>();

        Integer part;

        for (int i = 0; i < aff.partitions(); i++) {
            part = i;

            for (Integer id : nodes) {
                if (aff.isPrimaryOrBackup(grid(id).cluster().localNode(), i)) {
                    part = null;

                    break;
                }
            }

            if (part != null)
                parts.add(i);
        }

        return parts;
    }

    /** */
    class TopologyChanger {
        /** Flag to delay partition exchange */
        private boolean delayExchange;

        /** List of nodes to kill */
        private List<Integer> killNodes;

        /** List of nodes to be alive */
        private List<Integer> aliveNodes;

        /** Delay between node stops */
        private long stopDelay;

        /**
         * @param delayExchange Flag for delay partition exchange.
         * @param killNodes List of nodes to kill.
         * @param aliveNodes List of nodes to be alive.
         * @param stopDelay Delay between stopping nodes.
         */
        public TopologyChanger(boolean delayExchange, List<Integer> killNodes, List<Integer> aliveNodes,
            long stopDelay) {
            this.delayExchange = delayExchange;
            this.killNodes = killNodes;
            this.aliveNodes = aliveNodes;
            this.stopDelay = stopDelay;
        }

        /**
         * @return Lost partition ID.
         * @throws Exception If failed.
         */
        protected List<Integer> changeTopology() throws Exception {
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

            final List<Integer> parts = noPrimaryOrBackupPartition(aliveNodes);

            if (parts.size() == 0)
                throw new IllegalStateException("No partition on nodes: " + killNodes);

            final List<Map<Integer, Semaphore>> lostMap = new ArrayList<>();

            for (int i : aliveNodes) {
                HashMap<Integer, Semaphore> semaphoreMap = new HashMap<>();

                for (Integer part : parts)
                    semaphoreMap.put(part, new Semaphore(0));

                lostMap.add(semaphoreMap);


                grid(i).events().localListen(new P1<Event>() {
                    @Override public boolean apply(Event evt) {
                        assert evt.type() == EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;

                        CacheRebalancingEvent cacheEvt = (CacheRebalancingEvent)evt;

                        if (F.eq(CACHE_NAME, cacheEvt.cacheName())) {
                            if (semaphoreMap.containsKey(cacheEvt.partition()))
                                semaphoreMap.get(cacheEvt.partition()).release();
                        }

                        return true;
                    }
                }, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

            }

            if (delayExchange)
                delayPartExchange.set(true);

            ExecutorService executor = Executors.newFixedThreadPool(killNodes.size());

            for (Integer node : killNodes) {
                executor.submit(new Runnable() {
                    @Override public void run() {
                        grid(node).close();
                    }
                });

                Thread.sleep(stopDelay);
            }

            executor.shutdown();

            delayPartExchange.set(false);

            Thread.sleep(5_000L);

            for (Map<Integer, Semaphore> map : lostMap) {
                for (Map.Entry<Integer, Semaphore> entry : map.entrySet())
                    assertTrue("Failed to wait for partition LOST event for partition:" + entry.getKey(), entry.getValue().tryAcquire(1));
            }

            for (Map<Integer, Semaphore> map : lostMap) {
                for (Map.Entry<Integer, Semaphore> entry : map.entrySet())
                    assertFalse("Partition LOST event raised twice for partition:" + entry.getKey(), entry.getValue().tryAcquire(1));
            }

            return parts;
        }
    }

}
