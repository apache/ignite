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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.PartitionLossPolicy.IGNORE;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_ALL;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;

/**
 *
 */
@RunWith(Parameterized.class)
public class IgniteCachePartitionLossPolicySelfTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS_CNT = 32;

    /** */
    private boolean client;

    /** */
    @Parameterized.Parameter(value = 0)
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(value = 1)
    public PartitionLossPolicy partLossPlc;

    /** */
    @Parameterized.Parameter(value = 2)
    public int backups;

    /** */
    @Parameterized.Parameter(value = 3)
    public boolean autoAdjust;

    /** */
    @Parameterized.Parameter(value = 4)
    public int nodes;

    /** */
    @Parameterized.Parameter(value = 5)
    public int[] stopNodes;

    /** */
    @Parameterized.Parameter(value = 6)
    public boolean persistence;

    /** */
    private static final String[] CACHES = new String[]{"cache1", "cache2"};

    /** */
    @Parameterized.Parameters(name = "{0} {1} {2} {3} {4} {6}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        Random r = new Random();
        System.out.println("Seed: " + U.field(r, "seed"));

        for (CacheAtomicityMode mode : Arrays.asList(TRANSACTIONAL, ATOMIC)) {
            // Test always scenarios.
            params.add(new Object[]{mode, IGNORE, 0, false, 3, new int[]{2}, false});
            params.add(new Object[]{mode, IGNORE, 0, false, 3, new int[]{2}, true});
            params.add(new Object[]{mode, READ_ONLY_SAFE, 1, true, 4, new int[]{2, 0}, false});
            params.add(new Object[]{mode, IGNORE, 1, false, 4, new int[]{0, 2}, false});
            params.add(new Object[]{mode, READ_WRITE_SAFE, 2, true, 5, new int[]{1, 0, 2}, false});

            // Random scenarios.
            for (Integer backups : Arrays.asList(0, 1, 2)) {
                int nodes = backups + 3;
                int[] stopIdxs = new int[backups + 1];

                List<Integer> tmp = IntStream.range(0, nodes).boxed().collect(Collectors.toList());
                Collections.shuffle(tmp, r);

                for (int i = 0; i < stopIdxs.length; i++)
                    stopIdxs[i] = tmp.get(i);

                params.add(new Object[]{mode, READ_WRITE_SAFE, backups, false, nodes, stopIdxs, false});
                params.add(new Object[]{mode, IGNORE, backups, false, nodes, stopIdxs, false});
                params.add(new Object[]{mode, READ_ONLY_SAFE, backups, false, nodes, stopIdxs, false});
                params.add(new Object[]{mode, READ_ONLY_ALL, backups, false, nodes, stopIdxs, false});
                params.add(new Object[]{mode, READ_WRITE_SAFE, backups, true, nodes, stopIdxs, false});
                params.add(new Object[]{mode, IGNORE, backups, true, nodes, stopIdxs, false});
                params.add(new Object[]{mode, READ_ONLY_SAFE, backups, true, nodes, stopIdxs, false});
                params.add(new Object[]{mode, READ_ONLY_ALL, backups, true, nodes, stopIdxs, false});

                boolean ignored = false; // Autoadjust is currently ignored for persistent mode.

                params.add(new Object[]{mode, READ_WRITE_SAFE, backups, ignored, nodes, stopIdxs, true});
                params.add(new Object[]{mode, IGNORE, backups, ignored, nodes, stopIdxs, true});
                params.add(new Object[]{mode, READ_ONLY_SAFE, backups, ignored, nodes, stopIdxs, true});
                params.add(new Object[]{mode, READ_ONLY_ALL, backups, ignored, nodes, stopIdxs, true});
                params.add(new Object[]{mode, READ_WRITE_SAFE, backups, ignored, nodes, stopIdxs, true});
                params.add(new Object[]{mode, IGNORE, backups, ignored, nodes, stopIdxs, true});
                params.add(new Object[]{mode, READ_ONLY_SAFE, backups, ignored, nodes, stopIdxs, true});
                params.add(new Object[]{mode, READ_ONLY_ALL, backups, ignored, nodes, stopIdxs, true});
            }
        }

        return params;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConsistentId(gridName);

        cfg.setClientMode(client);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setWalSegmentSize(4 * 1024 * 1024)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(persistence)
                        .setMaxSize(100L * 1024 * 1024))
        );

        CacheConfiguration[] ccfgs = new CacheConfiguration[CACHES.length];

        for (int i = 0; i < ccfgs.length; i++) {
            ccfgs[i] = new CacheConfiguration(CACHES[i])
                .setAtomicityMode(atomicityMode)
                .setCacheMode(PARTITIONED)
                .setBackups(backups)
                .setWriteSynchronizationMode(FULL_SYNC)
                .setPartitionLossPolicy(partLossPlc)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));
        }

        cfg.setCacheConfiguration(ccfgs);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void checkLostPartition() throws Exception {
        log.info("Stop sequence: " + IntStream.of(stopNodes).boxed().collect(Collectors.toList()));

        boolean safe = persistence || !(partLossPlc == IGNORE && autoAdjust);

        String cacheName = CACHES[ThreadLocalRandom.current().nextInt(CACHES.length)];

        Map<UUID, Set<Integer>> lostMap = new ConcurrentHashMap<>();

        Set<Integer> expLostParts = prepareTopology(nodes, autoAdjust, new P1<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt.type() == EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;

                CacheRebalancingEvent cacheEvt = (CacheRebalancingEvent)evt;

                lostMap.computeIfAbsent(evt.node().id(), k -> Collections.synchronizedSet(new HashSet<>())).add(cacheEvt.partition());

                return true;
            }
        }, stopNodes);

        int[] stopNodesSorted = Arrays.copyOf(stopNodes, stopNodes.length);
        Arrays.sort(stopNodesSorted);

        for (Ignite ig : G.allGrids()) {
            if (Arrays.binarySearch(stopNodesSorted, getTestIgniteInstanceIndex(ig.name())) >= 0)
                continue;

            verifyCacheOps(cacheName, expLostParts, ig, safe);
        }

        // Check that partition state does not change after we return nodes.
        for (int i = 0; i < stopNodes.length; i++) {
            int node = stopNodes[i];

            IgniteEx grd = startGrid(node);

            info("Newly started node: " + grd.cluster().localNode().id());
        }

        for (int i = 0; i < nodes + 1; i++)
            verifyCacheOps(cacheName, expLostParts, grid(i), safe);

        if (safe)
            ignite(0).resetLostPartitions(Arrays.asList(CACHES));

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

        if (safe) {
            for (Ignite ig : G.allGrids()) {
                if (Arrays.binarySearch(stopNodesSorted, getTestIgniteInstanceIndex(ig.name())) >= 0)
                    continue;

                Set<Integer> lostParts = lostMap.get(ig.cluster().localNode().id());

                assertEquals(expLostParts, lostParts);
            }
        }
    }

    /**
     * @param cacheName Cache name.
     * @param expLostParts Expected lost parts.
     * @param ig Ignite.
     * @param safe Safe.
     */
    private void verifyCacheOps(String cacheName, Set<Integer> expLostParts, Ignite ig, boolean safe) {
        boolean readOnly = partLossPlc == READ_ONLY_SAFE || partLossPlc == READ_ONLY_ALL;

        IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

        int parts = ig.affinity(cacheName).partitions();

        if (!safe)
            assertTrue(cache.lostPartitions().isEmpty());

        // Check single reads.
        for (int p = 0; p < parts; p++) {
            try {
                Integer actual = cache.get(p);

                if (safe) {
                    assertTrue("Reading from a lost partition should have failed [part=" + p + ']',
                        !cache.lostPartitions().contains(p));

                    assertEquals(p, actual.intValue());
                }
                else
                    assertEquals(expLostParts.contains(p) ? null : p, actual);
            }
            catch (CacheException e) {
                assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));

                assertTrue("Read exception should only be triggered for a lost partition " +
                    "[ex=" + X.getFullStackTrace(e) + ", part=" + p + ']', cache.lostPartitions().contains(p));
            }
        }

        // Check single writes.
        for (int p = 0; p < parts; p++) {
            try {
                cache.put(p, p);

                if (!safe && expLostParts.contains(p))
                    cache.remove(p);

                if (readOnly) {
                    assertTrue(!cache.lostPartitions().contains(p));

                    fail("Writing to a cache containing lost partitions should have failed [part=" + p + ']');
                }

                if (safe) {
                    assertTrue("Writing to a lost partition should have failed [part=" + p + ']',
                        !cache.lostPartitions().contains(p));
                }
            }
            catch (CacheException e) {
                assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));

                assertTrue("Write exception should only be triggered for a lost partition or in read-only mode " +
                    "[ex=" + X.getFullStackTrace(e) + ", part=" + p + ']', readOnly || cache.lostPartitions().contains(p));
            }
        }

        Set<Integer> notLost = IntStream.range(0, parts).boxed().filter(p -> !expLostParts.contains(p)).collect(Collectors.toSet());

        try {
            Map<Integer, Integer> res = cache.getAll(expLostParts);

            assertFalse("Reads from lost partitions should have been allowed only in non-safe mode", safe);
        }
        catch (CacheException e) {
            assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));
        }

        try {
            Map<Integer, Integer> res = cache.getAll(notLost);
        }
        catch (Exception e) {
            fail("Reads from non lost partitions should have been always allowed");
        }

        try {
            cache.putAll(expLostParts.stream().collect(Collectors.toMap(k -> k, v -> v)));

            assertFalse("Writes to lost partitions should have been allowed only in non-safe mode", safe);

            cache.removeAll(expLostParts);
        }
        catch (CacheException e) {
            assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));
        }

        try {
            cache.putAll(notLost.stream().collect(Collectors.toMap(k -> k, v -> v)));

            assertTrue("Writes to non-lost partitions should have been allowed only in read-write or non-safe mode",
                !safe || !readOnly);
        }
        catch (CacheException e) {
            assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));
        }

        // Check queries.
        for (int p = 0; p < parts; p++) {
            boolean loc = ig.affinity(cacheName).isPrimary(ig.cluster().localNode(), p);

            List<?> objects;

            try {
                objects = runQuery(ig, cacheName, false, p);

                assertTrue("Query over lost partition should have failed: safe=" + safe +
                    ", expLost=" + expLostParts + ", p=" + p, !safe || !expLostParts.contains(p));

                if (safe)
                    assertEquals(1, objects.size());
            } catch (Exception e) {
                assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));
            }

            try {
                runQuery(ig, cacheName, false, -1);

                assertFalse("Query should have failed in safe mode with lost partitions", safe);
            } catch (Exception e) {
                assertTrue("Query must always work in unsafe mode", safe);

                assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));
            }

            if (loc) {
                try {
                    objects = runQuery(ig, cacheName, true, p);

                    assertTrue("Query over lost partition should have failed: safe=" + safe +
                        ", expLost=" + expLostParts + ", p=" + p, !safe || !expLostParts.contains(p));

                    if (safe)
                        assertEquals(1, objects.size());
                } catch (Exception e) {
                    assertTrue(X.getFullStackTrace(e), X.hasCause(e, CacheInvalidStateException.class));
                }
            }
        }
    }

    /**
     * @param ig Ignite.
     * @param cacheName Cache name.
     * @param loc Local.
     * @param part Partition.
     */
    protected List<?> runQuery(Ignite ig, String cacheName, boolean loc, int part) {
        IgniteCache cache = ig.cache(cacheName);

        ScanQuery qry = new ScanQuery();

        if (part != -1)
            qry.setPartition(part);

        if (loc)
            qry.setLocal(true);

        return cache.query(qry).getAll();
    }

    /**
     * @param nodes Nodes.
     * @param autoAdjust Auto adjust.
     * @param lsnr Listener.
     * @param stopNodes Stop nodes.
     */
    private Set<Integer> prepareTopology(int nodes, boolean autoAdjust, P1<Event> lsnr, int... stopNodes) throws Exception {
        final IgniteEx crd = startGrids(nodes);
        crd.cluster().baselineAutoAdjustEnabled(autoAdjust);
        crd.cluster().active(true);

        Affinity<Object> aff = ignite(0).affinity(CACHES[0]);

        for (int i = 0; i < aff.partitions(); i++) {
            for (String cacheName0 : CACHES)
                ignite(0).cache(cacheName0).put(i, i);
        }

        client = true;

        startGrid(nodes);

        client = false;

        for (int i = 0; i < nodes; i++)
            info(">>> Node [idx=" + i + ", nodeId=" + ignite(i).cluster().localNode().id() + ']');

        awaitPartitionMapExchange();

        Set<Integer> expLostParts = new HashSet<>();

        int[] stopNodesSorted = Arrays.copyOf(stopNodes, stopNodes.length);
        Arrays.sort(stopNodesSorted);

        // Find partitions not owned by any remaining node.
        for (int i = 0; i < PARTS_CNT; i++) {
            int c = 0;

            for (int idx = 0; idx < nodes; idx++) {
                if (Arrays.binarySearch(stopNodesSorted, idx) < 0 && !aff.isPrimary(grid(idx).localNode(), i) && !aff.isBackup(grid(idx).localNode(), i))
                    c++;
            }

            if (c == nodes - stopNodes.length)
                expLostParts.add(i);
        }

        assertFalse("Expecting lost partitions for the test scneario", expLostParts.isEmpty());

        for (Ignite ignite : G.allGrids()) {
            // Prevent rebalancing to bring partitions in owning state.
            if (backups > 0) {
                TestRecordingCommunicationSpi.spi(ignite).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                    @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                        return msg instanceof GridDhtPartitionDemandMessage;
                    }
                });
            }

            ignite.events().localListen(lsnr, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);
        }

        for (int i = 0; i < stopNodes.length; i++)
            stopGrid(stopNodes[i], true);

        return expLostParts;
    }
}
