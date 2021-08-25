/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.partId;
import static org.apache.ignite.testframework.GridTestUtils.DFLT_TEST_TIMEOUT;

/** */
public class IgniteSnapshotRemoteRequestTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRemoteRequest() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        Map<Integer, Set<Integer>> parts = owningParts(ignite,
            new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))),
            grid(1).localNode().id());

        Map<Integer, Set<Integer>> parts0 = new HashMap<>();

        for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet())
            parts0.computeIfAbsent(e.getKey(), k -> new HashSet<>()).addAll(e.getValue());

        awaitPartitionMapExchange();
        IgniteSnapshotManager mgr = snp(ignite);

        IgniteInternalFuture<?> fut = mgr.requestRemoteSnapshot(grid(1).localNode().id(),
            SNAPSHOT_NAME,
            parts,
            (part, gpId) -> {
                assertEquals(partId(part.getName()), gpId.getPartitionId());
                assertTrue(parts0.get(gpId.getGroupId()).remove(gpId.getPartitionId()));
            });

        fut.get(DFLT_TEST_TIMEOUT);

        assertEquals(F.size(parts0.values(), Set::isEmpty), parts0.size());
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRemoteRequestEachOther() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        IgniteSnapshotManager mgr0 = snp(ignite);
        IgniteSnapshotManager mgr1 = snp(grid(1));

        UUID node0 = grid(0).localNode().id();
        UUID node1 = grid(1).localNode().id();

        Map<Integer, Set<Integer>> fromNode1 = owningParts(ignite,
            new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))),
            node1);

        Map<Integer, Set<Integer>> fromNode0 = owningParts(grid(1),
            new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))),
            node0);

        G.allGrids().forEach(g -> TestRecordingCommunicationSpi.spi(g)
            .blockMessages((n, msg) -> msg instanceof SnapshotRequestMessage));

        // Snapshot must be taken on node1 and transmitted to node0.
        IgniteInternalFuture<?> futFrom1To0 = mgr0.requestRemoteSnapshot(node1, SNAPSHOT_NAME, fromNode1,
            (part, pair) -> assertTrue("Received partition has not been requested",
                    fromNode1.get(pair.getGroupId()).remove(pair.getPartitionId())));
        IgniteInternalFuture<?> futFrom0To1 = mgr1.requestRemoteSnapshot(node0, SNAPSHOT_NAME, fromNode0,
            (part, pair) -> assertTrue("Received partition has not been requested",
                    fromNode0.get(pair.getGroupId()).remove(pair.getPartitionId())));

        G.allGrids().forEach(g -> TestRecordingCommunicationSpi.spi(g).stopBlock());

        futFrom0To1.get();
        futFrom1To0.get();

        assertTrue("Not all of partitions have been received: " + fromNode1,
            fromNode1.get(CU.cacheId(DEFAULT_CACHE_NAME)).isEmpty());
        assertTrue("Not all of partitions have been received: " + fromNode0,
            fromNode0.get(CU.cacheId(DEFAULT_CACHE_NAME)).isEmpty());
    }

    /** @throws Exception If fails. */
    @Test(expected = ClusterTopologyCheckedException.class)
    public void testRemoteRequestedInitiatorNodeLeft() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        awaitPartitionMapExchange();

        IgniteSnapshotManager mgr1 = snp(grid(1));
        UUID rmtNodeId = grid(1).localNode().id();
        UUID locNodeId = grid(0).localNode().id();

        Map<Integer, Set<Integer>> parts = owningParts(ignite, Collections.singleton(CU.cacheId(DEFAULT_CACHE_NAME)), rmtNodeId);

        CountDownLatch sndLatch = new CountDownLatch(1);

        mgr1.remoteSnapshotSenderFactory(new BiFunction<String, UUID, SnapshotSender>() {
            @Override public SnapshotSender apply(String s, UUID uuid) {
                return new DelegateSnapshotSender(log, mgr1.snapshotExecutorService(), mgr1.remoteSnapshotSenderFactory(s, uuid)) {
                    @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                        if (partId(part.getName()) > 0) {
                            try {
                                sndLatch.await(DFLT_TEST_TIMEOUT, TimeUnit.MILLISECONDS);
                            }
                            catch (Exception e) {
                                throw new IgniteException(e);
                            }
                        }

                        super.sendPart0(part, cacheDirName, pair, length);
                    }
                };
            }
        });

        snp(ignite).requestRemoteSnapshot(grid(1).localNode().id(),
            SNAPSHOT_NAME,
            parts,
            (part, grp) -> { });

        IgniteInternalFuture<?>[] futs = new IgniteInternalFuture[1];

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                IgniteInternalFuture<?> snpFut = snp(grid(1)).lastScheduledSnapshotResponseRemoteTask(locNodeId);

                if (snpFut == null)
                    return false;
                else {
                    futs[0] = snpFut;

                    return true;
                }
            }
        }, 5_000L));

        stopGrid(0);
        sndLatch.countDown();

        futs[0].get();
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRequestRemoteNodeLeft() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        awaitPartitionMapExchange();

        Map<Integer, Set<Integer>> parts = owningParts(ignite, Collections.singleton(CU.cacheId(DEFAULT_CACHE_NAME)),
            grid(1).localNode().id());

        CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = snp(ignite).requestRemoteSnapshot(grid(1).localNode().id(),
            SNAPSHOT_NAME,
            parts,
            (part, gpId) -> {
                assertEquals(partId(part.getName()), gpId.getPartitionId());
                assertTrue(parts.get(gpId.getGroupId()).remove(gpId.getPartitionId()));

                latch.countDown();
            });

        latch.await(DFLT_TEST_TIMEOUT, TimeUnit.MILLISECONDS);

        stopGrid(1);

        fut.get(DFLT_TEST_TIMEOUT);
    }

    /**
     * @param src Source node to calculate.
     * @param grps Groups to collect owning parts.
     * @param rmtNodeId Remote node id.
     * @return Map of collected parts.
     */
    private static Map<Integer, Set<Integer>> owningParts(IgniteEx src, Set<Integer> grps, UUID rmtNodeId) {
        Map<Integer, Set<Integer>> result = new HashMap<>();

        for (Integer grpId : grps) {
            Set<Integer> parts = src.context()
                .cache()
                .cacheGroup(grpId)
                .topology()
                .partitions(rmtNodeId)
                .entrySet()
                .stream()
                .filter(p -> p.getValue() == GridDhtPartitionState.OWNING)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            result.put(grpId, parts);
        }

        return result;
    }
}
