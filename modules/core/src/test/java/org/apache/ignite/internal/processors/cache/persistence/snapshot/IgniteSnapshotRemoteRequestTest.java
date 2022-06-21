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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.TransmissionCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.partId;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.groupIdFromTmpDir;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class IgniteSnapshotRemoteRequestTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRemoteRequestFromSingleNode() throws Exception {
        int rqCnt = 10;

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        Map<Integer, Set<Integer>> parts = owningParts(ignite, CU.cacheId(DEFAULT_CACHE_NAME), grid(1).localNode().id());

        awaitPartitionMapExchange();

        CountDownLatch latch = new CountDownLatch(parts.values().stream().mapToInt(Set::size).sum() * rqCnt);
        GridCompoundFuture<Void, Void> compFut = new GridCompoundFuture<>();

        IgniteInternalFuture<?> runFut = GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                Map<Integer, Set<Integer>> parts0 = new HashMap<>();

                for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet())
                    parts0.computeIfAbsent(e.getKey(), k -> new HashSet<>()).addAll(e.getValue());

                IgniteInternalFuture<Void> locFut;

                compFut.add(locFut = snp(ignite).requestRemoteSnapshotFiles(grid(1).localNode().id(),
                    SNAPSHOT_NAME,
                    null,
                    parts,
                    () -> false,
                    defaultPartitionConsumer(parts0, latch)));

                locFut.listen(f -> assertEquals("All partitions must be handled: " + parts0,
                    F.size(parts0.values(), Set::isEmpty), parts0.size()));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }, rqCnt, "rq-creator-");

        runFut.get(TIMEOUT);
        U.await(latch, TIMEOUT, TimeUnit.MILLISECONDS);
        compFut.markInitialized().get(TIMEOUT);
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

        Map<Integer, Set<Integer>> fromNode1 = owningParts(ignite, CU.cacheId(DEFAULT_CACHE_NAME), node1);
        Map<Integer, Set<Integer>> fromNode0 = owningParts(grid(1), CU.cacheId(DEFAULT_CACHE_NAME), node0);

        G.allGrids().forEach(g -> TestRecordingCommunicationSpi.spi(g)
            .blockMessages((n, msg) -> msg instanceof SnapshotFilesRequestMessage));

        CountDownLatch latch = new CountDownLatch(fromNode1.values().stream().mapToInt(Set::size).sum() +
            fromNode0.values().stream().mapToInt(Set::size).sum());

        // Snapshot must be taken on node1 and transmitted to node0.
        IgniteInternalFuture<?> futFrom1To0 = mgr0.requestRemoteSnapshotFiles(node1, SNAPSHOT_NAME, null, fromNode1, () -> false,
            defaultPartitionConsumer(fromNode1, latch));
        IgniteInternalFuture<?> futFrom0To1 = mgr1.requestRemoteSnapshotFiles(node0, SNAPSHOT_NAME, null, fromNode0, () -> false,
            defaultPartitionConsumer(fromNode0, latch));

        G.allGrids().forEach(g -> TestRecordingCommunicationSpi.spi(g).stopBlock());

        latch.await(TIMEOUT, TimeUnit.MILLISECONDS);

        futFrom0To1.get(TIMEOUT);
        futFrom1To0.get(TIMEOUT);
    }

    /** @throws Exception If fails. */
    @Test
    public void testRemoteRequestedInitiatorNodeLeft() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        awaitPartitionMapExchange();

        IgniteSnapshotManager mgr1 = snp(grid(1));
        UUID rmtNodeId = grid(1).localNode().id();
        UUID locNodeId = grid(0).localNode().id();

        Map<Integer, Set<Integer>> parts = owningParts(ignite, CU.cacheId(DEFAULT_CACHE_NAME), rmtNodeId);

        CountDownLatch sndLatch = new CountDownLatch(1);

        mgr1.remoteSnapshotSenderFactory(new BiFunction<String, UUID, SnapshotSender>() {
            @Override public SnapshotSender apply(String s, UUID uuid) {
                return new DelegateSnapshotSender(log, mgr1.snapshotExecutorService(), mgr1.remoteSnapshotSenderFactory(s, uuid)) {
                    @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                        if (partId(part.getName()) > 0) {
                            try {
                                sndLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);
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

        snp(ignite).requestRemoteSnapshotFiles(grid(1).localNode().id(),
            SNAPSHOT_NAME,
            null,
            parts,
            () -> false,
            (part, t) -> {
            });

        IgniteInternalFuture<?>[] futs = new IgniteInternalFuture[1];

        assertTrue(waitForCondition(new GridAbsPredicate() {
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

        GridTestUtils.assertThrowsAnyCause(log, () -> futs[0].get(TIMEOUT), ClusterTopologyCheckedException.class, null);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRequestRemoteSourceNodeLeft() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        Map<Integer, Set<Integer>> parts = owningParts(ignite, CU.cacheId(DEFAULT_CACHE_NAME),
            grid(1).localNode().id());

        CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = snp(ignite).requestRemoteSnapshotFiles(grid(1).localNode().id(),
            SNAPSHOT_NAME,
            null,
            parts,
            () -> false,
            (part, t) -> {
                if (t == null) {
                    int grpId = groupIdFromTmpDir(part.getParentFile());

                    assertTrue("Received cache group has not been requested", parts.containsKey(grpId));
                    assertTrue("Received partition has not been requested",
                        parts.get(grpId).contains(partId(part.getName())));

                    try {
                        U.await(latch, TIMEOUT, TimeUnit.MILLISECONDS);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
                else {
                    assertTrue(t instanceof ClusterTopologyCheckedException);
                    assertNull(part);
                }
            });

        stopGrid(1);

        latch.countDown();

        assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), ClusterTopologyCheckedException.class,
            "he node from which a snapshot has been requested left the grid");
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRequestRemoteCancel() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        Map<Integer, Set<Integer>> parts = owningParts(ignite, CU.cacheId(DEFAULT_CACHE_NAME),
            grid(1).localNode().id());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean stopChecker = new AtomicBoolean();

        IgniteInternalFuture<Void> fut = snp(ignite).requestRemoteSnapshotFiles(grid(1).localNode().id(),
            SNAPSHOT_NAME,
            null,
            parts,
            stopChecker::get,
            (part, t) -> {
                try {
                    U.await(latch, TIMEOUT, TimeUnit.MILLISECONDS);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteException(e);
                }
            });

        IgniteInternalFuture<?>[] futs = new IgniteInternalFuture[1];

        assertTrue(waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                IgniteInternalFuture<?> snpFut = snp(grid(1))
                    .lastScheduledSnapshotResponseRemoteTask(grid(0).localNode().id());

                if (snpFut == null)
                    return false;
                else {
                    futs[0] = snpFut;

                    return true;
                }
            }
        }, 5_000L));

        stopChecker.set(true);
        latch.countDown();

        assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), TransmissionCancelledException.class,
            "Future cancelled prior to the all requested partitions processed");
    }

    /**
     * @param parts Expected partitions.
     * @param latch Latch to await partitions processed.
     * @return Consumer.
     */
    private static BiConsumer<File, Throwable> defaultPartitionConsumer(Map<Integer, Set<Integer>> parts, CountDownLatch latch) {
        return (part, t) -> {
            assertNull(t);

            int grpId = groupIdFromTmpDir(part.getParentFile());

            assertTrue("Received cache group has not been requested", parts.containsKey(grpId));
            assertTrue("Received partition has not been requested",
                parts.get(grpId).remove(partId(part.getName())));

            latch.countDown();
        };
    }

    /**
     * @param src Source node to calculate.
     * @param grpId Group id to collect OWNING partitions.
     * @param rmtNodeId Remote node id.
     * @return Map of collected parts.
     */
    private static Map<Integer, Set<Integer>> owningParts(IgniteEx src, int grpId, UUID rmtNodeId) {
        return Collections.singletonMap(grpId, src.context()
            .cache()
            .cacheGroup(grpId)
            .topology()
            .partitions(rmtNodeId)
            .entrySet()
            .stream()
            .filter(p -> p.getValue() == GridDhtPartitionState.OWNING)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet()));
    }
}
