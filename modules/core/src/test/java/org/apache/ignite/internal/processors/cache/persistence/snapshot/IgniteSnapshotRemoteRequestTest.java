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
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** */
public class IgniteSnapshotRemoteRequestTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRemoteRequest() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        Map<Integer, Set<Integer>> parts = owningParts(ignite,
            new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))),
            grid(1).localNode().id());

        awaitPartitionMapExchange();
        IgniteSnapshotManager mgr = snp(ignite);

        IgniteInternalFuture<?> fut = mgr.requestRemoteSnapshot(grid(1).localNode().id(),
            SNAPSHOT_NAME,
            parts,
            new BiConsumer<File, GroupPartitionId>() {
                @Override public void accept(File file, GroupPartitionId id) {
                    System.out.println(">>>>> " + file.getName() + ", pair=" + id);
                }
            });

        fut.get();
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRemoteWithNodeFiler() throws Exception {
        int grids = 3;
        CacheConfiguration<Integer, Integer> ccfg = txCacheConfig(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME))
            .setNodeFilter(node -> node.consistentId().toString().endsWith("1"));

        for (int i = 0; i < grids; i++)
            startGrid(optimize(getConfiguration(getTestIgniteInstanceName(i)).setCacheConfiguration()));

        IgniteEx ig0 = grid(0);
        ig0.cluster().baselineAutoAdjustEnabled(false);
        ig0.cluster().state(ACTIVE);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.getOrCreateCache(ccfg).put(i, i);

        Map<Integer, Set<Integer>> parts = new HashMap<>();
        parts.put(CU.cacheId(DEFAULT_CACHE_NAME), null);

        IgniteInternalFuture<?> fut = snp(grid(0)).requestRemoteSnapshot(grid(2).localNode().id(),
            SNAPSHOT_NAME,
            parts,
            new BiConsumer<File, GroupPartitionId>() {
                @Override public void accept(File file, GroupPartitionId gprPartId) {
                    fail("We should not receive anything.");
                }
            });

        assertThrowsAnyCause(log,
            fut::get,
            IgniteCheckedException.class,
            "The snapshot operation stopped");
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRemoteOnBothNodes() throws Exception {
        IgniteEx ig0 = startGrids(2);

        ig0.cluster().state(ClusterState.ACTIVE);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.cache(DEFAULT_CACHE_NAME).put(i, i);

        forceCheckpoint(ig0);

        IgniteSnapshotManager mgr0 = snp(ig0);
        IgniteSnapshotManager mgr1 = snp(grid(1));

        UUID node0 = grid(0).localNode().id();
        UUID node1 = grid(1).localNode().id();

        Map<Integer, Set<Integer>> fromNode1 = owningParts(ig0,
            new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))),
            node1);

        Map<Integer, Set<Integer>> fromNode0 = owningParts(grid(1),
            new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))),
            node0);

        // Snapshot must be taken on node1 and transmitted to node0.
        IgniteInternalFuture<?> futFrom1To0 = mgr0.requestRemoteSnapshot(node1, SNAPSHOT_NAME, fromNode1,
            (part, pair) -> assertTrue("Received partition has not been requested", fromNode1.get(pair.getGroupId())
                .remove(pair.getPartitionId())));
        IgniteInternalFuture<?> futFrom0To1 = mgr1.requestRemoteSnapshot(node0, SNAPSHOT_NAME, fromNode0,
            (part, pair) -> assertTrue("Received partition has not been requested", fromNode0.get(pair.getGroupId())
                .remove(pair.getPartitionId())));

        futFrom0To1.get();
        futFrom1To0.get();

        assertTrue("Not all of partitions have been received: " + fromNode1,
            fromNode1.get(CU.cacheId(DEFAULT_CACHE_NAME)).isEmpty());
        assertTrue("Not all of partitions have been received: " + fromNode0,
            fromNode0.get(CU.cacheId(DEFAULT_CACHE_NAME)).isEmpty());
    }

    /** @throws Exception If fails. */
    @Test(expected = ClusterTopologyCheckedException.class)
    public void testRemoteSnapshotRequestedNodeLeft() throws Exception {
        IgniteEx ig0 = startGridWithCache(dfltCacheCfg, CACHE_KEYS_RANGE);
        IgniteEx ig1 = startGrid(1);

        ig0.cluster().setBaselineTopology(ig0.cluster().forServers().nodes());

        awaitPartitionMapExchange();

        CountDownLatch hold = new CountDownLatch(1);

        ((GridCacheDatabaseSharedManager)ig1.context().cache().context().database())
            .addCheckpointListener(new CheckpointListener() {
                /** {@inheritDoc} */
                @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
                    // Listener will be executed inside the checkpoint thead.
                    U.await(hold);
                }

                /** {@inheritDoc} */
                @Override public void onMarkCheckpointBegin(Context ctx) {
                    // No-op.
                }

                /** {@inheritDoc} */
                @Override public void onCheckpointBegin(Context ctx) {
                    // No-op.
                }
            });

        UUID rmtNodeId = ig1.localNode().id();

        Map<Integer, Set<Integer>> parts = new HashMap<>();
        parts.put(CU.cacheId(DEFAULT_CACHE_NAME), null);

        snp(ig0).requestRemoteSnapshot(rmtNodeId, SNAPSHOT_NAME, parts, (part, grp) -> {});

        IgniteInternalFuture<?>[] futs = new IgniteInternalFuture[1];

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                IgniteInternalFuture<?> snpFut = snp(ig1)
                    .lastScheduledSnapshotResponseRemoteTask(ig0.localNode().id());

                if (snpFut == null)
                    return false;
                else
                    futs[0] = snpFut;

                return true;
            }
        }, 5_000L));

        stopGrid(0);

        hold.countDown();

        futs[0].get();
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
