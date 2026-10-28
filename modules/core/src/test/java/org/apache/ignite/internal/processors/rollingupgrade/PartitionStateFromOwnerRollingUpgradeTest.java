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

package org.apache.ignite.internal.processors.rollingupgrade;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.junit.Test;

import static org.apache.ignite.cache.PartitionLossPolicy.IGNORE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.rollingupgrade.feature.SupportedFeatureRegistry.PART_STATE_FROM_OWNER_FEATURE;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Checks that the other nodes write the state of the new owners of lost partitions into their partition maps
 * themselves only until {@code PART_STATE_FROM_OWNER_FEATURE} is active. An older node doesn't send its map after it
 * owns a lost partition, so until every node runs the new version the others can't wait for that map.
 * <p>
 * In both parts a server leaves with the only copies of some partitions under the IGNORE policy, while the maps that
 * the other servers send outside an exchange are held. The servers send their maps as before, so holding the maps
 * stands for an older node that doesn't send them. The coordinator and the client watch the new owners' partitions.
 * The client hasn't started the cache, so it keeps the cache's partition map in a client topology.
 */
public class PartitionStateFromOwnerRollingUpgradeTest extends AbstractRollingUpgradeTest {
    /** */
    private static final String CACHE = "cache";

    /** */
    private static final int PARTS = 64;

    /** Version whose features include {@code PART_STATE_FROM_OWNER_FEATURE}, by its id. */
    private static final String NEW_VER = "2.19.2";

    /** Index of the client node, as started by {@link #startCluster()}. */
    private static final int CLIENT = 2;

    /** Index of the server that leaves. */
    private static final int LEAVING = 3;

    /**
     * Before the upgrade the coordinator and the client see the new owners right after the exchange, with the owners'
     * maps held. Once all nodes are upgraded and the version is finalized, the feature is active, and they see the new
     * owners only after the maps arrive.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOwnersReportStateAfterUpgrade() throws Exception {
        startCluster();
        startGrid(LEAVING, TEST_DEFAULT_VER);

        grid(0).createCache(new CacheConfiguration<Integer, Integer>(CACHE)
            .setBackups(0)
            .setPartitionLossPolicy(IGNORE)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS)));

        awaitPartitionMapExchange();

        assertFalse(isFeatureActive(grid(0), PART_STATE_FROM_OWNER_FEATURE));

        IgniteEx oldCrd = stopLeavingWithMapsHeld();

        for (IgniteEx viewer : new IgniteEx[] {oldCrd, grid(CLIENT)}) {
            assertTrue(viewer.name() + " didn't write the new owners' state: " + mismatches(viewer),
                mismatches(viewer).isEmpty());
        }

        TestRecordingCommunicationSpi.stopBlockAll();

        ru(0).enableVersionUpgrade();

        upgradeNodeVersion(1, NEW_VER);
        upgradeNodeVersion(CLIENT, NEW_VER);
        upgradeNodeVersion(0, NEW_VER);

        // Also checks that every node has the features of the new version active.
        finalizeClusterVersion(1, NEW_VER);

        startGrid(LEAVING, NEW_VER);

        awaitPartitionMapExchange();

        IgniteEx crd = stopLeavingWithMapsHeld();

        for (IgniteEx viewer : new IgniteEx[] {crd, grid(CLIENT)})
            assertFalse(viewer.name() + " wrote the new owners' state itself", mismatches(viewer).isEmpty());

        TestRecordingCommunicationSpi.stopBlockAll();

        for (IgniteEx viewer : new IgniteEx[] {crd, grid(CLIENT)}) {
            if (!waitForCondition(() -> mismatches(viewer).isEmpty(), 10_000))
                fail("The partition map of " + viewer.name() + " differs from the nodes: " + mismatches(viewer));
        }
    }

    /**
     * Stops the leaving server while the other servers' partition maps sent outside an exchange are held, and waits
     * for every node to finish the exchange.
     *
     * @return Coordinator.
     * @throws Exception If failed.
     */
    private IgniteEx stopLeavingWithMapsHeld() throws Exception {
        IgniteEx crd = grid(coordinatorIndex());

        UUID crdId = crd.localNode().id();

        for (Ignite g : G.allGrids()) {
            if (g != crd && !g.cluster().localNode().isClient()) {
                TestRecordingCommunicationSpi.spi(g).blockMessages((node, msg) -> node.id().equals(crdId)
                    && msg instanceof GridDhtPartitionsSingleMessage
                    && ((GridDhtPartitionsSingleMessage)msg).exchangeId() == null);
            }
        }

        long leaveVer = crd.cluster().topologyVersion() + 1;

        stopGrid(LEAVING);

        for (Ignite g : G.allGrids()) {
            assertTrue(waitForCondition(() -> {
                GridDhtPartitionsExchangeFuture fut = ((IgniteEx)g).context().cache().context().exchange()
                    .lastFinishedFuture();

                return fut != null && fut.topologyVersion().topologyVersion() >= leaveVer;
            }, getTestTimeout()));
        }

        return crd;
    }

    /**
     * @param viewer Server with the cache started, or the client, which keeps the cache in a client topology.
     * @return Partitions whose state in the viewer's map differs from the state on the server itself.
     */
    private List<String> mismatches(IgniteEx viewer) {
        GridDhtPartitionTopology viewerTop = viewer.cluster().localNode().isClient()
            ? viewer.context().cache().context().exchange().clientTopologyIfExists(CU.cacheId(CACHE))
            : viewer.cachex(CACHE).context().topology();

        assertNotNull(viewer.name(), viewerTop);

        List<String> res = new ArrayList<>();

        for (Ignite g : G.allGrids()) {
            if (g.cluster().localNode().isClient())
                continue;

            GridDhtPartitionMap view = viewerTop.partitionMap(false).get(g.cluster().localNode().id());

            GridDhtPartitionTopology top = ((IgniteEx)g).cachex(CACHE).context().topology();

            for (int p = 0; p < PARTS; p++) {
                GridDhtLocalPartition locPart = top.localPartition(p, AffinityTopologyVersion.NONE, false, true);

                GridDhtPartitionState loc = locPart == null ? null : locPart.state();
                GridDhtPartitionState seen = view == null ? null : view.get(p);

                // The map keeps evicted partitions, the node doesn't.
                if (seen == EVICTED)
                    seen = null;

                if (loc != seen)
                    res.add(g.name() + " p=" + p + " local=" + loc + " seen=" + seen);
            }
        }

        return res;
    }
}
