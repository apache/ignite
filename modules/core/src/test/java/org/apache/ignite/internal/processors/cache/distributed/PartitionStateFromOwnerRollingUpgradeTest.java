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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.rollingupgrade.AbstractRollingUpgradeTest;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

import static org.apache.ignite.cache.PartitionLossPolicy.IGNORE;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Checks that until {@code PART_STATE_FROM_OWNER_FEATURE} is active the other nodes write the new owners' state into
 * their partition maps themselves, as the previous version does. Once the feature is active, they take it from the
 * owners' maps.
 * <p>
 * In both parts a server leaves with the only copies of some partitions under the IGNORE policy, while the maps that
 * the other servers send outside an exchange are held, so the coordinator sees only the state it wrote itself. The
 * coordinator and the client watch the new owners' partitions. The client hasn't started the cache, so it keeps the
 * cache's partition map in a client topology.
 */
public class PartitionStateFromOwnerRollingUpgradeTest extends AbstractRollingUpgradeTest {
    /** */
    private static final String CACHE = "cache";

    /** */
    private static final int PARTS = 64;

    /** Version without {@code PART_STATE_FROM_OWNER_FEATURE}. */
    private static final String OLD_VER = "2.18.0";

    /** Index of the client node, as started by {@link #startCluster(String)}. */
    private static final int CLIENT = 2;

    /** Index of the server that leaves. */
    private static final int LEAVING = 3;

    /** Time to wait for an exchange, in milliseconds. */
    private static final long WAIT = 30_000;

    /**
     * Before the upgrade the coordinator and the client see the new owners right after the exchange, with the owners'
     * maps held. Once all nodes are upgraded and the version is finalized, the feature is active, and they see the new
     * owners only after the maps arrive.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOtherNodesStopWritingAfterUpgrade() throws Exception {
        startCluster(OLD_VER);
        startGrid(LEAVING, OLD_VER);

        grid(0).createCache(new CacheConfiguration<Integer, Integer>(CACHE)
            .setBackups(0)
            .setPartitionLossPolicy(IGNORE)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS)));

        awaitPartitionMapExchange();

        IgniteEx oldCrd = stopLeavingWithMapsHeld();

        for (IgniteEx viewer : new IgniteEx[] {oldCrd, grid(CLIENT)}) {
            List<String> mismatches = partitionMapMismatches(viewer, CACHE);

            assertTrue(viewer.name() + " didn't write the new owners' state: " + mismatches, mismatches.isEmpty());
        }

        TestRecordingCommunicationSpi.stopBlockAll();

        ru(0).enableVersionUpgrade();

        upgradeNodeVersion(1, OLD_VER, MASTER_VER_REF);
        upgradeNodeVersion(CLIENT, OLD_VER, MASTER_VER_REF);
        upgradeNodeVersion(0, OLD_VER, MASTER_VER_REF);

        awaitPartitionMapExchange();

        // Also checks that every node has the features of the new version active.
        finalizeClusterVersion(1, MASTER_VER_REF);

        startGrid(LEAVING, MASTER_VER_REF);

        // A map resent after a late eviction would be held with the others and show a RENTING copy.
        awaitPartitionMapExchange(true, true, null);

        IgniteEx crd = stopLeavingWithMapsHeld();

        for (IgniteEx viewer : new IgniteEx[] {crd, grid(CLIENT)}) {
            List<String> mismatches = partitionMapMismatches(viewer, CACHE);

            assertFalse(viewer.name() + " wrote the new owners' state itself", mismatches.isEmpty());

            assertTrue(viewer.name() + " differs from the nodes in more than the new owners' partitions: " + mismatches,
                mismatches.stream().allMatch(m -> m.contains(" local=OWNING ")));
        }

        TestRecordingCommunicationSpi.stopBlockAll();

        awaitPartitionMapsMatchLocalStates(CACHE);
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
            assertTrue(g.name() + " didn't finish the exchange",
                waitForCondition(() -> exchangeFinished((IgniteEx)g, leaveVer), WAIT));
        }

        return crd;
    }

    /**
     * @param node Node.
     * @param topVer Topology version.
     * @return Whether the node has finished the exchange for the topology version.
     */
    private static boolean exchangeFinished(IgniteEx node, long topVer) {
        GridDhtPartitionsExchangeFuture fut = node.context().cache().context().exchange().lastFinishedFuture();

        return fut != null && fut.topologyVersion().topologyVersion() >= topVer;
    }
}
