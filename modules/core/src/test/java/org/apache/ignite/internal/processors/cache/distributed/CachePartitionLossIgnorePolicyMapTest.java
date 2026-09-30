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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.util.lang.ClusterNodeFunc;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.TestDependencyResolver;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.PartitionLossPolicy.IGNORE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Checks that the coordinator's partition map agrees with the nodes after a node leaves with the only copy of its
 * partitions under the {@link org.apache.ignite.cache.PartitionLossPolicy#IGNORE IGNORE} loss policy.
 * <p>
 * Each lost partition is recreated empty on its new primary as MOVING and owned there when the node detects the loss.
 * If the new primary sends its partition map between these two steps, the map carries MOVING and overwrites what the
 * coordinator already knows, so the node must send its map again once it owns the partition. Otherwise the other nodes
 * get the stale state from the coordinator and see no owner of the partition.
 */
public class CachePartitionLossIgnorePolicyMapTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "cache";

    /** */
    private static final int PARTS = 64;

    /** Index of the node that becomes the new primary of the lost partitions. */
    private static final int NEW_PRIMARY = 1;

    /** Index of the node that leaves. */
    private static final int LEAVING = 2;

    /** Index of the node that checks who owns the lost partitions: neither the coordinator nor their new primary. */
    private static final int OBSERVER = 3;

    /** When set, the new primary sends its partition map right before it owns a MOVING partition of the cache. */
    private final AtomicBoolean sendBeforeOwn = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(CACHE)
                .setBackups(0)
                .setPartitionLossPolicy(IGNORE)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * A node joins and takes partitions over, the old owners evict their copies, and the node leaves. Its partitions
     * are lost, and the new primary sends a stale map, with the lost partition still MOVING, before it owns them.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStaleMapBeforeLostPartitionOwned() throws Exception {
        IgniteEx crd = startGrid(0);

        startGrid(NEW_PRIMARY, sendBeforeOwnResolver());

        IgniteEx observer = startGrid(OBSERVER);

        IgniteCache<Integer, Integer> cache = crd.cache(CACHE);

        for (int i = 0; i < PARTS * 4; i++)
            cache.put(i, i);

        startGrid(LEAVING);

        awaitPartitionMapExchange(true, true, null);

        int[] lostParts = crd.affinity(CACHE).primaryPartitions(grid(LEAVING).localNode());

        UUID crdId = crd.localNode().id();

        TestRecordingCommunicationSpi newPrimarySpi = TestRecordingCommunicationSpi.spi(grid(NEW_PRIMARY));

        // Hold the new primary's maps sent outside an exchange until the coordinator finishes the exchange.
        newPrimarySpi.blockMessages((node, msg) -> node.id().equals(crdId)
            && msg instanceof GridDhtPartitionsSingleMessage
            && ((GridDhtPartitionsSingleMessage)msg).exchangeId() == null);

        sendBeforeOwn.set(true);

        long leaveVer = crd.cluster().topologyVersion() + 1;

        stopGrid(LEAVING);

        newPrimarySpi.waitForBlocked();

        assertFalse("The new primary didn't own a lost partition", sendBeforeOwn.get());

        assertTrue(waitForCondition(() -> {
            GridDhtPartitionsExchangeFuture fut = crd.context().cache().context().exchange().lastFinishedFuture();

            return fut != null && fut.topologyVersion().topologyVersion() >= leaveVer;
        }, getTestTimeout()));

        newPrimarySpi.stopBlock();

        boolean agreed = waitForCondition(
            () -> mismatches(crd).isEmpty() && ownerMismatches(observer, lostParts).isEmpty(), 10_000);

        if (!agreed) {
            fail("The coordinator's partition map differs from the nodes: " + mismatches(crd) +
                ", the observer sees wrong owners of the lost partitions: " + ownerMismatches(observer, lostParts));
        }

        awaitPartitionMapExchange();
    }

    /**
     * @param observer Node that is neither the coordinator nor the new primary of the lost partitions it checks.
     * @param lostParts Lost partitions.
     * @return Lost partitions whose owners, as the observer sees them, are not just their primary.
     */
    private List<String> ownerMismatches(IgniteEx observer, int[] lostParts) {
        GridDhtPartitionTopology top = observer.cachex(CACHE).context().topology();

        List<String> res = new ArrayList<>();

        for (int p : lostParts) {
            ClusterNode primary = observer.affinity(CACHE).mapPartitionToNode(p);

            if (primary.isLocal())
                continue;

            List<ClusterNode> owners = top.owners(p);

            if (!owners.equals(Collections.singletonList(primary)))
                res.add("p=" + p + " owners=" + ClusterNodeFunc.nodeIds(owners) + " primary=" + primary.id());
        }

        return res;
    }

    /**
     * @return Resolver that makes the new primary send its partition map right before it owns a MOVING partition of
     * the cache, once {@link #sendBeforeOwn} is set.
     */
    private TestDependencyResolver sendBeforeOwnResolver() {
        return new TestDependencyResolver(new DependencyResolver() {
            @Override public <T> T resolve(T instance) {
                if (instance instanceof GridDhtPartitionTopologyImpl) {
                    GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)instance;

                    top.partitionFactory((ctx, grp, id, recovery) -> new GridDhtLocalPartition(ctx, grp, id, recovery) {
                        @Override public boolean own() {
                            if (CACHE.equals(grp.cacheOrGroupName()) && state() == MOVING
                                && sendBeforeOwn.compareAndSet(true, false))
                                ctx.exchange().refreshPartitions(Collections.singleton(grp));

                            return super.own();
                        }
                    });
                }

                return instance;
            }
        });
    }

    /**
     * @param crd Coordinator.
     * @return Partitions whose state in the coordinator's map differs from the state on the node itself.
     */
    private List<String> mismatches(IgniteEx crd) {
        GridDhtPartitionTopology crdTop = crd.cachex(CACHE).context().topology();

        List<String> res = new ArrayList<>();

        for (Ignite g : G.allGrids()) {
            UUID id = g.cluster().localNode().id();

            GridDhtPartitionMap crdView = crdTop.partitionMap(false).get(id);

            GridDhtPartitionTopology top = ((IgniteEx)g).cachex(CACHE).context().topology();

            for (int p = 0; p < PARTS; p++) {
                GridDhtLocalPartition locPart = top.localPartition(p, AffinityTopologyVersion.NONE, false, true);

                GridDhtPartitionState loc = locPart == null ? null : locPart.state();
                GridDhtPartitionState seen = crdView == null ? null : crdView.get(p);

                // The coordinator keeps evicted partitions in its map, the node doesn't.
                if (seen == EVICTED)
                    seen = null;

                if (loc != seen)
                    res.add(g.name() + " p=" + p + " local=" + loc + " crd=" + seen);
            }
        }

        return res;
    }
}
