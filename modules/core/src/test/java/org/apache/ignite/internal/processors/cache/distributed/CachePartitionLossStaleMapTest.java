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
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.EvictionContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.TestDependencyResolver;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.PartitionLossPolicy.IGNORE;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Checks that the coordinator's partition map agrees with the nodes after a node leaves with the only copy of its
 * partitions.
 * <p>
 * Each lost partition is recreated empty on its new primary as MOVING. When the node detects the loss, it owns the
 * partition under the {@link PartitionLossPolicy#IGNORE IGNORE} policy or marks it LOST under the safe policies. If the
 * new primary sends its partition map between these two steps, the map carries MOVING and overwrites what the
 * coordinator already knows, so the node must send its map again after the change.
 * <p>
 * The window spans most of the exchange on the new primary: it opens when the node applies the coordinator's full
 * message and creates the partition, and closes when the node detects lost partitions at the end of the exchange. In a
 * real cluster a map gets into it when a resend scheduled by {@code scheduleResendPartitions()} (after an eviction, a
 * partition moving to RENTING or a partition map change) fires there.
 */
public class CachePartitionLossStaleMapTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "cache";

    /** */
    private static final int PARTS = 64;

    /** Index of the node that becomes the new primary of the lost partitions. */
    private static final int NEW_PRIMARY = 1;

    /** Index of the node that leaves. */
    private static final int LEAVING = 2;

    /**
     * When set, the new primary sends its partition map right before it owns a MOVING partition of the cache or marks
     * it LOST.
     */
    private final AtomicBoolean sendBeforeChange = new AtomicBoolean();

    /** When set, the next eviction of a partition of the cache waits for {@link #evictionResume}. */
    private final AtomicBoolean holdEviction = new AtomicBoolean();

    /** Counted down when an eviction starts waiting. */
    private final CountDownLatch evictionHeld = new CountDownLatch(1);

    /** Lets the held eviction go on. */
    private final CountDownLatch evictionResume = new CountDownLatch(1);

    /** When set, the coordinator waits for {@link #crdResume} right after it sends the next exchange full message. */
    private final AtomicBoolean stallAfterFullMessage = new AtomicBoolean();

    /** Counted down when the coordinator starts waiting. */
    private final CountDownLatch crdStalled = new CountDownLatch(1);

    /** Lets the coordinator go on. */
    private final CountDownLatch crdResume = new CountDownLatch(1);

    /** Partition loss policy of the cache. */
    private PartitionLossPolicy plc = IGNORE;

    /** Whether the cache uses {@link JoinOrderAffinityFunction}. */
    private boolean joinOrderAff;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new StallingCommunicationSpi())
            // Other evictions don't queue behind a held one.
            .setRebalanceThreadPoolSize(4)
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(CACHE)
                .setBackups(0)
                .setPartitionLossPolicy(plc)
                .setAffinity(joinOrderAff
                    ? new JoinOrderAffinityFunction()
                    : new RendezvousAffinityFunction(false, PARTS)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        evictionResume.countDown();
        crdResume.countDown();

        stopAllGrids();

        super.afterTest();
    }

    /**
     * The new primary sends a stale map before it owns a lost partition under the IGNORE policy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStaleMapBeforeLostPartitionOwned() throws Exception {
        checkStaleMapBeforeLostPartitionChanged(IGNORE);
    }

    /**
     * The new primary sends a stale map before it marks a lost partition LOST under the READ_WRITE_SAFE policy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStaleMapBeforeLostPartitionMarkedLost() throws Exception {
        checkStaleMapBeforeLostPartitionChanged(READ_WRITE_SAFE);
    }

    /**
     * A node joins and takes partitions over, the old owners evict their copies, and the node leaves. Its partitions
     * are lost, and the new primary sends a stale map, with a lost partition still MOVING, before it changes them.
     *
     * @param plc Partition loss policy.
     * @throws Exception If failed.
     */
    private void checkStaleMapBeforeLostPartitionChanged(PartitionLossPolicy plc) throws Exception {
        this.plc = plc;

        IgniteEx crd = startGrid(0);

        startGrid(NEW_PRIMARY, partitionHooksResolver());

        IgniteCache<Integer, Integer> cache = crd.cache(CACHE);

        for (int i = 0; i < PARTS * 4; i++)
            cache.put(i, i);

        startGrid(LEAVING);

        awaitPartitionMapExchange(true, true, null);

        UUID crdId = crd.localNode().id();

        TestRecordingCommunicationSpi newPrimarySpi = TestRecordingCommunicationSpi.spi(grid(NEW_PRIMARY));

        // Hold the new primary's maps sent outside an exchange until the coordinator finishes the exchange.
        newPrimarySpi.blockMessages((node, msg) -> node.id().equals(crdId)
            && msg instanceof GridDhtPartitionsSingleMessage
            && ((GridDhtPartitionsSingleMessage)msg).exchangeId() == null);

        sendBeforeChange.set(true);

        long leaveVer = crd.cluster().topologyVersion() + 1;

        stopGrid(LEAVING);

        newPrimarySpi.waitForBlocked();

        assertFalse("The new primary didn't change a lost partition", sendBeforeChange.get());

        assertTrue(waitForCondition(() -> exchangeFinished(crd, leaveVer), getTestTimeout()));

        newPrimarySpi.stopBlock();

        if (!waitForCondition(() -> mismatches(crd).isEmpty(), 10_000))
            fail("The coordinator's partition map differs from the nodes: " + mismatches(crd));

        if (plc != IGNORE)
            crd.resetLostPartitions(Collections.singleton(CACHE));

        awaitPartitionMapExchange();
    }

    /**
     * The coordinator itself is the new primary of all lost partitions. It creates them before it sends the full
     * message, so a plain resend carries the same map version and the other node ignores it. Here the coordinator
     * stalls after it sends the full message, as when sending to the next node is slow, and an eviction finishes there
     * meanwhile. The resend scheduled after the eviction carries a newer map with the lost partitions still MOVING,
     * and the other node, which has already finished the exchange, takes it. The coordinator must send its map again
     * once it owns the partitions.
     * <p>
     * The other node gets no lost partitions: otherwise its own resend after detecting them makes the coordinator
     * resend its map later, which hides the problem.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStaleCoordinatorMapAfterEviction() throws Exception {
        joinOrderAff = true;

        IgniteEx crd = startGrid(0, partitionHooksResolver());

        IgniteCache<Integer, Integer> cache = crd.cache(CACHE);

        for (int i = 0; i < PARTS * 4; i++)
            cache.put(i, i);

        // The next eviction on the coordinator waits until the exchange, as the eviction of a large partition would.
        holdEviction.set(true);

        IgniteEx other = startGrid(1);

        awaitPartitionMapExchange();

        assertTrue("The coordinator didn't start an eviction", evictionHeld.await(getTestTimeout(), MILLISECONDS));

        startGrid(LEAVING);

        awaitPartitionMapExchange();

        GridDhtPartitionTopology crdTop = crd.cachex(CACHE).context().topology();

        // Only the held eviction is left, so the resend is scheduled when it finishes.
        assertTrue(waitForCondition(() -> crdTop.localPartitions().stream()
            .filter(p -> p.state() == RENTING).count() == 1, getTestTimeout()));

        UUID crdId = crd.localNode().id();

        stallAfterFullMessage.set(true);

        long leaveVer = crd.cluster().topologyVersion() + 1;

        stopGrid(LEAVING);

        assertTrue("The coordinator didn't send the full message", crdStalled.await(getTestTimeout(), MILLISECONDS));

        assertTrue(waitForCondition(() -> exchangeFinished(other, leaveVer), getTestTimeout()));

        evictionResume.countDown();

        GridDhtPartitionTopology otherTop = other.cachex(CACHE).context().topology();

        assertTrue("The other node didn't take a stale map of the coordinator", waitForCondition(() -> {
            GridDhtPartitionMap crdMap = otherTop.partitionMap(false).get(crdId);

            return crdMap != null && crdMap.hasMovingPartitions();
        }, 10_000));

        crdResume.countDown();

        assertTrue(waitForCondition(() -> exchangeFinished(crd, leaveVer), getTestTimeout()));

        if (!waitForCondition(() -> mismatches(other).isEmpty(), 10_000))
            fail("The other node's partition map differs from the nodes: " + mismatches(other));

        awaitPartitionMapExchange();
    }

    /**
     * @return Resolver whose partitions send the partition map right before a MOVING partition of the cache is owned or
     * marked LOST, once {@link #sendBeforeChange} is set, and hold the next eviction of a partition of the cache, once
     * {@link #holdEviction} is set.
     */
    private TestDependencyResolver partitionHooksResolver() {
        return new TestDependencyResolver(new DependencyResolver() {
            @Override public <T> T resolve(T instance) {
                if (instance instanceof GridDhtPartitionTopologyImpl) {
                    GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl)instance;

                    top.partitionFactory((ctx, grp, id, recovery) -> new GridDhtLocalPartition(ctx, grp, id, recovery) {
                        @Override public boolean own() {
                            sendIfArmed();

                            return super.own();
                        }

                        @Override public boolean markLost() {
                            sendIfArmed();

                            return super.markLost();
                        }

                        /** {@inheritDoc} */
                        @Override protected long clearAll(EvictionContext evictionCtx) throws NodeStoppingException {
                            if (CACHE.equals(grp.cacheOrGroupName()) && state() == RENTING
                                && holdEviction.compareAndSet(true, false)) {
                                evictionHeld.countDown();

                                U.awaitQuiet(evictionResume);
                            }

                            return super.clearAll(evictionCtx);
                        }

                        /** Sends the partition map once, if this is a MOVING partition of the cache. */
                        private void sendIfArmed() {
                            if (CACHE.equals(grp.cacheOrGroupName()) && state() == MOVING
                                && sendBeforeChange.compareAndSet(true, false))
                                ctx.exchange().refreshPartitions(Collections.singleton(grp));
                        }
                    });
                }

                return instance;
            }
        });
    }

    /**
     * @param node Node.
     * @param topVer Topology version.
     * @return Whether the node has finished the exchange for the topology version.
     */
    private boolean exchangeFinished(IgniteEx node, long topVer) {
        GridDhtPartitionsExchangeFuture fut = node.context().cache().context().exchange().lastFinishedFuture();

        return fut != null && fut.topologyVersion().topologyVersion() >= topVer;
    }

    /**
     * @param viewer Node whose partition map is checked.
     * @return Partitions whose state in the viewer's map differs from the state on the node itself.
     */
    private List<String> mismatches(IgniteEx viewer) {
        GridDhtPartitionTopology viewerTop = viewer.cachex(CACHE).context().topology();

        List<String> res = new ArrayList<>();

        for (Ignite g : G.allGrids()) {
            UUID id = g.cluster().localNode().id();

            GridDhtPartitionMap view = viewerTop.partitionMap(false).get(id);

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

    /** Holds the coordinator right after it sends the full message of an exchange, once armed. */
    private class StallingCommunicationSpi extends TestRecordingCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            super.sendMessage(node, msg, ackC);

            if (msg instanceof GridIoMessage
                && ((GridIoMessage)msg).message() instanceof GridDhtPartitionsFullMessage
                && ((GridDhtPartitionsFullMessage)((GridIoMessage)msg).message()).exchangeId() != null
                && stallAfterFullMessage.compareAndSet(true, false)) {
                crdStalled.countDown();

                U.awaitQuiet(crdResume);
            }
        }
    }

    /**
     * Assigns partition {@code p} to the server node with index {@code p % 3} in join order, or to the oldest node if
     * there is no such node. The third node takes the partitions with {@code p % 3 == 2} from the oldest node and gives
     * them back when it leaves.
     */
    private static class JoinOrderAffinityFunction implements AffinityFunction {
        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return PARTS;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return U.safeAbs(key.hashCode()) % PARTS;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext ctx) {
            List<ClusterNode> nodes = new ArrayList<>(ctx.currentTopologySnapshot());

            nodes.sort(Comparator.comparingLong(ClusterNode::order));

            List<List<ClusterNode>> res = new ArrayList<>(PARTS);

            for (int p = 0; p < PARTS; p++)
                res.add(Collections.singletonList(nodes.get(p % 3 < nodes.size() ? p % 3 : 0)));

            return res;
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }
    }
}
