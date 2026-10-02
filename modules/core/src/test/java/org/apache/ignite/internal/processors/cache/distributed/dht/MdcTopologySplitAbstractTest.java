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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.MdcAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.topology.MdcTopologyValidator;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_CENTER_ID;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Base for tests that cut the network between data centers (DCs) of one cluster.
 *
 * <p>The cluster has {@link #serversPerDc()} servers and one client in each DC of {@link #dataCenters()}.
 * Servers {@code 0 .. dcs * serversPerDc - 1} go DC by DC; the client of DC {@code i} has index
 * {@code dcs * serversPerDc + i}. A client knows only the servers of its own DC, so it stays in its DC's segment
 * of a split.</p>
 *
 * <p>{@link #split(String...)} cuts the given DCs off from the rest, and {@link #splitInto(List)} splits the DCs into
 * any number of segments: discovery connections between segments fail, and communication messages between them are
 * held. Either then waits, with a timeout, until every node sees exactly its own segment. {@link #heal(String...)}
 * drops the held messages (delivering them would replay one segment's view on another) and restarts the given DCs,
 * which then join the cluster again. Subclasses can hold more messages while the cluster is split through
 * {@link #blockMessage(ClusterNode, ClusterNode, Message)}.</p>
 *
 * <p>Unlike {@link IgniteCacheTopologySplitAbstractTest#splitAndWait()}, nothing here waits for one exact topology
 * version without a timeout, so a split that ends in an unexpected topology fails the test instead of hanging it.</p>
 */
public abstract class MdcTopologySplitAbstractTest extends IgniteCacheTopologySplitAbstractTest {
    /** */
    protected static final String DC1 = "DC1";

    /** */
    protected static final String DC2 = "DC2";

    /** */
    protected static final String DC3 = "DC3";

    /** Time for the segments of a split to see only themselves, and for a healed cluster to become whole. */
    protected static final long TOPOLOGY_TIMEOUT = 30_000;

    /** */
    private static final String LOCAL_IP = "127.0.0.1";

    /** Message of the exception a write gets when the topology validator rejects it. */
    private static final String VALIDATOR_REJECTION = "cache topology is not valid";

    /**
     * Segment number of each DC in the current split, from 0: DCs with the same number are in one segment. Empty
     * when the cluster is whole.
     */
    private volatile Map<String, Integer> segmentNumByDc = Collections.emptyMap();

    /** @return DCs of the cluster, in the order their servers are numbered. */
    protected abstract List<String> dataCenters();

    /** @return Number of servers in each DC. */
    protected int serversPerDc() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int idx = getTestIgniteInstanceIndex(igniteInstanceName);

        String dc = dataCenterOf(idx);

        cfg.setUserAttributes(F.asMap(IGNITE_DATA_CENTER_ID, dc));

        TcpDiscoverySpi disco = new MdcSplitDiscoverySpi(dc);

        disco.setReconnectCount(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getReconnectCount());

        if (isServer(idx)) {
            disco.setLocalPort(discoveryPort(idx));
            disco.setLocalPortRange(0);
            disco.setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(serverAddresses(dataCenters())));
        }
        else
            disco.setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(serverAddresses(Collections.singleton(dc))));

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        segmentNumByDc = Collections.emptyMap();

        stopAllGrids();

        super.afterTest();
    }

    /**
     * Starts every server, then a client in each DC.
     *
     * @throws Exception If failed.
     */
    protected void startCluster() throws Exception {
        startGridsMultiThreaded(serverCount());

        for (String dc : dataCenters())
            startClientGrid(clientIndex(dc));

        awaitPartitionMapExchange();
    }

    /**
     * Cuts the given DCs off from the rest of the cluster and waits until each segment sees only itself.
     *
     * @param dcs DCs to cut off.
     */
    protected void split(String... dcs) throws Exception {
        List<String> cut = Arrays.asList(dcs);

        List<String> rest = new ArrayList<>(dataCenters());

        rest.removeAll(cut);

        splitInto(Arrays.asList(rest, cut));
    }

    /**
     * Splits the DCs into the given segments and waits until each segment sees only itself.
     *
     * @param segments Segments of the split: two at least, together holding every DC exactly once.
     */
    protected void splitInto(List<List<String>> segments) throws Exception {
        assertTrue("Cluster is split already: " + segmentNumByDc, segmentNumByDc.isEmpty());
        assertTrue("A split needs two segments at least: " + segments, segments.size() >= 2);

        Map<String, Integer> split = new HashMap<>();

        for (int seg = 0; seg < segments.size(); seg++) {
            assertFalse("Every segment must have a DC: " + segments, segments.get(seg).isEmpty());

            for (String dc : segments.get(seg)) {
                assertTrue("Unknown DC: " + dc, dataCenters().contains(dc));
                assertNull("DC is listed twice: " + dc, split.put(dc, seg));
            }
        }

        assertEquals("Every DC must be in a segment: " + segments, dataCenters().size(), split.size());

        log.info(">>> Splitting DCs into " + segments);

        Map<String, Integer> segmentMap = Collections.unmodifiableMap(split);

        // Hold communication first, then cut discovery: no message crosses the split once any node sees it.
        for (Ignite ignite : G.allGrids())
            communication(ignite).blockMessages(new MdcSplitBlocker(ignite.cluster().localNode(), segmentMap));

        segmentNumByDc = segmentMap;

        long start = System.nanoTime();

        awaitSegmentsSeeThemselves();

        log.info(">>> Split done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " ms");
    }

    /**
     * Restarts every node of the given DCs and waits until the cluster is whole again. Every held message is
     * dropped and every message filter is removed, including those added by {@link #blockMessage}. The given DCs
     * must be whole segments of the split, every segment but one, normally the segments that lost writes: they rejoin
     * the remaining segment and rebalance from it. The segments form separate rings, which never merge on their own.
     *
     * @param restartDcs DCs to restart.
     */
    protected void heal(String... restartDcs) throws Exception {
        Map<String, Integer> split = segmentNumByDc;

        assertFalse("Cluster is not split", split.isEmpty());

        Set<String> restartSet = new HashSet<>(Arrays.asList(restartDcs));

        Set<Integer> kept = new HashSet<>();

        for (Map.Entry<String, Integer> e : split.entrySet()) {
            if (!restartSet.contains(e.getKey()))
                kept.add(e.getValue());
        }

        boolean wholeSegments = split.keySet().containsAll(restartSet)
            && restartSet.stream().noneMatch(dc -> kept.contains(split.get(dc)));

        assertTrue("DCs to restart must be whole segments of the split " + split + ", every segment but one: " +
            restartSet, wholeSegments && kept.size() == 1);

        log.info(">>> Healing the split, restarting DCs " + Arrays.toString(restartDcs));

        List<Integer> restart = new ArrayList<>();

        for (String dc : dataCenters()) {
            if (!restartSet.contains(dc))
                continue;

            restart.addAll(serverIndexes(dc));
            restart.add(clientIndex(dc));
        }

        for (int idx : restart)
            stopGrid(idx, true);

        segmentNumByDc = Collections.emptyMap();

        for (Ignite ignite : G.allGrids())
            communication(ignite).stopBlock(false);

        for (int idx : restart) {
            if (isServer(idx))
                startGrid(idx);
        }

        for (int idx : restart) {
            if (!isServer(idx))
                startClientGrid(idx);
        }

        int total = serverCount() + dataCenters().size();

        assertTrue("Cluster did not become whole: " + topologyViews(),
            GridTestUtils.waitForCondition(() -> G.allGrids().stream()
                .allMatch(ignite -> ignite.cluster().nodes().size() == total), TOPOLOGY_TIMEOUT));

        awaitPartitionMapExchange();

        log.info(">>> Heal done");
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @param validator Topology validator, or {@code null} for none.
     * @return Cache configuration that keeps one copy of each partition in every DC.
     */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration(
        String name,
        CacheAtomicityMode atomicityMode,
        @Nullable MdcTopologyValidator validator
    ) {
        int dcs = dataCenters().size();

        int backups = dcs - 1;

        return new CacheConfiguration<Integer, Integer>(name)
            .setAtomicityMode(atomicityMode)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setBackups(backups)
            .setAffinity(new RendezvousAffinityFunction(false, 32)
                .setAffinityBackupFilter(new MdcAffinityBackupFilter(dcs, backups)))
            .setTopologyValidator(validator);
    }

    /**
     * @param dcs Every DC of the cluster, an odd number of them.
     * @return Validator that lets a segment write while it sees a majority of the DCs.
     */
    protected static MdcTopologyValidator majorityValidator(String... dcs) {
        // The validator accepts an even set too, and then an even split leaves no segment that writes.
        assertTrue("Majority validation needs an odd number of DCs, use mainDcValidator() for an even one: " +
            Arrays.toString(dcs), dcs.length % 2 == 1);

        MdcTopologyValidator validator = new MdcTopologyValidator();

        validator.setDatacenters(new HashSet<>(Arrays.asList(dcs)));

        return validator;
    }

    /**
     * @param mainDc Main DC.
     * @return Validator that lets a segment write while it sees the main DC.
     */
    protected static MdcTopologyValidator mainDcValidator(String mainDc) {
        MdcTopologyValidator validator = new MdcTopologyValidator();

        validator.setMainDatacenter(mainDc);

        return validator;
    }

    /**
     * Checks that a write is rejected by the topology validator. The rejection surfaces as
     * {@link CacheInvalidStateException} for atomic caches and explicit transactions, and wrapped into a
     * {@code TransactionRollbackException} for implicit transactional writes, so the cause chain is searched.
     * Other reasons for the same exception (lost partitions, inactive or read-only cluster) don't count.
     *
     * @param write Write that must be rejected.
     */
    protected static void assertWriteRejected(Runnable write) {
        Throwable err = GridTestUtils.assertThrowsWithCause(write, Exception.class);

        assertTrue("Unexpected rejection: " + X.getFullStackTrace(err),
            X.hasCause(err, VALIDATOR_REJECTION, CacheInvalidStateException.class));
    }

    /**
     * Checks that every DC keeps a copy of every key with the expected value.
     *
     * @param cacheName Cache name.
     * @param expected Expected cache content.
     */
    protected void assertDataInEveryDc(String cacheName, Map<Integer, Integer> expected) {
        assertEquals(expected.size(), client(dataCenters().get(0)).cache(cacheName).size(CachePeekMode.PRIMARY));

        for (String dc : dataCenters()) {
            Map<Integer, Integer> dcData = new HashMap<>();

            for (int idx : serverIndexes(dc)) {
                IgniteCache<Integer, Integer> cache = grid(idx).cache(cacheName);

                for (Integer key : expected.keySet()) {
                    Integer val = cache.localPeek(key, CachePeekMode.PRIMARY, CachePeekMode.BACKUP);

                    if (val != null)
                        assertNull("Key has two copies in " + dc + ": " + key, dcData.put(key, val));
                }
            }

            assertEquals("Data in " + dc, expected, dcData);
        }
    }

    /** @return Number of servers. */
    protected int serverCount() {
        return dataCenters().size() * serversPerDc();
    }

    /**
     * @param dc DC.
     * @return Indexes of the DC's servers.
     */
    protected List<Integer> serverIndexes(String dc) {
        int first = dataCenters().indexOf(dc) * serversPerDc();

        assertTrue("Unknown DC: " + dc, first >= 0);

        List<Integer> idxs = new ArrayList<>(serversPerDc());

        for (int i = 0; i < serversPerDc(); i++)
            idxs.add(first + i);

        return idxs;
    }

    /**
     * @param dc DC.
     * @return Index of the DC's client.
     */
    protected int clientIndex(String dc) {
        int dcIdx = dataCenters().indexOf(dc);

        assertTrue("Unknown DC: " + dc, dcIdx >= 0);

        return serverCount() + dcIdx;
    }

    /**
     * @param dc DC.
     * @return The DC's client.
     */
    protected IgniteEx client(String dc) {
        return grid(clientIndex(dc));
    }

    /**
     * Lets a subclass hold more messages while the cluster is split, in addition to those crossing the split.
     * Held messages are dropped by {@link #heal(String...)}.
     *
     * @param locNode Sending node.
     * @param rmtNode Receiving node.
     * @param msg Message.
     * @return {@code True} to hold the message.
     */
    protected boolean blockMessage(ClusterNode locNode, ClusterNode rmtNode, Message msg) {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean segmented() {
        return !segmentNumByDc.isEmpty();
    }

    /** {@inheritDoc} */
    @Override protected boolean isBlocked(int locPort, int rmtPort) {
        // Not used: the discovery SPI here decides by the local node's DC, since a client has no port of its own.
        throw new UnsupportedOperationException();
    }

    /**
     * @param dc1 DC.
     * @param dc2 DC.
     * @return {@code True} if the DCs are in different segments of the current split.
     */
    private boolean dcsAreInDifferentSegments(String dc1, String dc2) {
        Map<String, Integer> split = segmentNumByDc;

        return !split.isEmpty() && !split.get(dc1).equals(split.get(dc2));
    }

    /** {@inheritDoc} */
    @Override protected int segment(ClusterNode node) {
        // Segment of the node in the current split, or 0 when the cluster is whole. There can be more than the
        // template's two segments: its splitAndWait(), which expects two, is not used.
        return segmentNumByDc.getOrDefault(node.dataCenterId(), 0);
    }

    /** Waits until every node sees exactly the nodes of its own segment, and its last exchange is done. */
    private void awaitSegmentsSeeThemselves() throws Exception {
        Map<Integer, Set<UUID>> segments = new HashMap<>();

        for (Ignite ignite : G.allGrids()) {
            ClusterNode node = ignite.cluster().localNode();

            segments.computeIfAbsent(segment(node), s -> new HashSet<>()).add(node.id());
        }

        boolean done = GridTestUtils.waitForCondition(() -> G.allGrids().stream().allMatch(ignite -> {
            Set<UUID> seen = ignite.cluster().nodes().stream().map(ClusterNode::id).collect(Collectors.toSet());

            if (!seen.equals(segments.get(segment(ignite.cluster().localNode()))))
                return false;

            GridDhtPartitionsExchangeFuture exchFut =
                ((IgniteEx)ignite).context().cache().context().exchange().lastTopologyFuture();

            return exchFut != null && exchFut.isDone()
                && exchFut.topologyVersion().topologyVersion() == ignite.cluster().topologyVersion();
        }), TOPOLOGY_TIMEOUT);

        assertTrue("Segments of the split did not separate within " + TOPOLOGY_TIMEOUT + " ms: " + topologyViews(),
            done);
    }

    /** @return What each node sees, for failure messages. */
    private String topologyViews() {
        StringBuilder sb = new StringBuilder();

        for (Ignite ignite : G.allGrids()) {
            sb.append("\n  ").append(ignite.name())
                .append(" [dc=").append(ignite.cluster().localNode().dataCenterId())
                .append(", topVer=").append(ignite.cluster().topologyVersion())
                .append(", sees=").append(ignite.cluster().nodes().stream()
                    .map(n -> n.dataCenterId() + (n.isClient() ? "c" : "s"))
                    .sorted()
                    .collect(Collectors.joining(" ")))
                .append(']');
        }

        return sb.toString();
    }

    /**
     * @param idx Node index.
     * @return {@code True} if the index belongs to a server.
     */
    private boolean isServer(int idx) {
        return idx < serverCount();
    }

    /**
     * @param idx Node index.
     * @return DC of the node.
     */
    private String dataCenterOf(int idx) {
        int dcIdx = isServer(idx) ? idx / serversPerDc() : idx - serverCount();

        assertTrue("No DC for node " + idx, dcIdx < dataCenters().size());

        return dataCenters().get(dcIdx);
    }

    /**
     * @param idx Server index.
     * @return Discovery port of the server.
     */
    private static int discoveryPort(int idx) {
        return TcpDiscoverySpi.DFLT_PORT + idx;
    }

    /**
     * @param port Port.
     * @return DC of the server listening on the discovery port, or {@code null} if it is not a server's port.
     */
    @Nullable private String dataCenterOfPort(int port) {
        int idx = port - TcpDiscoverySpi.DFLT_PORT;

        return idx >= 0 && isServer(idx) ? dataCenterOf(idx) : null;
    }

    /**
     * @param dcs DCs.
     * @return Discovery addresses of the DCs' servers.
     */
    private Collection<String> serverAddresses(Collection<String> dcs) {
        List<String> addrs = new ArrayList<>();

        for (String dc : dcs) {
            for (int idx : serverIndexes(dc))
                addrs.add(LOCAL_IP + ':' + discoveryPort(idx));
        }

        return addrs;
    }

    /**
     * @param ignite Node.
     * @return Its communication SPI.
     */
    private static TestRecordingCommunicationSpi communication(Ignite ignite) {
        return (TestRecordingCommunicationSpi)ignite.configuration().getCommunicationSpi();
    }

    /** Holds communication messages between the segments of a split, and those {@link #blockMessage} picks. */
    private class MdcSplitBlocker implements IgniteBiPredicate<ClusterNode, Message> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Local node. */
        private final ClusterNode locNode;

        /** Segment number of each DC in the split. */
        private final Map<String, Integer> segmentNumByDc;

        /**
         * @param locNode Local node.
         * @param segmentNumByDc Segment number of each DC in the split.
         */
        MdcSplitBlocker(ClusterNode locNode, Map<String, Integer> segmentNumByDc) {
            this.locNode = locNode;
            this.segmentNumByDc = segmentNumByDc;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node, Message msg) {
            return !segmentNumByDc.get(locNode.dataCenterId()).equals(segmentNumByDc.get(node.dataCenterId()))
                || blockMessage(locNode, node, msg);
        }
    }

    /**
     * Discovery SPI that fails connections from its node to servers in another segment of the current split.
     * The segment is taken from the node's DC, not from its local port: a client has no port of its own.
     */
    private class MdcSplitDiscoverySpi extends SplitTcpDiscoverySpi {
        /** DC of the local node. */
        private final String dc;

        /** @param dc DC of the local node. */
        MdcSplitDiscoverySpi(String dc) {
            this.dc = dc;
        }

        /** {@inheritDoc} */
        @Override protected boolean segmented(InetSocketAddress sockAddr) {
            String rmtDc = dataCenterOfPort(sockAddr.getPort());

            return rmtDc != null && dcsAreInDifferentSegments(dc, rmtDc);
        }
    }
}
