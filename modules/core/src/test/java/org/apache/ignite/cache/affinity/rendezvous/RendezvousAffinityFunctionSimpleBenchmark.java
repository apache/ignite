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

package org.apache.ignite.cache.affinity.rendezvous;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintStream;
import java.io.Serializable;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Simple benchmarks, compatibility test and distribution check utils for affinity functions.
 * Needs to check changes at the {@link RendezvousAffinityFunction}.
 */
public class RendezvousAffinityFunctionSimpleBenchmark extends GridCommonAbstractTest {
    /** MAC prefix. */
    private static final String MAC_PREF = "MAC";

    /** Ignite. */
    private static Ignite ignite;

    /** Max experiments. */
    private static final int MAX_EXPERIMENTS = 200;

    /** Max experiments. */
    private TopologyModificationMode mode = TopologyModificationMode.CHANGE_LAST_NODE;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 3600 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        ignite = null;
    }

    /**
     * @param nodesCnt Count of nodes to generate.
     * @return Nodes list.
     */
    private List<ClusterNode> createBaseNodes(int nodesCnt) {
        List<ClusterNode> nodes = new ArrayList<>(nodesCnt);

        for (int i = 0; i < nodesCnt; i++) {
            GridTestNode node = new GridTestNode(UUID.randomUUID());

            // two neighbours nodes
            node.setAttribute(IgniteNodeAttributes.ATTR_MACS, MAC_PREF + i / 2);

            nodes.add(node);
        }
        return nodes;
    }

    /**
     * Modify the topology by remove the last / add new node.
     *
     * @param nodes Topology.
     * @param prevAssignment Previous afinity.
     * @param iter Number of iteration.
     * @param backups Backups count.
     * @return Affinity context.
     */
    private GridAffinityFunctionContextImpl nodesModificationChangeLast(List<ClusterNode> nodes,
        List<List<ClusterNode>> prevAssignment, int iter, int backups) {
        DiscoveryEvent discoEvt;

        discoEvt = iter % 2 == 0 ? addNode(nodes, iter) : removeNode(nodes, nodes.size() - 1);

        return new GridAffinityFunctionContextImpl(nodes,
            prevAssignment, discoEvt, new AffinityTopologyVersion(nodes.size()), backups);
    }

    /**
     * @param nodes Topology.
     * @param idx Index of node to remove.
     * @return Discovery event.
     */
    @NotNull private DiscoveryEvent removeNode(List<ClusterNode> nodes, int idx) {
        return new DiscoveryEvent(nodes.get(0), "", EventType.EVT_NODE_LEFT, nodes.remove(idx));
    }

    /**
     * Modify the topology by remove the first node / add new node
     *
     * @param nodes Topology.
     * @param prevAssignment Previous affinity.
     * @param iter Number of iteration.
     * @param backups Backups count.
     * @return Affinity context.
     */
    private GridAffinityFunctionContextImpl nodesModificationChangeFirst(List<ClusterNode> nodes,
        List<List<ClusterNode>> prevAssignment, int iter, int backups) {
        DiscoveryEvent discoEvt;

        discoEvt = iter % 2 == 0 ? addNode(nodes, iter) : removeNode(nodes, 0);

        return new GridAffinityFunctionContextImpl(nodes,
            prevAssignment, discoEvt, new AffinityTopologyVersion(nodes.size()), backups);
    }

    /**
     * @param nodes Topology.
     * @param iter Iteration count.
     * @return Discovery event.
     */
    @NotNull private DiscoveryEvent addNode(List<ClusterNode> nodes, int iter) {
        GridTestNode node = new GridTestNode(UUID.randomUUID());

        // two neighbours nodes
        node.setAttribute(IgniteNodeAttributes.ATTR_MACS, MAC_PREF + "_add_" + iter / 4);

        nodes.add(node);

        return new DiscoveryEvent(nodes.get(0), "", EventType.EVT_NODE_JOINED, node);
    }

    /**
     *
     * @param aff Affinity function.
     * @param nodes Topology.
     * @param iter Number of iteration.
     * @param prevAssignment Previous affinity assignment.
     * @param backups Backups count.
     * @return Tuple with affinity and time spend of the affinity calculation.
     */
    private IgniteBiTuple<Long, List<List<ClusterNode>>> assignPartitions(AffinityFunction aff,
        List<ClusterNode> nodes, List<List<ClusterNode>> prevAssignment, int backups, int iter) {

        GridAffinityFunctionContextImpl ctx = null;
        switch (mode) {
            case CHANGE_LAST_NODE:
                ctx = nodesModificationChangeLast(nodes, prevAssignment, iter, backups);
                break;
            case CHANGE_FIRST_NODE:
                ctx = nodesModificationChangeFirst(nodes, prevAssignment, iter, backups);
                break;

            case ADD:
                ctx = new GridAffinityFunctionContextImpl(nodes,
                    prevAssignment, addNode(nodes, iter), new AffinityTopologyVersion(nodes.size()), backups);
                break;

            case REMOVE_RANDOM:
                ctx = new GridAffinityFunctionContextImpl(nodes,
                    prevAssignment, removeNode(nodes, nodes.size() - 1),
                    new AffinityTopologyVersion(nodes.size()), backups);
                break;

            case NONE:
                ctx = new GridAffinityFunctionContextImpl(nodes,
                    prevAssignment,
                    new DiscoveryEvent(nodes.get(0), "", EventType.EVT_NODE_JOINED, nodes.get(nodes.size() - 1)),
                    new AffinityTopologyVersion(nodes.size()), backups);
                break;

        }

        long start = System.currentTimeMillis();

        List<List<ClusterNode>> assignments = aff.assignPartitions(ctx);

        return F.t(System.currentTimeMillis() - start, assignments);
    }

    /**
     * @param lst List pf measures.
     * @return Average of measures.
     */
    private double average(Collection<Long> lst) {
        if (lst.isEmpty())
            return 0;

        long sum = 0;

        for (long l : lst)
            sum += l;

        return (double)sum / lst.size();
    }

    /**
     * @param lst List pf measures.
     * @param avg Average of the measures.
     * @return Variance of the measures.
     */
    private double variance(Collection<Long> lst, double avg) {
        if (lst.isEmpty())
            return 0;

        long sum = 0;

        for (long l : lst)
            sum += (l - avg) * (l - avg);

        return Math.sqrt((double)sum / lst.size());
    }

    /**
     * The table with count of partitions on node:
     *
     * column 0 - primary partitions counts
     * column 1 - backup#0 partitions counts
     * etc
     *
     * Rows correspond to the nodes.
     *
     * @param lst Affinity result.
     * @param nodes Topology.
     * @return Frequency distribution: counts of partitions on node.
     */
    private static List<List<Integer>> freqDistribution(List<List<ClusterNode>> lst, Collection<ClusterNode> nodes) {
        List<Map<ClusterNode, AtomicInteger>> nodeMaps = new ArrayList<>();

        int backups = lst.get(0).size();

        for (int i = 0; i < backups; ++i) {
            Map<ClusterNode, AtomicInteger> map = new HashMap<>();

            for (List<ClusterNode> l : lst) {
                ClusterNode node = l.get(i);

                if (!map.containsKey(node))
                    map.put(node, new AtomicInteger(1));
                else
                    map.get(node).incrementAndGet();
            }

            nodeMaps.add(map);
        }

        List<List<Integer>> byNodes = new ArrayList<>(nodes.size());
        for (ClusterNode node : nodes) {
            List<Integer> byBackups = new ArrayList<>(backups);

            for (int j = 0; j < backups; ++j) {
                if (nodeMaps.get(j).get(node) == null)
                    byBackups.add(0);
                else
                    byBackups.add(nodeMaps.get(j).get(node).get());
            }

            byNodes.add(byBackups);
        }
        return byNodes;
    }

    /**
     * @param byNodes Frequency distribution.
     * @param suffix Label suffix.
     * @throws IOException On error.
     */
    private void printDistribution(Collection<List<Integer>> byNodes, String suffix) throws IOException {
        int nodes = byNodes.size();

        try (PrintStream ps = new PrintStream(Files.newOutputStream(FileSystems.getDefault()
            .getPath(String.format("%03d", nodes) + suffix)))) {

            for (List<Integer> byNode : byNodes) {
                for (int w : byNode)
                    ps.print(String.format("%05d ", w));

                ps.println("");
            }
        }
    }

    /**
     * Chi-square test of the distribution with uniform distribution.
     *
     * @param byNodes Distribution.
     * @param parts Partitions count.
     * @param goldenNodeWeight Weight of according the uniform distribution.
     * @return Chi-square test.
     */
    private double chiSquare(List<List<Integer>> byNodes, int parts, double goldenNodeWeight) {
        double sum = 0;

        for (List<Integer> byNode : byNodes) {
            double w = (double)byNode.get(0) / parts;

            sum += (goldenNodeWeight - w) * (goldenNodeWeight - w) / goldenNodeWeight;
        }
        return sum;
    }

    /**
     * @throws IOException On error.
     */
    @Test
    public void testDistribution() throws IOException {
        AffinityFunction aff0 = new RendezvousAffinityFunction(true, 1024);

        AffinityFunction aff1 = new RendezvousAffinityFunctionOld(true, 1024);

        GridTestUtils.setFieldValue(aff1, "ignite", ignite);

        affinityDistribution(aff0, aff1);
    }

    /**
     *
     * @param aff0 Affinity function to compare.
     * @param aff1 Affinity function to compare.
     */
    private void affinityDistribution(AffinityFunction aff0, AffinityFunction aff1) {
        int[] nodesCnts = {5, 64, 100, 128, 200, 256, 300, 400, 500, 600};

        for (int nodesCnt : nodesCnts) {
            List<ClusterNode> nodes0 = createBaseNodes(nodesCnt);
            List<ClusterNode> nodes1 = createBaseNodes(nodesCnt);

            assignPartitions(aff0, nodes0, null, 2, 0).get2();
            List<List<ClusterNode>> lst0 = assignPartitions(aff0, nodes0, null, 2, 1).get2();

            assignPartitions(aff1, nodes1, null, 2, 0).get2();
            List<List<ClusterNode>> lst1 = assignPartitions(aff1, nodes1, null, 2, 1).get2();

            List<List<Integer>> dist0 = freqDistribution(lst0, nodes0);
            List<List<Integer>> dist1 = freqDistribution(lst1, nodes1);

            info(String.format("Chi^2. Test %d nodes. %s: %f; %s: %f;",
                nodesCnt,
                aff0.getClass().getSimpleName(),
                chiSquare(dist0, aff0.partitions(), 1.0 / nodesCnt),
                aff1.getClass().getSimpleName(),
                chiSquare(dist1, aff0.partitions(), 1.0 / nodesCnt)));

            try {
                printDistribution(dist0, "." + aff0.getClass().getSimpleName());
                printDistribution(dist1, "." + aff1.getClass().getSimpleName());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     *
     */
    @Test
    public void testAffinityBenchmarkAdd() {
        mode = TopologyModificationMode.ADD;

        AffinityFunction aff0 = new RendezvousAffinityFunctionOld(true, 1024);

        GridTestUtils.setFieldValue(aff0, "ignite", ignite);

        affinityBenchmark(aff0, new RendezvousAffinityFunction(true, 1024));
    }

    /**
     *
     */
    @Test
    public void testAffinityBenchmarkChangeLast() {
        mode = TopologyModificationMode.CHANGE_LAST_NODE;

        AffinityFunction aff0 = new RendezvousAffinityFunctionOld(true, 1024);

        GridTestUtils.setFieldValue(aff0, "ignite", ignite);

        affinityBenchmark(aff0, new RendezvousAffinityFunction(true, 1024));
    }

    /**
     * @param aff0 Affinity function. to compare.
     * @param aff1 Affinity function. to compare.
     */
    private void affinityBenchmark(AffinityFunction aff0, AffinityFunction aff1) {
        int[] nodesCnts = {100, 4, 100, 200, 300, 400, 500, 600};

        final int backups = 2;

        for (int nodesCnt : nodesCnts) {
            List<ClusterNode> nodes0 = createBaseNodes(nodesCnt);
            List<ClusterNode> nodes1 = createBaseNodes(nodesCnt);

            List<Long> times0 = new ArrayList<>(MAX_EXPERIMENTS);
            List<Long> times1 = new ArrayList<>(MAX_EXPERIMENTS);

            List<List<ClusterNode>> prevAssignment =
                assignPartitions(aff0, nodes0, null, backups, 0).get2();

            for (int i = 0; i < MAX_EXPERIMENTS; ++i) {
                IgniteBiTuple<Long, List<List<ClusterNode>>> aa
                    = assignPartitions(aff0, nodes0, prevAssignment, backups, i);

                prevAssignment = aa.get2();

                times0.add(aa.get1());
            }

            prevAssignment = assignPartitions(aff1, nodes1, null, backups, 0).get2();

            for (int i = 0; i < MAX_EXPERIMENTS; ++i) {
                IgniteBiTuple<Long, List<List<ClusterNode>>> aa
                    = assignPartitions(aff1, nodes1, prevAssignment, backups, i);

                prevAssignment = aa.get2();

                times1.add(aa.get1());
            }

            double avr0 = average(times0);
            double var0 = variance(times0, avr0);

            double avr1 = average(times1);
            double var1 = variance(times1, avr1);

            info(String.format("Test %d nodes. %s: %.1f ms +/- %.3f ms; %s: %.1f ms +/- %.3f ms;",
                nodesCnt,
                aff0.getClass().getSimpleName(),
                avr0, var0,
                aff1.getClass().getSimpleName(),
                avr1, var1));
        }
    }

    /**
     *
     * @param affOld Old affinity.
     * @param affNew New affinity/
     * @return Count of partitions to migrate.
     */
    private int countPartitionsToMigrate(List<List<ClusterNode>> affOld, List<List<ClusterNode>> affNew) {
        if (affOld == null || affNew == null)
            return 0;

        assertEquals(affOld.size(), affNew.size());

        int diff = 0;
        for (int i = 0; i < affOld.size(); ++i) {
            Collection<ClusterNode> s0 = new HashSet<>(affOld.get(i));
            Iterable<ClusterNode> s1 = new HashSet<>(affNew.get(i));

            for (ClusterNode n : s1) {
                if (!s0.contains(n))
                    ++diff;
            }
        }

        return diff;
    }

    /**
     *
     */
    @Test
    public void testPartitionsMigrate() {
        int[] nodesCnts = {2, 3, 10, 64, 100, 200, 300, 400, 500, 600};

        final int backups = 2;

        AffinityFunction aff0 = new RendezvousAffinityFunction(true, 256);
        // TODO choose another affinity function to compare.
        AffinityFunction aff1 = new RendezvousAffinityFunction(true, 256);

        for (int nodesCnt : nodesCnts) {
            List<ClusterNode> nodes0 = createBaseNodes(nodesCnt);
            List<ClusterNode> nodes1 = createBaseNodes(nodesCnt);

            List<List<ClusterNode>> affPrev = null;

            int diffCnt0 = 0;

            affPrev = assignPartitions(aff0, nodes0, null, backups, 0).get2();
            for (int i = 0; i < MAX_EXPERIMENTS; ++i) {
                List<List<ClusterNode>> affCur = assignPartitions(aff0, nodes0, affPrev, backups, i).get2();
                diffCnt0 += countPartitionsToMigrate(affPrev, affCur);
                affPrev = affCur;
            }

            affPrev = assignPartitions(aff1, nodes1, null, backups, 0).get2();
            int diffCnt1 = 0;
            for (int i = 0; i < MAX_EXPERIMENTS; ++i) {
                List<List<ClusterNode>> affCur = assignPartitions(aff1, nodes1, affPrev, backups, i).get2();
                diffCnt1 += countPartitionsToMigrate(affPrev, affCur);
                affPrev = affCur;
            }

            double goldenChangeAffinity = (double)aff1.partitions() / nodesCnt * (backups + 1);
            info(String.format("Test %d nodes. Golden: %.1f; %s: %.1f; %s: %.1f;",
                nodesCnt, goldenChangeAffinity,
                aff0.getClass().getSimpleName(),
                (double)diffCnt0 / (MAX_EXPERIMENTS - 1),
                aff1.getClass().getSimpleName(),
                (double)diffCnt1 / (MAX_EXPERIMENTS - 1)));
        }
    }

    /**
     *
     */
    @Test
    public void testAffinityCompatibility() {
        AffinityFunction aff0 = new RendezvousAffinityFunction(true, 1024);

        // Use the full copy of the old implementation of the RendezvousAffinityFunction to check the compatibility.
        AffinityFunction aff1 = new RendezvousAffinityFunctionOld(true, 1024);
        GridTestUtils.setFieldValue(aff1, "ignite", ignite);

        structuralCompatibility(aff0, aff1);
    }

    /**
     * @param aff0 Affinity function to compare.
     * @param aff1 Affinity function to compare.
     */
    private void structuralCompatibility(AffinityFunction aff0, AffinityFunction aff1) {
        int[] nodesCnts = {64, 100, 200, 300, 400, 500, 600};

        final int backups = 2;

        mode = TopologyModificationMode.NONE;

        for (int nodesCnt : nodesCnts) {
            List<ClusterNode> nodes = createBaseNodes(nodesCnt);

            List<Integer> structure0 = structureOf(assignPartitions(aff0, nodes, null, backups, 0).get2());

            List<Integer> structure1 = structureOf(assignPartitions(aff1, nodes, null, backups, 0).get2());

            assertEquals(structure0, structure1);
        }
    }

    /** */
    private List<Integer> structureOf(List<List<ClusterNode>> assignment) {
        List<Integer> res = new ArrayList<>();

        for (List<ClusterNode> nodes : assignment)
            res.add(nodes != null && !nodes.contains(null) ? nodes.size() : null);

        return res;
    }

    /**
     *
     */
    private enum TopologyModificationMode {
        /** Change the last node. */
        CHANGE_LAST_NODE,

        /** Change the first node. */
        CHANGE_FIRST_NODE,

        /** Add. */
        ADD,

        /** Remove random. */
        REMOVE_RANDOM,

        /** Do nothing*/
        NONE
    }

    /**
     *  Full copy of the old implementation of the RendezvousAffinityFunction to check compatibility and performance.
     */
    private static class RendezvousAffinityFunctionOld implements AffinityFunction, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Default number of partitions. */
        public static final int DFLT_PARTITION_COUNT = 1024;

        /** Comparator. */
        private static final Comparator<IgniteBiTuple<Long, ClusterNode>> COMPARATOR = new HashComparator();

        /** Thread local message digest. */
        private ThreadLocal<MessageDigest> digest = new ThreadLocal<MessageDigest>() {
            @Override protected MessageDigest initialValue() {
                try {
                    return MessageDigest.getInstance("MD5");
                }
                catch (NoSuchAlgorithmException e) {
                    assert false : "Should have failed in constructor";

                    throw new IgniteException("Failed to obtain message digest (digest was available in constructor)", e);
                }
            }
        };

        /** Number of partitions. */
        private int parts;

        /** Exclude neighbors flag. */
        private boolean exclNeighbors;

        /** Exclude neighbors warning. */
        private transient boolean exclNeighborsWarn;

        /** Optional backup filter. First node is primary, second node is a node being tested. */
        private IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter;

        /** Optional affinity backups filter. The first node is a node being tested,
         *  the second is a list of nodes that are already assigned for a given partition (the first node in the list
         *  is primary). */
        private IgniteBiPredicate<ClusterNode, List<ClusterNode>> affinityBackupFilter;

        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger instance. */
        @LoggerResource
        private transient IgniteLogger log;

        /**
         * Empty constructor with all defaults.
         */
        public RendezvousAffinityFunctionOld() {
            this(false);
        }

        /**
         * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other
         * and specified number of backups.
         * <p>
         * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
         *
         * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
         *      of each other.
         */
        public RendezvousAffinityFunctionOld(boolean exclNeighbors) {
            this(exclNeighbors, DFLT_PARTITION_COUNT);
        }

        /**
         * Initializes affinity with flag to exclude same-host-neighbors from being backups of each other,
         * and specified number of backups and partitions.
         * <p>
         * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
         *
         * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups
         *      of each other.
         * @param parts Total number of partitions.
         */
        public RendezvousAffinityFunctionOld(boolean exclNeighbors, int parts) {
            this(exclNeighbors, parts, null);
        }

        /**
         * Initializes optional counts for replicas and backups.
         * <p>
         * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
         *
         * @param parts Total number of partitions.
         * @param backupFilter Optional back up filter for nodes. If provided, backups will be selected
         *      from all nodes that pass this filter. First argument for this filter is primary node, and second
         *      argument is node being tested.
         * <p>
         * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
         */
        public RendezvousAffinityFunctionOld(int parts, @Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
            this(false, parts, backupFilter);
        }

        /**
         * Private constructor.
         *
         * @param exclNeighbors Exclude neighbors flag.
         * @param parts Partitions count.
         * @param backupFilter Backup filter.
         */
        private RendezvousAffinityFunctionOld(boolean exclNeighbors, int parts,
            IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
            A.ensure(parts > 0, "parts > 0");

            this.exclNeighbors = exclNeighbors;
            this.parts = parts;
            this.backupFilter = backupFilter;

            try {
                MessageDigest.getInstance("MD5");
            }
            catch (NoSuchAlgorithmException e) {
                throw new IgniteException("Failed to obtain MD5 message digest instance.", e);
            }
        }

        /**
         * Gets total number of key partitions. To ensure that all partitions are
         * equally distributed across all nodes, please make sure that this
         * number is significantly larger than a number of nodes. Also, partition
         * size should be relatively small. Try to avoid having partitions with more
         * than quarter million keys.
         * <p>
         * Note that for fully replicated caches this method should always
         * return {@code 1}.
         *
         * @return Total partition count.
         */
        public int getPartitions() {
            return parts;
        }

        /**
         * Sets total number of partitions.
         *
         * @param parts Total number of partitions.
         */
        public void setPartitions(int parts) {
            A.ensure(parts <= CacheConfiguration.MAX_PARTITIONS_COUNT, "parts <= " + CacheConfiguration.MAX_PARTITIONS_COUNT);

            this.parts = parts;
        }

        /**
         * Gets optional backup filter. If not {@code null}, backups will be selected
         * from all nodes that pass this filter. First node passed to this filter is primary node,
         * and second node is a node being tested.
         * <p>
         * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
         *
         * @return Optional backup filter.
         */
        @Nullable public IgniteBiPredicate<ClusterNode, ClusterNode> getBackupFilter() {
            return backupFilter;
        }

        /**
         * Sets optional backup filter. If provided, then backups will be selected from all
         * nodes that pass this filter. First node being passed to this filter is primary node,
         * and second node is a node being tested.
         * <p>
         * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
         *
         * @param backupFilter Optional backup filter.
         * @deprecated Use {@code affinityBackupFilter} instead.
         */
        @Deprecated
        public void setBackupFilter(@Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
            this.backupFilter = backupFilter;
        }

        /**
         * Gets optional backup filter. If not {@code null}, backups will be selected
         * from all nodes that pass this filter. First node passed to this filter is a node being tested,
         * and the second parameter is a list of nodes that are already assigned for a given partition (primary node is
         * the first in the list).
         * <p>
         * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
         *
         * @return Optional backup filter.
         */
        @Nullable public IgniteBiPredicate<ClusterNode, List<ClusterNode>> getAffinityBackupFilter() {
            return affinityBackupFilter;
        }

        /**
         * Sets optional backup filter. If provided, then backups will be selected from all
         * nodes that pass this filter. First node being passed to this filter is a node being tested,
         * and the second parameter is a list of nodes that are already assigned for a given partition (primary node is
         * the first in the list).
         * <p>
         * Note that {@code affinityBackupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
         *
         * @param affinityBackupFilter Optional backup filter.
         */
        public void setAffinityBackupFilter(@Nullable IgniteBiPredicate<ClusterNode,
            List<ClusterNode>> affinityBackupFilter) {
            this.affinityBackupFilter = affinityBackupFilter;
        }

        /**
         * Checks flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
         * <p>
         * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
         *
         * @return {@code True} if nodes residing on the same host may not act as backups of each other.
         */
        public boolean isExcludeNeighbors() {
            return exclNeighbors;
        }

        /**
         * Sets flag to exclude same-host-neighbors from being backups of each other (default is {@code false}).
         * <p>
         * Note that {@code backupFilter} is ignored if {@code excludeNeighbors} is set to {@code true}.
         *
         * @param exclNeighbors {@code True} if nodes residing on the same host may not act as backups of each other.
         */
        public void setExcludeNeighbors(boolean exclNeighbors) {
            this.exclNeighbors = exclNeighbors;
        }

        /**
         * Resolves node hash.
         *
         * @param node Cluster node;
         * @return Node hash.
         */
        public Object resolveNodeHash(ClusterNode node) {
            return node.consistentId();
        }

        /**
         * Returns collection of nodes (primary first) for specified partition.
         *
         * @param d Message digest.
         * @param part Partition.
         * @param nodes Nodes.
         * @param nodesHash Serialized nodes hashes.
         * @param backups Number of backups.
         * @param neighborhoodCache Neighborhood.
         * @return Assignment.
         */
        public List<ClusterNode> assignPartition(MessageDigest d,
            int part,
            List<ClusterNode> nodes,
            Map<ClusterNode, byte[]> nodesHash,
            int backups,
            @Nullable Map<UUID, Collection<ClusterNode>> neighborhoodCache) {
            if (nodes.size() <= 1)
                return nodes;

            if (d == null)
                d = digest.get();

            List<IgniteBiTuple<Long, ClusterNode>> lst = new ArrayList<>(nodes.size());

            try {
                for (int i = 0; i < nodes.size(); i++) {
                    ClusterNode node = nodes.get(i);

                    byte[] nodeHashBytes = nodesHash.get(node);

                    if (nodeHashBytes == null) {
                        Object nodeHash = resolveNodeHash(node);

                        byte[] nodeHashBytes0 = U.marshal(ignite.configuration().getMarshaller(), nodeHash);

                        // Add 4 bytes for partition bytes.
                        nodeHashBytes = new byte[nodeHashBytes0.length + 4];

                        System.arraycopy(nodeHashBytes0, 0, nodeHashBytes, 4, nodeHashBytes0.length);

                        nodesHash.put(node, nodeHashBytes);
                    }

                    U.intToBytes(part, nodeHashBytes, 0);

                    d.reset();

                    byte[] bytes = d.digest(nodeHashBytes);

                    long hash =
                        (bytes[0] & 0xFFL)
                            | ((bytes[1] & 0xFFL) << 8)
                            | ((bytes[2] & 0xFFL) << 16)
                            | ((bytes[3] & 0xFFL) << 24)
                            | ((bytes[4] & 0xFFL) << 32)
                            | ((bytes[5] & 0xFFL) << 40)
                            | ((bytes[6] & 0xFFL) << 48)
                            | ((bytes[7] & 0xFFL) << 56);

                    lst.add(F.t(hash, node));
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            Collections.sort(lst, COMPARATOR);

            int primaryAndBackups = backups == Integer.MAX_VALUE ? nodes.size() : Math.min(backups + 1, nodes.size());

            List<ClusterNode> res = new ArrayList<>(primaryAndBackups);

            ClusterNode primary = lst.get(0).get2();

            res.add(primary);

            // Select backups.
            if (backups > 0) {
                for (int i = 1; i < lst.size() && res.size() < primaryAndBackups; i++) {
                    IgniteBiTuple<Long, ClusterNode> next = lst.get(i);

                    ClusterNode node = next.get2();

                    if (exclNeighbors) {
                        Collection<ClusterNode> allNeighbors = GridCacheUtils.neighborsForNodes(neighborhoodCache, res);

                        if (!allNeighbors.contains(node))
                            res.add(node);
                    }
                    else if (affinityBackupFilter != null && affinityBackupFilter.apply(node, res))
                        res.add(next.get2());
                    else if (backupFilter != null && backupFilter.apply(primary, node))
                        res.add(next.get2());
                    else if (affinityBackupFilter == null && backupFilter == null)
                        res.add(next.get2());
                }
            }

            if (res.size() < primaryAndBackups && nodes.size() >= primaryAndBackups && exclNeighbors) {
                // Need to iterate again in case if there are no nodes which pass exclude neighbors backups criteria.
                for (int i = 1; i < lst.size() && res.size() < primaryAndBackups; i++) {
                    IgniteBiTuple<Long, ClusterNode> next = lst.get(i);

                    ClusterNode node = next.get2();

                    if (!res.contains(node))
                        res.add(next.get2());
                }

                if (!exclNeighborsWarn) {
                    LT.warn(log, "Affinity function excludeNeighbors property is ignored " +
                        "because topology has no enough nodes to assign backups.");

                    exclNeighborsWarn = true;
                }
            }

            assert res.size() <= primaryAndBackups;

            return res;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return parts;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            if (key == null)
                throw new IllegalArgumentException("Null key is passed for a partition calculation. " +
                    "Make sure that an affinity key that is used is initialized properly.");

            return U.safeAbs(key.hashCode() % parts);
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<List<ClusterNode>> assignments = new ArrayList<>(parts);

            Map<UUID, Collection<ClusterNode>> neighborhoodCache = exclNeighbors ?
                GridCacheUtils.neighbors(affCtx.currentTopologySnapshot()) : null;

            MessageDigest d = digest.get();

            List<ClusterNode> nodes = affCtx.currentTopologySnapshot();

            Map<ClusterNode, byte[]> nodesHash = U.newHashMap(nodes.size());

            for (int i = 0; i < parts; i++) {
                List<ClusterNode> partAssignment = assignPartition(d,
                    i,
                    nodes,
                    nodesHash,
                    affCtx.backups(),
                    neighborhoodCache);

                assignments.add(partAssignment);
            }

            return assignments;
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(parts);
            out.writeBoolean(exclNeighbors);
            out.writeObject(backupFilter);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            parts = in.readInt();
            exclNeighbors = in.readBoolean();
            backupFilter = (IgniteBiPredicate<ClusterNode, ClusterNode>)in.readObject();
        }

        /**
         *
         */
        private static class HashComparator implements Comparator<IgniteBiTuple<Long, ClusterNode>>, Serializable {
            /** */
            private static final long serialVersionUID = 0L;

            /** {@inheritDoc} */
            @Override public int compare(IgniteBiTuple<Long, ClusterNode> o1, IgniteBiTuple<Long, ClusterNode> o2) {
                return o1.get1() < o2.get1() ? -1 : o1.get1() > o2.get1() ? 1 :
                    o1.get2().id().compareTo(o2.get2().id());
            }
        }
    }
}
