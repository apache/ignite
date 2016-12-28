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

import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link RendezvousAffinityFunction}.
 */
public class RendezvousAffinityFunctionSimpleBenchmark extends GridCommonAbstractTest {
    /** MAC prefix. */
    private static final String MAC_PREF = "MAC";

    /** Max funcs. */
    private static final int MAX_FUNCS = 10;

    /** Compatibility version. */
    private static final IgniteProductVersion compatibilityVersion
        = new IgniteProductVersion((byte)1, (byte)6, (byte)0, 0L, null);

    /** Ignite. */
    private static Ignite ignite;

    /** Max experiments. */
    private static int MAX_EXPERIMENTS = 30;

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
        stopAllGrids();
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
     * @param iter Number of iteration.
     * @param newVer Flag to setup latest version at the context.
     * @param backups Backups count.
     * @return Affinity context.
     */
    GridAffinityFunctionContextImpl nodesModificationRemoveLast(List<ClusterNode> nodes, int iter, int backups,
        boolean newVer) {
        DiscoveryEvent discoEvt;

        if (iter % 2 == 0) {
            // Add new node.
            GridTestNode node = new GridTestNode(UUID.randomUUID());

            // two neighbours nodes
            node.setAttribute(IgniteNodeAttributes.ATTR_MACS, MAC_PREF + "_add_" + iter / 4);

            nodes.add(node);

            discoEvt = new DiscoveryEvent(nodes.get(0), "", EventType.EVT_NODE_JOINED, node);
        }
        else {
            // Remove last node.
            discoEvt = new DiscoveryEvent(nodes.get(0), "", EventType.EVT_NODE_LEFT, nodes.remove(nodes.size() - 1));
        }

        return new GridAffinityFunctionContextImpl(nodes,
            null, discoEvt, new AffinityTopologyVersion(nodes.size()), backups,
            (newVer) ? null : compatibilityVersion);
    }

    /**
     * Modify the topology by remove the first node / add new node
     *
     * @param nodes Topology.
     * @param iter Number of iteration.
     * @param newVersion Flag to setup latest version at the context.
     * @param backups Backups count.
     * @return Affinity context.
     */
    GridAffinityFunctionContextImpl nodesModificationRemoveFirst(List<ClusterNode> nodes, int iter, int backups,
        boolean newVersion) {
        DiscoveryEvent discoEvt;

        if (iter % 2 == 0) {
            // Add new node.
            GridTestNode node = new GridTestNode(UUID.randomUUID());

            // two neighbours nodes
            node.setAttribute(IgniteNodeAttributes.ATTR_MACS, MAC_PREF + "_add_" + iter / 4);

            nodes.add(node);

            discoEvt = new DiscoveryEvent(nodes.get(0), "", EventType.EVT_NODE_JOINED, node);
        }
        else {
            // Remove first node.
            discoEvt = new DiscoveryEvent(nodes.get(0), "", EventType.EVT_NODE_LEFT, nodes.remove(0));
        }

        return new GridAffinityFunctionContextImpl(nodes,
            null, discoEvt, new AffinityTopologyVersion(nodes.size()), backups,
            (newVersion) ? null : compatibilityVersion);
    }

    /**
     *
     * @param aff Affinity function.
     * @param nodes Topology.
     * @param iter Number of iteration.
     * @param backups Backups count.
     * @return Tuple with affinity and time spend of the affinity calculation.
     */
    private IgniteBiTuple<Long, List<List<ClusterNode>>> assignPartitions(AffinityFunction aff,
        List<ClusterNode> nodes, int backups, int iter) {
        GridAffinityFunctionContextImpl ctx = nodesModificationRemoveFirst(nodes, iter, backups, true);

        long start = System.currentTimeMillis();

        List<List<ClusterNode>> assignments = aff.assignPartitions(ctx);

        return F.t(System.currentTimeMillis() - start, assignments);
    }

    /**
     * @param lst List pf measures.
     * @return Average of measures.
     */
    private double average(List<Long> lst) {
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
    private double variance(List<Long> lst, double avg) {
        if (lst.size() == 0)
            return 0;

        long sum = 0;

        for (long l : lst)
            sum += (l - avg) * (l - avg);

        return Math.sqrt((double)sum / lst.size());
    }

    /**
     *
     * @param lst Affinity result.
     * @param nodes Topology.
     * @return Frequency distribution of nodes by partition.
     */
    private static List<List<Integer>> freqDistribution(List<List<ClusterNode>> lst, List<ClusterNode> nodes) {
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
     * @param goldenFreq Golden frequency (according with uniform distribution).
     * @throws IOException On error.
     */
    void printDistribution(List<List<Integer>> byNodes, String suffix, double goldenFreq) throws IOException {
        int nodes = byNodes.size();

        try (PrintStream ps = new PrintStream(Files.newOutputStream(FileSystems.getDefault()
            .getPath(String.format("%03d", nodes) + suffix)))) {

            for (int i = 0; i < byNodes.size(); ++i) {
                for (int w : byNodes.get(i))
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

        for (int i = 0; i < byNodes.size(); ++i) {
            double w = (double)byNodes.get(i).get(0) / parts;

            sum += (goldenNodeWeight - w) * (goldenNodeWeight - w) / goldenNodeWeight;
        }
        return sum;
    }

    /**
     * @throws IOException On error.
     */
    public void testDistribution() throws IOException {
        AffinityFunction aff = new RendezvousAffinityFunction(true, 1024);

        GridTestUtils.setFieldValue(aff, "ignite", ignite);

        affinityDistribution(new FairAffinityFunction(true, 256), aff);
    }

    /**
     *
     * @param aff0 Affinity function to compare.
     * @param aff1 Affinity function to compare.
     */
    private void affinityDistribution(AffinityFunction aff0, AffinityFunction aff1) {
        int[] nodesCnts = {3, 64, 100, 128, 200, 256, 300, 400, 500, 600};

        for (int nodesCnt : nodesCnts) {
            List<ClusterNode> nodes_old = createBaseNodes(nodesCnt);
            List<ClusterNode> nodes_new = createBaseNodes(nodesCnt);

            assignPartitions(aff0, nodes_old, 2, 0).get2();
            List<List<ClusterNode>> lst_old = assignPartitions(aff0, nodes_old, 2, 1).get2();

            assignPartitions(aff1, nodes_new, 2, 0).get2();
            List<List<ClusterNode>> lst_new = assignPartitions(aff1, nodes_new, 2, 1).get2();

            List<List<Integer>> oldDist = freqDistribution(lst_old, nodes_old);
            List<List<Integer>> newDist = freqDistribution(lst_new, nodes_new);

            info(String.format("Chi^2. Test %d nodes. %s: %f; %s: %f;",
                nodesCnt,
                aff0.getClass().getSimpleName(),
                chiSquare(oldDist, aff0.partitions(), 1.0 / nodesCnt),
                aff1.getClass().getSimpleName(),
                chiSquare(newDist, aff0.partitions(), 1.0 / nodesCnt)));

//            printDistribution(oldDist, ".old", (double)affOld.partitions() / nodesCnts[nodesCntIdx]);
//            printDistribution(newDist, ".new", (double)affNew.partitions() / nodesCnts[nodesCntIdx]);
        }
    }

    /**
     *
     */
    public void testAffinityBenchmark() {
        affinityBenchmark(new Factory<AffinityFunction>() {
            @Override public AffinityFunction create() {
                AffinityFunction aff = new RendezvousAffinityFunctionOld(true);

                GridTestUtils.setFieldValue(aff, "ignite", ignite);

                return aff;
            }
        }, new Factory<AffinityFunction>() {
            @Override public AffinityFunction create() {
                RendezvousAffinityFunction aff = new RendezvousAffinityFunction(true);

                GridTestUtils.setFieldValue(aff, "ignite", ignite);

                return aff;
            }
        });
    }

    /**
     * @param affFactoryOld Factory to create affinity function.
     * @param affFactoryNew Factory to create affinity function.
     */
    private void affinityBenchmark(Factory<AffinityFunction> affFactoryOld, Factory<AffinityFunction> affFactoryNew) {
        int[] nodesCnts = {100, 63, 100, 200, 300, 400, 500, 600};

        List<AffinityFunction> funcListOld = new ArrayList<>();
        List<AffinityFunction> funcListNew = new ArrayList<>();

        for (int i = 0; i < MAX_FUNCS; ++i) {
            funcListOld.add(affFactoryOld.create());
            funcListNew.add(affFactoryNew.create());
        }

        final int backups = 2;

        for (int nodesCnt : nodesCnts) {
            List<ClusterNode> nodes_old = createBaseNodes(nodesCnt);
            List<ClusterNode> nodes_new = createBaseNodes(nodesCnt);

            List<Long> times_old = new ArrayList<>(MAX_EXPERIMENTS);
            List<Long> times_new = new ArrayList<>(MAX_EXPERIMENTS);

            for (int i = 0; i < MAX_EXPERIMENTS; ++i) {
                for (AffinityFunction aff : funcListOld)
                    times_old.add(assignPartitions(aff, nodes_old, backups, i).get1());
            }

            for (int i = 0; i < MAX_EXPERIMENTS; ++i) {
                for (AffinityFunction aff : funcListNew)
                    times_new.add(assignPartitions(aff, nodes_new, backups, i).get1());
            }

            double avr_old = average(times_old);
            double var_old = variance(times_old, avr_old);

            double avr_new = average(times_new);
            double var_new = variance(times_new, avr_new);

            info(String.format("Test %d nodes. %s: %.1f ms +/- %.3f ms; %s: %.1f ms +/- %.3f ms;",
                nodesCnt,
                affFactoryOld.create().getClass().getSimpleName(),
                avr_old, var_old,
                affFactoryNew.create().getClass().getSimpleName(),
                avr_new, var_new));
        }
    }

    /**
     *
     */
    private int countNodesToExchange(List<List<ClusterNode>> affOld, List<List<ClusterNode>> affNew) {
        if (affOld == null || affNew == null)
            return 0;

        assertEquals(affOld.size(), affNew.size());

        int diff = 0;
        for (int i = 0; i < affOld.size(); ++i) {
            Set<ClusterNode> s0 = new HashSet<>(affOld.get(i));
            Set<ClusterNode> s1 = new HashSet<>(affNew.get(i));
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
    public void testChangeAffinity() {
        int[] nodesCnts = {62, 66, 100, 200, 300, 400, 500, 600};

        final int backups = 2;

        AffinityFunction funcMd5 = new RendezvousAffinityFunction(true, 256);
        AffinityFunction funcWang = new FairAffinityFunction(true, 256);

        for (int nodesCnt : nodesCnts) {
            List<ClusterNode> nodes_old = createBaseNodes(nodesCnt);
            List<ClusterNode> nodes_new = createBaseNodes(nodesCnt);

            List<List<ClusterNode>> affPrev = null;

            int diffCntOld = 0;

            for (int i = 0; i < MAX_EXPERIMENTS; ++i) {
                List<List<ClusterNode>> affCur = assignPartitions(funcMd5, nodes_old, backups, i).get2();
                diffCntOld += countNodesToExchange(affPrev, affCur);
                affPrev = affCur;
            }

            affPrev = null;
            int diffCntNew = 0;
            for (int i = 0; i < MAX_EXPERIMENTS; ++i) {
                List<List<ClusterNode>> affCur = assignPartitions(funcWang, nodes_new, backups, i).get2();
                diffCntNew += countNodesToExchange(affPrev, affCur);
                affPrev = affCur;
            }

            double goldenChangeAffinity = (double)funcWang.partitions() / nodesCnt * (backups + 1);
            info(String.format("Test %d nodes. Golden: %.1f; Old: %.1f; New: %.1f;",
                nodesCnt, goldenChangeAffinity,
                (double)diffCntOld / (MAX_EXPERIMENTS - 1), (double)diffCntNew / (MAX_EXPERIMENTS - 1)));
        }
    }
}
