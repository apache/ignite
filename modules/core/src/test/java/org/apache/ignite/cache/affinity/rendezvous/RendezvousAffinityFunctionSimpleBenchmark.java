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

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link RendezvousAffinityFunction}.
 */
public class RendezvousAffinityFunctionSimpleBenchmark extends GridCommonAbstractTest {
    /** MAC prefix. */
    private static final String MAC_PREF = "MAC";
    private static final int MAX_FUNCS = 10;
    /** Ignite. */
    private static Ignite ignite;
    private static int MAX_EXPERIMENTS = 30;

    protected long getTestTimeout() {
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

    List<ClusterNode> createBaseNodes(int nodesCnt) {
        List<ClusterNode> nodes = new ArrayList<>(nodesCnt);

        for (int i = 0; i < nodesCnt; i++) {

            GridTestNode node = new GridTestNode(UUID.randomUUID());

            // two neighbours nodes
            node.setAttribute(IgniteNodeAttributes.ATTR_MACS, MAC_PREF + i / 2);

            nodes.add(node);
        }
        return nodes;
    }

    GridAffinityFunctionContextImpl nodesModification(List<ClusterNode> nodes, int iter, int backups) {
        DiscoveryEvent discoEvt;

        if (iter % 2 == 0) {
            // Add new node.
            ClusterNode node = new GridTestNode(UUID.randomUUID());
            node.attribute(MAC_PREF + iter);
            nodes.add(node);

            discoEvt = new DiscoveryEvent(nodes.get(0), "", EventType.EVT_NODE_JOINED, node);
        }
        else {
            // Remove last node.
            discoEvt = new DiscoveryEvent(nodes.get(0), "", EventType.EVT_NODE_LEFT, nodes.remove(nodes.size() - 1));
        }

        return new GridAffinityFunctionContextImpl(nodes,
            null, discoEvt, new AffinityTopologyVersion(nodes.size()), backups);
    }

    IgniteBiTuple<Long, List<List<ClusterNode>>> assignPartitions_old(RendezvousAffinityFunction aff,
        List<ClusterNode> nodes, int backups, int iter) {
        GridAffinityFunctionContextImpl ctx = nodesModification(nodes, iter, backups);

        long start = System.currentTimeMillis();
        List<List<ClusterNode>> assignments = aff.assignPartitions_old(ctx);
        return F.t(System.currentTimeMillis() - start, assignments);
    }

    IgniteBiTuple<Long, List<List<ClusterNode>>> assignPartitions_new(RendezvousAffinityFunction aff,
        List<ClusterNode> nodes, int backups, int iter) {
        GridAffinityFunctionContextImpl ctx = nodesModification(nodes, iter, backups);

        long start = System.currentTimeMillis();
        List<List<ClusterNode>> assignments = aff.assignPartitions_new(ctx);
        return F.t(System.currentTimeMillis() - start, assignments);
    }

    protected RendezvousAffinityFunction affinityFunction() {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(true);

        GridTestUtils.setFieldValue(aff, "ignite", ignite);

        return aff;
    }

    private long average(List<Long> results) {
        if (results.size() == 0)
            return 0;
        long sum = 0;
        for (long l : results)
            sum += l;
        return sum / results.size();
    }

    private double variance(List<Long> results, long average) {
        if (results.size() == 0)
            return 0;

        long sum = 0;
        for (long l : results)
            sum += (l - average) * (l - average);

        return Math.sqrt((double)sum / results.size());
    }

    private Map<ClusterNode, AtomicInteger> freqDistribution(List<List<ClusterNode>> lst) {
        Map<ClusterNode, AtomicInteger> map = new HashMap<>();
        for (List<ClusterNode> l : lst) {
            ClusterNode node = l.get(0);
            if (!map.containsKey(node))
                map.put(node, new AtomicInteger(1));
            else
                map.get(node).incrementAndGet();
        }
        return map;
    }

    void printDistribution(Map<ClusterNode, AtomicInteger> map) {
        for (Map.Entry<ClusterNode, AtomicInteger> e : map.entrySet())
            System.out.println(e.getKey().id() + ", " + e.getValue().get());
    }

    public void testDistribution() {
        int[] nodesCnts = {2, 100, 200, 300, 400, 500, 600};

        for (int nodesCntIdx = 0; nodesCntIdx < nodesCnts.length; ++nodesCntIdx) {
            List<ClusterNode> nodes_old = createBaseNodes(nodesCnts[nodesCntIdx]);
            List<ClusterNode> nodes_new = createBaseNodes(nodesCnts[nodesCntIdx]);

            List<List<ClusterNode>> lst_old = assignPartitions_old(affinityFunction(), nodes_old, nodesCnts[nodesCntIdx] / 10, 0).get2();
            List<List<ClusterNode>> lst_new = assignPartitions_new(affinityFunction(), nodes_new, nodesCnts[nodesCntIdx] / 10, 0).get2();

            Map<ClusterNode, AtomicInteger> old_map = freqDistribution(lst_old);
            Map<ClusterNode, AtomicInteger> new_map = freqDistribution(lst_new);

            info(String.format("------------------------- Old DISTR %d", old_map.size()));
//            printDistribution(old_map);
            info(String.format("------------------------- New DISTR %d", new_map.size()));
//            printDistribution(new_map);
        }
    }

    public void testBench() {
        int[] nodesCnts = {200, 100, 200, 300, 400, 500, 600};
        int[] backupsCnts = {2, 3, 10};

        List<RendezvousAffinityFunction> funcListMd5 = new ArrayList<>();
        List<RendezvousAffinityFunction> funcListWang = new ArrayList<>();
        for (int i = 0; i < MAX_FUNCS; ++i) {
            funcListMd5.add(affinityFunction());
            funcListWang.add(affinityFunction());
        }

        for (int nodesCnt : nodesCnts) {
            List<ClusterNode> nodes_old = createBaseNodes(nodesCnt);
            List<ClusterNode> nodes_new = createBaseNodes(nodesCnt);
            List<Long> times_old = new ArrayList<>(MAX_EXPERIMENTS);
            List<Long> times_new = new ArrayList<>(MAX_EXPERIMENTS);

            for (int i = 0; i < MAX_EXPERIMENTS; ++i) {
                for (RendezvousAffinityFunction aff : funcListMd5)
                    times_new.add(assignPartitions_new(aff, nodes_new, 2, i).get1());
            }

            for (int i = 0; i < MAX_EXPERIMENTS; ++i) {
                for (RendezvousAffinityFunction aff : funcListWang)
                    times_old.add(assignPartitions_old(aff, nodes_old, 2, i).get1());
            }

            long avr = average(times_old);
            double var = variance(times_old, avr);
            long avr_new = average(times_new);
            double var_new = variance(times_new, avr_new);

            info(String.format("Test %d nodes. Old: %d ms +/- %.3f ms; New: %d ms +/- %.3f ms;",
                nodesCnt, avr, var, avr_new, var_new));
        }
    }
}

