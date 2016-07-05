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

package org.apache.ignite.internal.processors.hadoop.planner;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.ArrayList;

/**
 * Map-reduce plan group.
 */
public class HadoopMapReducePlanGroup {
    /** Node. */
    private ClusterNode node;

    /** Nodes. */
    private ArrayList<ClusterNode> nodes;

    /** MAC addresses. */
    private final String macs;

    /** CPUs. */
    private final int cpus;

    /** Weight. */
    private int weight;

    /**
     * Constructor.
     *
     * @param node First node in the group.
     * @param macs MAC addresses.
     */
    public HadoopMapReducePlanGroup(ClusterNode node, String macs) {
        this.node = node;
        this.macs = macs;

        cpus = node.metrics().getTotalCpus();
    }

    /**
     * Add node to the group.
     *
     * @param newNode New node.
     */
    public void add(ClusterNode newNode) {
        assert newNode.metrics().getTotalCpus() == cpus;

        if (node != null) {
            nodes = new ArrayList<>(2);

            nodes.add(node);

            node = null;
        }

        nodes.add(newNode);
    }

    /**
     * @return {@code True} if only sinle node present.
     */
    public boolean single() {
        return nodeCount() == 1;
    }

    /**
     * Get node by index.
     *
     * @param idx Index.
     * @return Node.
     */
    public ClusterNode node(int idx) {
        if (node != null) {
            assert idx == 0;

            return node;
        }
        else {
            assert nodes != null;
            assert idx < nodes.size();

            return nodes.get(idx);
        }
    }

    /**
     * @return Node count.
     */
    public int nodeCount() {
        return node != null ? 1 : nodes.size();
    }

    /**
     * @return CPU count.
     */
    public int cpuCount() {
        return cpus;
    }

    /**
     * @return weight.
     */
    public int weight() {
        return weight;
    }

    /**
     * @param weight weight.
     */
    public void weight(int weight) {
        this.weight = weight;
    }


    /** {@inheritDoc} */
    @Override public int hashCode() {
        return macs.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj instanceof HadoopMapReducePlanGroup && F.eq(macs, ((HadoopMapReducePlanGroup)obj).macs);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HadoopMapReducePlanGroup.class, this);
    }
}
