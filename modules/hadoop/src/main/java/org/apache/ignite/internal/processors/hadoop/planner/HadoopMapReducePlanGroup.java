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
import java.util.UUID;

/**
 * Map-reduce plan group of nodes on a single physical machine.
 */
public class HadoopMapReducePlanGroup {
    /** Node. */
    private ClusterNode node;

    /** Nodes. */
    private ArrayList<ClusterNode> nodes;

    /** MAC addresses. */
    private final String macs;

    /** Weight. */
    private int weight;

    /**
     * Constructor.
     *
     * @param node First node in the group.
     * @param macs MAC addresses.
     */
    public HadoopMapReducePlanGroup(ClusterNode node, String macs) {
        assert node != null;
        assert macs != null;

        this.node = node;
        this.macs = macs;
    }

    /**
     * Add node to the group.
     *
     * @param newNode New node.
     */
    public void add(ClusterNode newNode) {
        if (node != null) {
            nodes = new ArrayList<>(2);

            nodes.add(node);

            node = null;
        }

        nodes.add(newNode);
    }

    /**
     * @return MAC addresses.
     */
    public String macs() {
        return macs;
    }

    /**
     * @return {@code True} if only sinle node present.
     */
    public boolean single() {
        return nodeCount() == 1;
    }

    /**
     * Get node ID by index.
     *
     * @param idx Index.
     * @return Node.
     */
    public UUID nodeId(int idx) {
        ClusterNode res;

        if (node != null) {
            assert idx == 0;

            res = node;
        }
        else {
            assert nodes != null;
            assert idx < nodes.size();

            res = nodes.get(idx);
        }

        assert res != null;

        return res.id();
    }

    /**
     * @return Node count.
     */
    public int nodeCount() {
        return node != null ? 1 : nodes.size();
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
