/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
