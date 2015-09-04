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

package org.apache.ignite.tests.p2p;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Mock affinity implementation that ensures constant key-to-node mapping based on {@code GridCacheModuloAffinity} The
 * partition selection is as follows: 0 maps to partition 1 and any other value maps to partition 1.
 */
public class GridExternalAffinityFunction implements AffinityFunction {
    /** Node attribute for index. */
    public static final String IDX_ATTR = "nodeIndex";

    /** Number of backups. */
    private int backups;

    /** Number of partitions. */
    private int parts;

    /** Empty constructor. Equivalent for {@code new GridCacheModuloAffinity(2, 0)}. */
    public GridExternalAffinityFunction() {
        this(2, 0);
    }

    /**
     * @param parts   Number of partitions.
     * @param backups Number of backups.
     */
    public GridExternalAffinityFunction(int parts, int backups) {
        assert parts > 0;
        assert backups >= 0;

        this.parts = parts;
        this.backups = backups;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext ctx) {
        List<List<ClusterNode>> res = new ArrayList<>(partitions());

        List<ClusterNode> topSnapshot = ctx.currentTopologySnapshot();

        for (int part = 0; part < parts; part++) {
            res.add(F.isEmpty(topSnapshot) ?
                Collections.<ClusterNode>emptyList() :
                // Wrap affinity nodes with unmodifiable list since unmodifiable generic collection
                // doesn't provide equals and hashCode implementations.
                U.sealList(nodes(part, topSnapshot)));
        }

        return res;
    }

    /**
     * Assigns nodes to one partition.
     *
     * @param part Partition to assign nodes for.
     * @param nodes Cache topology nodes.
     * @return Assigned nodes, first node is primary, others are backups.
     */
    public Collection<ClusterNode> nodes(int part, Collection<ClusterNode> nodes) {
        List<ClusterNode> sorted = new ArrayList<>(nodes);

        Collections.sort(sorted, new Comparator<ClusterNode>() {
            @Override public int compare(ClusterNode n1, ClusterNode n2) {
                int idx1 = n1.<Integer>attribute(IDX_ATTR);
                int idx2 = n2.<Integer>attribute(IDX_ATTR);

                return idx1 < idx2 ? -1 : idx1 == idx2 ? 0 : 1;
            }
        });

        int max = 1 + backups;

        if (max > nodes.size())
            max = nodes.size();

        Collection<ClusterNode> ret = new ArrayList<>(max);

        Iterator<ClusterNode> it = sorted.iterator();

        for (int i = 0; i < max; i++) {
            ClusterNode n = null;

            if (i == 0) {
                while (it.hasNext()) {
                    n = it.next();

                    int nodeIdx = n.<Integer>attribute(IDX_ATTR);

                    if (part <= nodeIdx)
                        break;
                    else
                        n = null;
                }
            }
            else {
                if (it.hasNext())
                    n = it.next();
                else {
                    it = sorted.iterator();

                    assert it.hasNext();

                    n = it.next();
                }
            }

            assert n != null || nodes.size() < parts;

            if (n == null)
                n = (it = sorted.iterator()).next();

            ret.add(n);
        }

        return ret;
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
        return key instanceof Integer ? 0 == (Integer)key ? 0 : 1 : 1;
    }

    /** {@inheritDoc}
     * @param nodeId*/
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridExternalAffinityFunction.class, this);
    }
}