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
 * Affinity which controls where nodes end up using mod operation.
 */
public class GridCacheModuloAffinityFunction implements AffinityFunction {
    /** Node attribute for index. */
    public static final String IDX_ATTR = "nodeIndex";

    /** Number of backups. */
    private int backups = -1;

    /** Number of partitions. */
    private int parts = -1;

    /**
     * Empty constructor.
     */
    public GridCacheModuloAffinityFunction() {
        // No-op.
    }

    /**
     * @param parts Number of partitions.
     * @param backups Number of backups.
     */
    public GridCacheModuloAffinityFunction(int parts, int backups) {
        assert parts > 0;
        assert backups >= 0;

        this.parts = parts;
        this.backups = backups;
    }

    /**
     * @param parts Number of partitions.
     */
    public void partitions(int parts) {
        assert parts > 0;

        this.parts = parts;
    }

    /**
     * @param backups Number of backups.
     */
    public void backups(int backups) {
        assert backups >= 0;

        this.backups = backups;
    }

    /**
     * @return Number of backups.
     */
    public int backups() {
        return backups;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext ctx) {
        List<List<ClusterNode>> res = new ArrayList<>(parts);

        Collection<ClusterNode> topSnapshot = ctx.currentTopologySnapshot();

        for (int part = 0; part < parts; part++) {
            res.add(F.isEmpty(topSnapshot) ?
                Collections.<ClusterNode>emptyList() :
                // Wrap affinity nodes with unmodifiable list since unmodifiable generic collection
                // doesn't provide equals and hashCode implementations.
                U.sealList(nodes(part, topSnapshot)));
        }

        return Collections.unmodifiableList(res);
    }

    /** {@inheritDoc} */
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

    /**
     * @param parts Number of partitions.
     * @param backups Number of backups.
     */
    public void reset(int parts, int backups) {
        this.parts = parts;
        this.backups = backups;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        parts = -1;
        backups = -1;
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return parts;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key) {
        if (key instanceof Number)
            return ((Number)key).intValue() % parts;

        return key == null ? 0 : U.safeAbs(key.hashCode() % parts);
    }

    /** {@inheritDoc} */
    @Override public void removeNode(UUID nodeId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheModuloAffinityFunction.class, this);
    }
}