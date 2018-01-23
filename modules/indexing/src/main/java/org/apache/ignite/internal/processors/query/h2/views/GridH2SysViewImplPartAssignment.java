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

package org.apache.ignite.internal.processors.query.h2.views;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: partition assignment map.
 */
public class GridH2SysViewImplPartAssignment extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplPartAssignment(GridKernalContext ctx) {
        super("PART_ASSIGNMENT", "Partition assignment map", ctx, "CACHE_GROUP_ID,PARTITION",
            newColumn("CACHE_GROUP_ID"),
            newColumn("TOPOLOGY_VERSION", Value.LONG),
            newColumn("PARTITION"),
            newColumn("NODE_ID", Value.UUID),
            newColumn("IS_PRIMARY", Value.BOOLEAN)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(final Session ses, SearchRow first, SearchRow last) {
        ColumnCondition idCond = conditionForColumn("CACHE_GROUP_ID", first, last);

        Collection<CacheGroupContext> cacheGroups;

        if (idCond.isEquality()) {
            log.debug("Get part assignment map: filter by group id");

            CacheGroupContext cacheGrp = ctx.cache().cacheGroup(idCond.getValue().getInt());

            if (cacheGrp != null)
                cacheGroups = Collections.<CacheGroupContext>singleton(cacheGrp);
            else
                cacheGroups = Collections.emptySet();
        }
        else {
            log.debug("Get part assignment map: full group scan");

            cacheGroups = ctx.cache().cacheGroups();
        }

        ColumnCondition partCond = conditionForColumn("PARTITION", first, last);

        final int partFilter = partCond.isEquality() ? partCond.getValue().getInt() : -1;

        return new ParentChildRowIterable<CacheGroupContext, PartitionAssignment>(
            ses, cacheGroups,
            new IgniteClosure<CacheGroupContext, Iterator<PartitionAssignment>>() {
                @Override public Iterator<PartitionAssignment> apply(CacheGroupContext grp) {
                    AffinityAssignment affAssignment = grp.affinity().cachedAffinity(AffinityTopologyVersion.NONE);

                    List<List<ClusterNode>> partAssignment = affAssignment.assignment();

                    Iterator<List<ClusterNode>> partAssignmentIter;

                    // Filter by partition number.
                    if (partFilter >= 0) {
                        if (partFilter < partAssignment.size())
                            partAssignmentIter = Collections.singleton(partAssignment.get(partFilter)).iterator();
                        else
                            partAssignmentIter = Collections.emptyIterator();
                    }
                    else
                        partAssignmentIter = partAssignment.iterator();

                    return new PartitionAssignmentIterator(partAssignmentIter, affAssignment.topologyVersion());
                }
            },
            new IgniteBiClosure<CacheGroupContext, PartitionAssignment, Object[]>() {
                @Override public Object[] apply(CacheGroupContext grp,
                    PartitionAssignment partAssignment) {
                    return new Object[] {
                        grp.groupId(),
                        partAssignment.getTopologyVersion().topologyVersion(),
                        partAssignment.getPartition(),
                        partAssignment.getNode().id(),
                        partAssignment.isPrimary()
                    };
                }
            }
        );
    }

    /**
     * Partition assignment iterator.
     */
    private class PartitionAssignmentIterator extends ParentChildIterator<List<ClusterNode>, ClusterNode,
        PartitionAssignment> {
        /** Partition. */
        private int part = 0;

        /** Is primary. */
        private Boolean isPrimary = Boolean.TRUE;

        /** {@inheritDoc} */
        @Override protected void moveParent() {
            super.moveParent();

            part++;
            isPrimary = Boolean.TRUE;
        }

        /**
         * @param partAssignmentIter Partitions assignment iterator.
         */
        public PartitionAssignmentIterator(final Iterator<List<ClusterNode>> partAssignmentIter,
            final AffinityTopologyVersion topVer) {
            super(partAssignmentIter,
                new IgniteClosure<List<ClusterNode>, Iterator<ClusterNode>>() {
                    @Override public Iterator<ClusterNode> apply(List<ClusterNode> nodes) {
                        return nodes.iterator();
                    }
                },
                new IgniteBiClosure<List<ClusterNode>, ClusterNode, PartitionAssignment>() {
                    @Override public PartitionAssignment apply(List<ClusterNode> nodes,
                        ClusterNode node) {
                        return new PartitionAssignment(topVer, node);
                    }
                }
            );
        }

        /** {@inheritDoc} */
        @Override public PartitionAssignment next() {
            PartitionAssignment res = super.next();

            res.setPart(part);
            res.setPrimary(isPrimary);

            isPrimary = Boolean.FALSE;

            return res;
        }
    }

    /**
     * Partition assignment row.
     */
    private class PartitionAssignment {
        /** Topology version. */
        private AffinityTopologyVersion topVer;

        /** Partition. */
        private Integer part;

        /** Node. */
        private ClusterNode node;

        /** Is primary. */
        private Boolean isPrimary;

        /**
         * @param topVer Topology version.
         * @param node Node.
         */
        public PartitionAssignment(AffinityTopologyVersion topVer, ClusterNode node) {
            this.topVer = topVer;
            this.node = node;
        }

        /**
         * Gets topology version.
         */
        public AffinityTopologyVersion getTopologyVersion() {
            return topVer;
        }

        /**
         * Gets partition.
         */
        public Integer getPartition() {
            return part;
        }

        /**
         * Gets node.
         */
        public ClusterNode getNode() {
            return node;
        }

        /**
         * Is primary node for partition.
         */
        public Boolean isPrimary() {
            return isPrimary;
        }

        /**
         * @param part Partition.
         */
        public void setPart(Integer part) {
            this.part = part;
        }

        /**
         * @param primary Primary.
         */
        public void setPrimary(Boolean primary) {
            isPrimary = primary;
        }
    }
}
