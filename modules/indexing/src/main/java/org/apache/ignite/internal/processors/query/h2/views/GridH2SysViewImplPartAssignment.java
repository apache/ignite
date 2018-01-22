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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * System view: cache groups.
 */
public class GridH2SysViewImplPartAssignment extends GridH2SysView {
    /**
     * @param ctx Grid context.
     */
    public GridH2SysViewImplPartAssignment(GridKernalContext ctx) {
        super("PART_ASSIGNMENT", "Partition assignment map", ctx, "CACHE_GROUP_ID",
            newColumn("CACHE_GROUP_ID"),
            newColumn("PARTITION"),
            newColumn("NODE_ID", Value.UUID),
            newColumn("IS_PRIMARY", Value.BOOLEAN)
        );
    }

    /** {@inheritDoc} */
    @Override public Iterable<Row> getRows(Session ses, SearchRow first, SearchRow last) {
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

            cacheGroups = ctx.cache().cacheGroups();;
        }

        return new GroupPartitionIterable(ses, cacheGroups);
    }

    /**
     * Group-partition iterable.
     */
    private class GroupPartitionIterable extends ParentChildIterable<CacheGroupContext> {
        /**
         * @param ses Session.
         * @param groups Cache groups.
         */
        public GroupPartitionIterable(Session ses, Iterable<CacheGroupContext> groups) {
            super(ses, groups);
        }

        /** {@inheritDoc} */
        @Override protected Iterator<Row> parentChildIterator(Session ses, Iterator<CacheGroupContext> groups) {
            return new GroupPartitionIterator(ses, groups);
        }
    }

    /**
     * Partition assignment iterator.
     */
    private class PartitionAssignmentIterator extends ParentChildIterator<List<ClusterNode>, ClusterNode,
        GridTuple3<Integer, ClusterNode, Boolean>> {
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
         * @param ses
         * @param partAssignmentIter Partitions assignment iterator.
         */
        public PartitionAssignmentIterator(Session ses, Iterator<List<ClusterNode>> partAssignmentIter) {
            super(ses, partAssignmentIter);
        }

        /** {@inheritDoc} */
        @Override protected Iterator<ClusterNode> childIterator(List<ClusterNode> partAssignment) {
            return partAssignment.iterator();
        }

        /** {@inheritDoc} */
        @Override protected GridTuple3<Integer, ClusterNode, Boolean> resultByParentChild(
            List<ClusterNode> partAssignment, ClusterNode node) {
            GridTuple3<Integer, ClusterNode, Boolean> res = new GridTuple3<>(part, node, isPrimary);

            isPrimary = Boolean.FALSE;

            return res;
        }
    }

    /**
     * Group-assignment iterator.
     */
    private class GroupPartitionIterator extends ParentChildIterator<CacheGroupContext, GridTuple3<Integer,
        ClusterNode, Boolean>, Row> {
        /**
         * @param ses
         * @param grpIter Groups iterator.
         */
        public GroupPartitionIterator(Session ses, Iterator<CacheGroupContext> grpIter) {
            super(ses, grpIter);
        }

        /** {@inheritDoc} */
        @Override protected Iterator<GridTuple3<Integer, ClusterNode, Boolean>> childIterator(CacheGroupContext grp) {
            return new PartitionAssignmentIterator(getSession(),
                grp.affinity().cachedAffinity(AffinityTopologyVersion.NONE).assignment().iterator());
        }

        /** {@inheritDoc} */
        @Override protected Row resultByParentChild(CacheGroupContext grp, GridTuple3<Integer, ClusterNode, Boolean> t3) {
            Row row = createRow(getSession(), getRowCount(),
                grp.groupId(),
                t3.get1(),
                t3.get2().id(),
                t3.get3()
            );

            return row;
        }
    }
}
