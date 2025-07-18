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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdColumnOrigins;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.jetbrains.annotations.Nullable;

/**
 * RelMdColumnOrigins supplies a default implementation of
 * {@link RelMetadataQuery#getColumnOrigins} for the standard logical algebra.
 */
public class IgniteMdColumnOrigins implements MetadataHandler<BuiltInMetadata.ColumnOrigin> {
    /** */
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.COLUMN_ORIGIN.method, new IgniteMdColumnOrigins());

    /** {@inheritDoc} */
    @Override public MetadataDef<BuiltInMetadata.ColumnOrigin> getDef() {
        return BuiltInMetadata.ColumnOrigin.DEF;
    }

    /** Provides column origin for Subset relation. */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(RelSubset rel, RelMetadataQuery mq, int outputColumn) {
        return mq.getColumnOrigins(rel.stripped(), outputColumn);
    }

    /**
     * Get column origins.
     *
     * @param rel Rel to get origins from.
     * @param mq Rel metadata query.
     * @param iOutputColumn Column idx.
     * @return Set of column origins.
     */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(
        ProjectableFilterableTableScan rel,
        RelMetadataQuery mq,
        int iOutputColumn
    ) {
        if (rel.projects() != null) {
            RexNode proj = rel.projects().get(iOutputColumn);
            Set<RexSlot> sources = new HashSet<>();

            getOperands(proj, RexSlot.class, sources);

            boolean derived = sources.size() > 1;
            Set<RelColumnOrigin> res = new HashSet<>();

            for (RexSlot slot : sources) {
                if (slot instanceof RexLocalRef) {
                    RelColumnOrigin slotOrigin = rel.columnOriginsByRelLocalRef(slot.getIndex());

                    res.add(new RelColumnOrigin(slotOrigin.getOriginTable(), slotOrigin.getOriginColumnOrdinal(),
                        derived));
                }
            }

            return res;
        }

        return Set.of(rel.columnOriginsByRelLocalRef(iOutputColumn));
    }

    /**
     * {@link RelMdColumnOrigins} has no method for a {@link Spool}. It takes
     * {@link RelMdColumnOrigins#getColumnOrigins(RelNode, RelMetadataQuery, int)} instead and returns {@code null}
     * because sees an input for current node.
     */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(Spool rel, RelMetadataQuery mq, int outputColumn) {
        return mq.getColumnOrigins(rel.getInput(), outputColumn);
    }

    /**
     * Get operands of specified type from RexCall nodes.
     *
     * @param rn RexNode to get operands
     * @param cls Target class.
     * @param res Set to store results into.
     */
    private <T> void getOperands(RexNode rn, Class<T> cls, Set<T> res) {
        if (cls.isAssignableFrom(rn.getClass()))
            res.add((T)rn);

        if (rn instanceof RexCall) {
            List<RexNode> operands = ((RexCall)rn).getOperands();

            for (RexNode op : operands)
                getOperands(op, cls, res);
        }
    }

}
