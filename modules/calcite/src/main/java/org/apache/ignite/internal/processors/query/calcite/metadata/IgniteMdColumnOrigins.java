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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
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

    /** */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(Aggregate rel, RelMetadataQuery mq, int iOutputColumn) {
        if (iOutputColumn < rel.getGroupCount()) {
            // get actual index of Group columns.
            return mq.getColumnOrigins(rel.getInput(), rel.getGroupSet().asList().get(iOutputColumn));
        }

        // Aggregate columns are derived from input columns
        AggregateCall call = rel.getAggCallList().get(iOutputColumn - rel.getGroupCount());

        final Set<RelColumnOrigin> set = new HashSet<>();

        for (Integer iInput : call.getArgList()) {
            Set<RelColumnOrigin> inputSet = mq.getColumnOrigins(rel.getInput(), iInput);
            inputSet = createDerivedColumnOrigins(inputSet);

            if (inputSet != null)
                set.addAll(inputSet);

        }
        return set;
    }

    /** */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(Join rel, RelMetadataQuery mq,
        int iOutputColumn) {
        int nLeftColumns = rel.getLeft().getRowType().getFieldList().size();
        Set<RelColumnOrigin> set;
        boolean derived = false;

        if (iOutputColumn < nLeftColumns) {
            set = mq.getColumnOrigins(rel.getLeft(), iOutputColumn);

            if (rel.getJoinType().generatesNullsOnLeft())
                derived = true;

        }
        else {
            set = mq.getColumnOrigins(rel.getRight(), iOutputColumn - nLeftColumns);

            if (rel.getJoinType().generatesNullsOnRight())
                derived = true;
        }

        if (derived) {
            // nulls are generated due to outer join; that counts
            // as derivation
            set = createDerivedColumnOrigins(set);
        }
        return set;
    }

    /** */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(SetOp rel, RelMetadataQuery mq, int iOutputColumn) {
        final Set<RelColumnOrigin> set = new HashSet<>();

        for (RelNode input : rel.getInputs()) {
            Set<RelColumnOrigin> inputSet = mq.getColumnOrigins(input, iOutputColumn);

            if (inputSet == null)
                return null;

            set.addAll(inputSet);
        }

        return set;
    }

    /** */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(Project rel,
        final RelMetadataQuery mq, int iOutputColumn) {
        final RelNode input = rel.getInput();
        RexNode rexNode = rel.getProjects().get(iOutputColumn);

        if (rexNode instanceof RexInputRef) {
            // Direct reference:  no derivation added.
            RexInputRef inputRef = (RexInputRef)rexNode;

            return mq.getColumnOrigins(input, inputRef.getIndex());
        }
        // Anything else is a derivation, possibly from multiple columns.
        final Set<RelColumnOrigin> set = getMultipleColumns(rexNode, input, mq);

        return createDerivedColumnOrigins(set);
    }

    /** */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(Calc rel, final RelMetadataQuery mq, int iOutputColumn) {
        final RelNode input = rel.getInput();
        final RexShuttle rexShuttle = new RexShuttle() {
            @Override public RexNode visitLocalRef(RexLocalRef localRef) {
                return rel.getProgram().expandLocalRef(localRef);
            }
        };
        final List<RexNode> projects = new ArrayList<>();

        for (RexNode rex: rexShuttle.apply(rel.getProgram().getProjectList()))
            projects.add(rex);

        final RexNode rexNode = projects.get(iOutputColumn);

        if (rexNode instanceof RexInputRef) {
            // Direct reference:  no derivation added.
            RexInputRef inputRef = (RexInputRef)rexNode;

            return mq.getColumnOrigins(input, inputRef.getIndex());
        }

        // Anything else is a derivation, possibly from multiple columns.
        final Set<RelColumnOrigin> set = getMultipleColumns(rexNode, input, mq);

        return createDerivedColumnOrigins(set);
    }

    /** */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(Filter rel, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
    }

    /** */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(Sort rel, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
    }

    /** */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(TableModify rel, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
    }

    /** */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(Exchange rel, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
    }

    /** */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(
        TableFunctionScan rel,
        RelMetadataQuery mq,
        int iOutputColumn
    ) {
        Set<RelColumnMapping> mappings = rel.getColumnMappings();

        if (mappings == null) {
            if (!rel.getInputs().isEmpty()) {
                // This is a non-leaf transformation:  say we don't
                // know about origins, because there are probably
                // columns below.
                return null;
            }
            else {
                // This is a leaf transformation: say there are for sure no
                // column origins.
                return Collections.emptySet();
            }
        }

        final Set<RelColumnOrigin> set = new HashSet<>();

        for (RelColumnMapping mapping : mappings) {
            if (mapping.iOutputColumn != iOutputColumn)
                continue;

            final RelNode input = rel.getInputs().get(mapping.iInputRel);
            final int column = mapping.iInputColumn;
            Set<RelColumnOrigin> origins = mq.getColumnOrigins(input, column);

            if (origins == null)
                return null;

            if (mapping.derived)
                origins = createDerivedColumnOrigins(origins);

            set.addAll(origins);
        }

        return set;
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

        return Collections.singleton(rel.columnOriginsByRelLocalRef(iOutputColumn));
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

    /**
     * Catch-all rule when none of the others apply.
     *
     * @param rel RelNode.
     * @param mq RelMetadataQuery.
     * @param iOutputColumn output column idx.
     * @return Set of column origins.
     */
    public @Nullable Set<RelColumnOrigin> getColumnOrigins(RelNode rel, RelMetadataQuery mq, int iOutputColumn) {
        // NOTE jvs 28-Mar-2006: We may get this wrong for a physical table
        // expression which supports projections.  In that case,
        // it's up to the plugin writer to override with the
        // correct information.

        if (!rel.getInputs().isEmpty()) {
            // No generic logic available for non-leaf rels.
            return null;
        }

        final Set<RelColumnOrigin> set = new HashSet<>();

        RelOptTable table = rel.getTable();
        if (table == null) {
            // Somebody is making column values up out of thin air, like a
            // VALUES clause, so we return an empty set.
            return set;
        }

        // Detect the case where a physical table expression is performing
        // projection, and say we don't know instead of making any assumptions.
        // (Theoretically we could try to map the projection using column
        // names.)  This detection assumes the table expression doesn't handle
        // rename as well.
        if (table.getRowType() != rel.getRowType())
            return null;

        set.add(new RelColumnOrigin(table, iOutputColumn, false));

        return set;
    }

    /**
     * Create derived set of column origins from specified.
     *
     * @param inputSet RelColumnOrigin set to derive from.
     * @return derived RelColumnOrigin or {@code null}.
     */
    private static Set<RelColumnOrigin> createDerivedColumnOrigins(Set<RelColumnOrigin> inputSet) {
        if (inputSet == null)
            return null;

        final Set<RelColumnOrigin> set = new HashSet<>();

        for (RelColumnOrigin rco : inputSet) {
            RelColumnOrigin derived = new RelColumnOrigin(rco.getOriginTable(), rco.getOriginColumnOrdinal(), true);
            set.add(derived);
        }

        return set;
    }

    /** */
    private static Set<RelColumnOrigin> getMultipleColumns(RexNode rexNode, RelNode input, final RelMetadataQuery mq) {
        final Set<RelColumnOrigin> set = new HashSet<>();

        final RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
                @Override public Void visitInputRef(RexInputRef inputRef) {
                    Set<RelColumnOrigin> inputSet = mq.getColumnOrigins(input, inputRef.getIndex());

                    if (inputSet != null)
                        set.addAll(inputSet);

                    return null;
                }
            };

        rexNode.accept(visitor);

        return set;
    }
}
