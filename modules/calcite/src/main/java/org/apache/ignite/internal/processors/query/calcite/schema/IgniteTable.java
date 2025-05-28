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
package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite table.
 */
public interface IgniteTable extends TranslatableTable {
    /**
     * @return Table description.
     */
    TableDescriptor<?> descriptor();

    /** {@inheritDoc} */
    default @Override RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return getRowType(typeFactory, null);
    }

    /**
     * Returns new type according {@code usedClumns} param.
     *
     * @param typeFactory Factory.
     * @param requiredColumns Used columns enumeration.
     */
    RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns);

    /** {@inheritDoc} */
    @Override default TableScan toRel(RelOptTable.ToRelContext ctx, RelOptTable relOptTable) {
        return toRel(ctx.getCluster(), relOptTable, null, null, null, ctx.getTableHints());
    }

    /**
     * Converts table into relational expression.
     *
     * @param cluster Custer.
     * @param relOptTbl Table.
     * @param proj List of required projections.
     * @param cond Conditions to filter rows.
     * @param requiredColumns Set of columns to extract from original row.
     * @param hints Table hints.
     * @return Table relational expression.
     */
    IgniteLogicalTableScan toRel(
        RelOptCluster cluster,
        RelOptTable relOptTbl,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColumns,
        @Nullable List<RelHint> hints
    );

    /**
     * Creates rows iterator over the table.
     *
     * @param execCtx Execution context.
     * @param grp Colocation group.
     * @param usedColumns Used columns enumeration.
     * @return Rows iterator.
     */
    public <Row> Iterable<Row> scan(
        ExecutionContext<Row> execCtx,
        ColocationGroup grp,
        @Nullable ImmutableBitSet usedColumns);

    /**
     * Returns nodes mapping.
     *
     * @param ctx Planning context.
     * @return Nodes mapping.
     */
    ColocationGroup colocationGroup(MappingQueryContext ctx);

    /**
     * @return Table distribution.
     */
    IgniteDistribution distribution();

    /**
     * Returns all table indexes.
     *
     * @return Indexes for the current table.
     */
    Map<String, IgniteIndex> indexes();

    /** Return indexes of column representing primary key in the order they are specified in the index. */
    default ImmutableIntList keyColumns() {
        Map<String, IgniteIndex> indexes = indexes();

        if (F.isEmpty(indexes))
            return ImmutableIntList.of();

        IgniteIndex idx = indexes.get("_key_PK");

        return idx == null ? ImmutableIntList.of() : idx.collation().getKeys();
    }

    /**
     * Adds index to table.
     *
     * @param idxTbl Index table.
     */
    void addIndex(IgniteIndex idxTbl);

    /**
     * Returns index by its name.
     *
     * @param idxName Index name.
     * @return Index.
     */
    IgniteIndex getIndex(String idxName);

    /**
     * @param idxName Index name.
     */
    void removeIndex(String idxName);

    /**
     * Is table modifiable.
     */
    boolean isModifiable();

    /**
     * Mark table for index rebuild.
     *
     * @param mark Mark/unmark flag, {@code true} if index rebuild started, {@code false} if finished.
     */
    void markIndexRebuildInProgress(boolean mark);

    /**
     * Returns index rebuild flag.
     *
     * @return {@code True} if index rebuild in progress.
     */
    boolean isIndexRebuildInProgress();

    /**
     * @return Table name.
     */
    String name();

    /**
     * Authorizes operation on table.
     */
    void authorize(Operation op);

    /** */
    enum Operation {
        /** */
        READ,

        /** */
        PUT,

        /** */
        REMOVE
    }
}
