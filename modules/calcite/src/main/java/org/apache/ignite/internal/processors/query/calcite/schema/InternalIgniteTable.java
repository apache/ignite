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
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite internal table.
 */
public interface InternalIgniteTable extends IgniteTable {
    /** {@inheritDoc} */
    @Override
    default IgniteLogicalTableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl) {
        return toRel(cluster, relOptTbl, null, null, null);
    }
    
    /**
     * Converts table into relational expression.
     *
     * @param cluster   Custer.
     * @param relOptTbl Table.
     * @param idxName   Index name.
     * @return Table relational expression.
     */
    default IgniteLogicalIndexScan toRel(RelOptCluster cluster, RelOptTable relOptTbl, String idxName) {
        return toRel(cluster, relOptTbl, idxName, null, null, null);
    }
    
    /**
     * Converts table into table scan relational expression.
     */
    IgniteLogicalTableScan toRel(
            RelOptCluster cluster,
            RelOptTable relOptTbl,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    );

    /**
     * Converts table into index scan relational expression.
     */
    IgniteLogicalIndexScan toRel(
            RelOptCluster cluster,
            RelOptTable relOptTbl,
            String idxName,
            List<RexNode> proj,
            RexNode condition,
            ImmutableBitSet requiredCols
    );
    
    /** Returns the internal table. */
    TableImpl table();

    /**
     * Converts a tuple to relational node row.
     *
     * @param ectx            Execution context.
     * @param row             Tuple to convert.
     * @param requiredColumns Participating columns.
     * @return Relational node row.
     */
    <RowT> RowT toRow(
            ExecutionContext<RowT> ectx,
            Tuple row,
            RowHandler.RowFactory<RowT> factory,
            @Nullable ImmutableBitSet requiredColumns
    );

    /**
     * Converts a relational node row to internal tuple.
     *
     * @param ectx Execution context.
     * @param row  Relational node row.
     * @param op   Operation.
     * @param arg  Operation specific argument.
     * @return Cache key-value tuple;
     */
    <RowT> Tuple toTuple(
            ExecutionContext<RowT> ectx,
            RowT row,
            TableModify.Operation op,
            @Nullable Object arg
    );

    /**
     * Returns nodes mapping.
     *
     * @param ctx Planning context.
     * @return Nodes mapping.
     */
    ColocationGroup colocationGroup(PlanningContext ctx);

    /**
     * Returns all table indexes.
     *
     * @return Indexes for the current table.
     */
    Map<String, IgniteIndex> indexes();

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
     * Returns index name.
     *
     * @param idxName Index name.
     */
    void removeIndex(String idxName);
}
