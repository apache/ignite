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

package org.apache.ignite.internal.sql.engine.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.prepare.MappingQueryContext;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.sql.engine.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.RewindabilityTrait;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite table implementation.
 */
public class IgniteTableImpl extends AbstractTable implements InternalIgniteTable {
    private final TableDescriptor desc;

    private final InternalTable table;

    private final SchemaRegistry schemaRegistry;

    public final SchemaDescriptor schemaDescriptor;

    private final Statistic statistic;

    private final Map<String, IgniteIndex> indexes = new ConcurrentHashMap<>();

    private final List<ColumnDescriptor> columnsOrderedByPhysSchema;

    /**
     * Constructor.
     *
     * @param desc  Table descriptor.
     * @param table Physical table this schema object created for.
     */
    public IgniteTableImpl(
            TableDescriptor desc,
            InternalTable table,
            SchemaRegistry schemaRegistry
    ) {
        this.desc = desc;
        this.table = table;
        this.schemaRegistry = schemaRegistry;
        this.schemaDescriptor = schemaRegistry.schema();

        assert schemaDescriptor != null;

        List<ColumnDescriptor> tmp = new ArrayList<>(desc.columnsCount());
        for (int i = 0; i < desc.columnsCount(); i++) {
            tmp.add(desc.columnDescriptor(i));
        }

        tmp.sort(Comparator.comparingInt(ColumnDescriptor::physicalIndex));

        columnsOrderedByPhysSchema = tmp;
        statistic = new StatisticsImpl();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteUuid id() {
        return table.tableId();
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet requiredColumns) {
        return desc.rowType((IgniteTypeFactory) typeFactory, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public Statistic getStatistic() {
        return statistic;
    }


    /** {@inheritDoc} */
    @Override
    public TableDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override
    public InternalTable table() {
        return table;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteLogicalTableScan toRel(
            RelOptCluster cluster,
            RelOptTable relOptTbl,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        RelTraitSet traitSet = cluster.traitSetOf(distribution())
                .replace(RewindabilityTrait.REWINDABLE);

        return IgniteLogicalTableScan.create(cluster, traitSet, relOptTbl, proj, cond, requiredColumns);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteLogicalIndexScan toRel(
            RelOptCluster cluster,
            RelOptTable relOptTable,
            String idxName,
            List<RexNode> proj,
            RexNode condition,
            ImmutableBitSet requiredCols
    ) {
        RelTraitSet traitSet = cluster.traitSetOf(Convention.Impl.NONE)
                .replace(distribution())
                .replace(RewindabilityTrait.REWINDABLE)
                .replace(getIndex(idxName).collation());

        return IgniteLogicalIndexScan.create(cluster, traitSet, relOptTable, idxName, proj, condition, requiredCols);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return desc.distribution();
    }

    /** {@inheritDoc} */
    @Override
    public ColocationGroup colocationGroup(MappingQueryContext ctx) {
        return partitionedGroup();
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, IgniteIndex> indexes() {
        return Collections.unmodifiableMap(indexes);
    }

    /** {@inheritDoc} */
    @Override
    public void addIndex(IgniteIndex idxTbl) {
        indexes.put(idxTbl.name(), idxTbl);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteIndex getIndex(String idxName) {
        return indexes.get(idxName);
    }

    /** {@inheritDoc} */
    @Override
    public void removeIndex(String idxName) {
        indexes.remove(idxName);
    }

    /** {@inheritDoc} */
    @Override
    public <C> C unwrap(Class<C> cls) {
        if (cls.isInstance(desc)) {
            return cls.cast(desc);
        }

        return super.unwrap(cls);
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> RowT toRow(
            ExecutionContext<RowT> ectx,
            BinaryRow binaryRow,
            RowHandler.RowFactory<RowT> factory,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        RowHandler<RowT> handler = factory.handler();

        assert handler == ectx.rowHandler();

        RowT res = factory.create();

        assert handler.columnCount(res) == (requiredColumns == null ? desc.columnsCount() : requiredColumns.cardinality());

        Row row = schemaRegistry.resolve(binaryRow, schemaDescriptor);

        if (requiredColumns == null) {
            for (int i = 0; i < desc.columnsCount(); i++) {
                ColumnDescriptor colDesc = desc.columnDescriptor(i);

                handler.set(i, res, row.value(colDesc.physicalIndex()));
            }
        } else {
            for (int i = 0, j = requiredColumns.nextSetBit(0); j != -1; j = requiredColumns.nextSetBit(j + 1), i++) {
                ColumnDescriptor colDesc = desc.columnDescriptor(j);

                handler.set(i, res, row.value(colDesc.physicalIndex()));
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> BinaryRow toBinaryRow(
            ExecutionContext<RowT> ectx,
            RowT row,
            TableModify.Operation op,
            Object arg
    ) {
        switch (op) {
            case INSERT:
                return insertTuple(row, ectx);
            case DELETE:
                return deleteTuple(row, ectx);
            case UPDATE:
                return updateTuple(row, (List<String>) arg, ectx);
            case MERGE:
                throw new UnsupportedOperationException();
            default:
                throw new AssertionError();
        }
    }

    private <RowT> BinaryRow insertTuple(RowT row, ExecutionContext<RowT> ectx) {
        int nonNullVarlenKeyCols = 0;
        int nonNullVarlenValCols = 0;

        RowHandler<RowT> hnd = ectx.rowHandler();

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            if (colDesc.physicalType().spec().fixedLength()) {
                continue;
            }

            Object val = hnd.get(colDesc.logicalIndex(), row);

            if (val != null) {
                if (colDesc.key()) {
                    nonNullVarlenKeyCols++;
                } else {
                    nonNullVarlenValCols++;
                }
            }
        }

        RowAssembler rowAssembler = new RowAssembler(schemaDescriptor, nonNullVarlenKeyCols, nonNullVarlenValCols);

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            RowAssembler.writeValue(rowAssembler, colDesc.physicalType(), hnd.get(colDesc.logicalIndex(), row));
        }

        return rowAssembler.build();
    }

    private <RowT> BinaryRow updateTuple(RowT row, List<String> updateColList, ExecutionContext<RowT> ectx) {
        int nonNullVarlenKeyCols = 0;
        int nonNullVarlenValCols = 0;

        RowHandler<RowT> hnd = ectx.rowHandler();
        int offset = desc.columnsCount();
        Set<String> toUpdate = new HashSet<>(updateColList);

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            if (colDesc.physicalType().spec().fixedLength()) {
                continue;
            }

            Object val = toUpdate.contains(colDesc.name())
                    ? hnd.get(colDesc.logicalIndex() + offset, row)
                    : hnd.get(colDesc.logicalIndex(), row);

            if (val != null) {
                if (colDesc.key()) {
                    nonNullVarlenKeyCols++;
                } else {
                    nonNullVarlenValCols++;
                }
            }
        }

        RowAssembler rowAssembler = new RowAssembler(schemaDescriptor, nonNullVarlenKeyCols, nonNullVarlenValCols);

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            RowAssembler.writeValue(
                    rowAssembler,
                    colDesc.physicalType(),
                    toUpdate.contains(colDesc.name())
                            ? hnd.get(colDesc.logicalIndex() + offset, row)
                            : hnd.get(colDesc.logicalIndex(), row)
            );
        }

        return rowAssembler.build();
    }

    private <RowT> BinaryRow deleteTuple(RowT row, ExecutionContext<RowT> ectx) {
        int nonNullVarlenKeyCols = 0;

        RowHandler<RowT> hnd = ectx.rowHandler();

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            if (!colDesc.key()) {
                break;
            }

            if (colDesc.physicalType().spec().fixedLength()) {
                continue;
            }

            Object val = hnd.get(colDesc.logicalIndex(), row);

            if (val != null) {
                nonNullVarlenKeyCols++;
            }
        }

        RowAssembler rowAssembler = new RowAssembler(schemaDescriptor, nonNullVarlenKeyCols, 0);

        for (ColumnDescriptor colDesc : columnsOrderedByPhysSchema) {
            if (!colDesc.key()) {
                break;
            }

            RowAssembler.writeValue(rowAssembler, colDesc.physicalType(), hnd.get(colDesc.logicalIndex(), row));
        }

        return rowAssembler.build();
    }

    private ColocationGroup partitionedGroup() {
        List<List<String>> assignments = table.assignments().stream()
                .map(Collections::singletonList)
                .collect(Collectors.toList());

        return ColocationGroup.forAssignments(assignments);
    }

    private class StatisticsImpl implements Statistic {
        /** {@inheritDoc} */
        @Override
        public Double getRowCount() {
            return 10_000d;
        }

        /** {@inheritDoc} */
        @Override
        public boolean isKey(ImmutableBitSet cols) {
            return false; // TODO
        }

        /** {@inheritDoc} */
        @Override
        public List<ImmutableBitSet> getKeys() {
            return null; // TODO
        }

        /** {@inheritDoc} */
        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return List.of();
        }

        /** {@inheritDoc} */
        @Override
        public List<RelCollation> getCollations() {
            return List.of(); // The method isn't used
        }

        /** {@inheritDoc} */
        @Override
        public IgniteDistribution getDistribution() {
            return distribution();
        }
    }
}
