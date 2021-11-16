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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite table implementation.
 */
public class IgniteTableImpl extends AbstractTable implements InternalIgniteTable {
    private final TableDescriptor desc;

    private final TableImpl table;

    private final Statistic statistic;

    private final Map<String, IgniteIndex> indexes = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param desc Table descriptor.
     */
    public IgniteTableImpl(TableDescriptor desc, TableImpl table) {
        this.desc = desc;
        this.table = table;

        statistic = new StatisticsImpl();
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
    public TableImpl table() {
        return table;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteLogicalTableScan toRel(RelOptCluster cluster, RelOptTable relOptTbl) {
        RelTraitSet traitSet = cluster.traitSetOf(distribution())
                .replace(RewindabilityTrait.REWINDABLE);

        return IgniteLogicalTableScan.create(cluster, traitSet, relOptTbl, null, null, null);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteLogicalIndexScan toRel(RelOptCluster cluster, RelOptTable relOptTbl, String idxName) {
        RelTraitSet traitSet = cluster.traitSetOf(Convention.Impl.NONE)
                .replace(distribution())
                .replace(RewindabilityTrait.REWINDABLE)
                .replace(getIndex(idxName).collation());

        return IgniteLogicalIndexScan.create(cluster, traitSet, relOptTbl, idxName, null, null, null);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return desc.distribution();
    }

    /** {@inheritDoc} */
    @Override
    public ColocationGroup colocationGroup(PlanningContext ctx) {
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
            Tuple row,
            RowHandler.RowFactory<RowT> factory,
            @Nullable ImmutableBitSet requiredColumns
    ) {
        RowHandler<RowT> handler = factory.handler();

        assert handler == ectx.rowHandler();

        RowT res = factory.create();

        assert handler.columnCount(res) == (requiredColumns == null ? desc.columnsCount() : requiredColumns.cardinality());

        if (requiredColumns == null) {
            for (int i = 0; i < desc.columnsCount(); i++) {
                ColumnDescriptor colDesc = desc.columnDescriptor(i);

                handler.set(i, res, row.value(colDesc.fieldIndex()));
            }
        } else {
            for (int i = 0, j = requiredColumns.nextSetBit(0); j != -1; j = requiredColumns.nextSetBit(j + 1), i++) {
                ColumnDescriptor colDesc = desc.columnDescriptor(j);

                handler.set(i, res, row.value(colDesc.fieldIndex()));
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public <RowT> Tuple toTuple(
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

    private <RowT> Tuple insertTuple(RowT row, ExecutionContext<RowT> ectx) {
        Tuple tuple = Tuple.create(desc.columnsCount());

        RowHandler<RowT> hnd = ectx.rowHandler();

        for (int i = 0; i < desc.columnsCount(); i++) {
            tuple.set(desc.columnDescriptor(i).name(), hnd.get(i, row));
        }

        return tuple;
    }

    private <RowT> Tuple updateTuple(RowT row, List<String> updateColList, ExecutionContext<RowT> ectx) {
        RowHandler<RowT> hnd = ectx.rowHandler();
        int offset = desc.columnsCount();
        Tuple tuple = Tuple.create(desc.columnsCount());
        Set<String> colsToSkip = new HashSet<>(updateColList);

        for (int i = 0; i < desc.columnsCount(); i++) {
            String colName = desc.columnDescriptor(i).name();

            if (!colsToSkip.contains(colName)) {
                tuple.set(colName, hnd.get(i, row));
            }
        }

        for (int i = 0; i < updateColList.size(); i++) {
            final ColumnDescriptor colDesc = Objects.requireNonNull(desc.columnDescriptor(updateColList.get(i)));

            assert !colDesc.key();

            Object fieldVal = hnd.get(i + offset, row);

            tuple.set(colDesc.name(), fieldVal);
        }

        return tuple;
    }

    private <RowT> Tuple deleteTuple(RowT row, ExecutionContext<RowT> ectx) {
        RowHandler<RowT> hnd = ectx.rowHandler();
        Tuple tuple = Tuple.create();

        int idx = 0;
        for (int i = 0; i < desc.columnsCount(); i++) {
            ColumnDescriptor colDesc = desc.columnDescriptor(i);

            if (colDesc.key()) {
                tuple.set(colDesc.name(), hnd.get(idx++, row));
            }
        }

        return tuple;
    }

    private ColocationGroup partitionedGroup() {
        List<List<String>> assignments = table.internalTable().assignments().stream()
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
