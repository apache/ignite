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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class TableDescriptorImpl extends NullInitializerExpressionFactory implements TableDescriptor {
    /**
     *
     */
    private static final ColumnDescriptor[] DUMMY = new ColumnDescriptor[0];

    private final TableImpl table;

    /**
     *
     */
    private final ColumnDescriptor[] descriptors;

    /**
     *
     */
    private final Map<String, ColumnDescriptor> descriptorsMap;

    /**
     *
     */
    private final ImmutableBitSet insertFields;

    /**
     *
     */
    private final ImmutableBitSet keyFields;

    /**
     *
     */
    public TableDescriptorImpl(
            TableImpl table,
            List<ColumnDescriptor> columnDescriptors
    ) {
        ImmutableBitSet.Builder keyFieldsBuilder = ImmutableBitSet.builder();

        Map<String, ColumnDescriptor> descriptorsMap = new HashMap<>(columnDescriptors.size());
        for (ColumnDescriptor descriptor : columnDescriptors) {
            descriptorsMap.put(descriptor.name(), descriptor);

            if (descriptor.key()) {
                keyFieldsBuilder.set(descriptor.fieldIndex());
            }
        }

        this.descriptors = columnDescriptors.toArray(DUMMY);
        this.descriptorsMap = descriptorsMap;
        this.table = table;

        insertFields = ImmutableBitSet.range(columnDescriptors.size());
        keyFields = keyFieldsBuilder.build();
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType insertRowType(IgniteTypeFactory factory) {
        return rowType(factory, insertFields);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType deleteRowType(IgniteTypeFactory factory) {
        return rowType(factory, keyFields);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDistribution distribution() {
        return IgniteDistributions.random();
    }

    /** {@inheritDoc} */
    @Override
    public TableImpl table() {
        return table;
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

        assert handler.columnCount(res) == (requiredColumns == null ? descriptors.length : requiredColumns.cardinality());

        if (requiredColumns == null) {
            for (int i = 0; i < descriptors.length; i++) {
                ColumnDescriptor desc = descriptors[i];

                handler.set(i, res, row.value(desc.fieldIndex()));
            }
        } else {
            for (int i = 0, j = requiredColumns.nextSetBit(0); j != -1; j = requiredColumns.nextSetBit(j + 1), i++) {
                ColumnDescriptor desc = descriptors[j];

                handler.set(i, res, row.value(desc.fieldIndex()));
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

    /** {@inheritDoc} */
    @Override
    public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
        return !descriptors[colIdx].key();
    }

    /** {@inheritDoc} */
    @Override
    public ColumnStrategy generationStrategy(RelOptTable tbl, int colIdx) {
        if (descriptors[colIdx].hasDefaultValue()) {
            return ColumnStrategy.DEFAULT;
        }

        return super.generationStrategy(tbl, colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public RexNode newColumnDefaultValue(RelOptTable tbl, int colIdx, InitializerContext ctx) {
        final ColumnDescriptor desc = descriptors[colIdx];

        if (!desc.hasDefaultValue()) {
            return super.newColumnDefaultValue(tbl, colIdx, ctx);
        }

        final RexBuilder rexBuilder = ctx.getRexBuilder();
        final IgniteTypeFactory typeFactory = (IgniteTypeFactory) rexBuilder.getTypeFactory();

        return rexBuilder.makeLiteral(desc.defaultValue(), desc.logicalType(typeFactory), false);
    }

    /** {@inheritDoc} */
    @Override
    public RelDataType rowType(IgniteTypeFactory factory, ImmutableBitSet usedColumns) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(factory);

        if (usedColumns == null) {
            for (int i = 0; i < descriptors.length; i++) {
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
            }
        } else {
            for (int i = usedColumns.nextSetBit(0); i != -1; i = usedColumns.nextSetBit(i + 1)) {
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
            }
        }

        return TypeUtils.sqlType(factory, b.build());
    }

    /** {@inheritDoc} */
    @Override
    public ColumnDescriptor columnDescriptor(String fieldName) {
        return fieldName == null ? null : descriptorsMap.get(fieldName);
    }

    /** {@inheritDoc} */
    @Override
    public ColocationGroup colocationGroup(PlanningContext ctx) {
        return partitionedGroup();
    }

    /**
     *
     */
    private ColocationGroup partitionedGroup() {
        List<List<String>> assignments = table.internalTable().assignments().stream()
                .map(Collections::singletonList)
                .collect(Collectors.toList());

        return ColocationGroup.forAssignments(assignments);
    }

    /**
     *
     */
    private <RowT> Tuple insertTuple(RowT row, ExecutionContext<RowT> ectx) {
        Tuple tuple = Tuple.create(descriptors.length);

        RowHandler<RowT> hnd = ectx.rowHandler();

        for (int i = 0; i < descriptors.length; i++) {
            tuple.set(descriptors[i].name(), hnd.get(i, row));
        }

        return tuple;
    }

    /**
     *
     */
    private <RowT> Tuple updateTuple(RowT row, List<String> updateColList, ExecutionContext<RowT> ectx) {
        RowHandler<RowT> hnd = ectx.rowHandler();
        int offset = descriptorsMap.size();
        Tuple tuple = Tuple.create(descriptors.length);
        Set<String> colsToSkip = new HashSet<>(updateColList);

        for (int i = 0; i < descriptors.length; i++) {
            String colName = descriptors[i].name();

            if (!colsToSkip.contains(colName)) {
                tuple.set(colName, hnd.get(i, row));
            }
        }

        for (int i = 0; i < updateColList.size(); i++) {
            final ColumnDescriptor desc = Objects.requireNonNull(descriptorsMap.get(updateColList.get(i)));

            assert !desc.key();

            Object fieldVal = hnd.get(i + offset, row);

            tuple.set(desc.name(), fieldVal);
        }

        return tuple;
    }

    /**
     *
     */
    private <RowT> Tuple deleteTuple(RowT row, ExecutionContext<RowT> ectx) {
        RowHandler<RowT> hnd = ectx.rowHandler();
        Tuple tuple = Tuple.create(keyFields.cardinality());

        int idx = 0;
        for (int i = 0; i < descriptors.length; i++) {
            ColumnDescriptor desc = descriptors[i];

            if (desc.key()) {
                tuple.set(desc.name(), hnd.get(idx++, row));
            }
        }

        return tuple;
    }
}
