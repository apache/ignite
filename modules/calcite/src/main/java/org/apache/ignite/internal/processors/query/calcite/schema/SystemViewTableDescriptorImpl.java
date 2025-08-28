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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.toSqlName;

/**
 * Table descriptor for system views.
 */
public class SystemViewTableDescriptorImpl<ViewRow> extends NullInitializerExpressionFactory
    implements TableDescriptor<ViewRow> {
    /** */
    private static final SystemViewColumnDescriptor[] DUMMY = new SystemViewColumnDescriptor[0];

    /** */
    private final SystemViewColumnDescriptor[] descriptors;

    /** */
    private final Map<String, SystemViewColumnDescriptor> descriptorsMap;

    /** */
    private final SystemView<ViewRow> sysView;

    /** */
    public SystemViewTableDescriptorImpl(SystemView<ViewRow> sysView) {
        List<SystemViewColumnDescriptor> descriptors = new ArrayList<>(sysView.walker().count());
        List<String> filtrableAttrs = sysView.walker().filtrableAttributes();

        sysView.walker().visitAll(new SystemViewRowAttributeWalker.AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                descriptors.add(new SystemViewColumnDescriptorImpl(name, clazz, idx, filtrableAttrs.contains(name)));
            }
        });

        Map<String, SystemViewColumnDescriptor> descriptorsMap = U.newHashMap(descriptors.size());

        for (SystemViewColumnDescriptor descriptor : descriptors)
            descriptorsMap.put(descriptor.name(), descriptor);

        this.sysView = sysView;
        this.descriptors = descriptors.toArray(DUMMY);
        this.descriptorsMap = descriptorsMap;
    }

    /** System view name. */
    public String name() {
        return toSqlName(sysView.name());
    }

    /** {@inheritDoc} */
    @Override public RelDataType insertRowType(IgniteTypeFactory factory) {
        return rowType(factory, null);
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        return IgniteDistributions.single();
    }

    /** {@inheritDoc} */
    @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
        return ColocationGroup.forNodes(Collections.singletonList(ctx.localNodeId()));
    }

    /** {@inheritDoc} */
    @Override public <Row> Row toRow(
        ExecutionContext<Row> ectx,
        ViewRow row,
        RowHandler.RowFactory<Row> factory,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        RowHandler<Row> hnd = factory.handler();

        assert hnd == ectx.rowHandler();

        Row res = factory.create();

        assert hnd.columnCount(res) == (requiredColumns == null ? descriptors.length : requiredColumns.cardinality());

        sysView.walker().visitAll(row, new SystemViewRowAttributeWalker.AttributeWithValueVisitor() {
            private int colIdx;

            @Override public <T> void accept(int idx, String name, Class<T> clazz, T val) {
                if (requiredColumns == null || requiredColumns.get(idx))
                    hnd.set(colIdx++, res, TypeUtils.toInternal(ectx, val, descriptors[idx].storageType()));
            }

            @Override public void acceptBoolean(int idx, String name, boolean val) {
                accept(idx, name, Boolean.class, val);
            }

            @Override public void acceptChar(int idx, String name, char val) {
                accept(idx, name, Character.class, val);
            }

            @Override public void acceptByte(int idx, String name, byte val) {
                accept(idx, name, Byte.class, val);
            }

            @Override public void acceptShort(int idx, String name, short val) {
                accept(idx, name, Short.class, val);
            }

            @Override public void acceptInt(int idx, String name, int val) {
                accept(idx, name, Integer.class, val);
            }

            @Override public void acceptLong(int idx, String name, long val) {
                accept(idx, name, Long.class, val);
            }

            @Override public void acceptFloat(int idx, String name, float val) {
                accept(idx, name, Float.class, val);
            }

            @Override public void acceptDouble(int idx, String name, double val) {
                accept(idx, name, Double.class, val);
            }
        });

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public RelDataType rowType(IgniteTypeFactory factory, ImmutableBitSet usedColumns) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(factory);

        if (usedColumns == null) {
            for (int i = 0; i < descriptors.length; i++)
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
        }
        else {
            for (int i = usedColumns.nextSetBit(0); i != -1; i = usedColumns.nextSetBit(i + 1))
                b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
        }

        return b.build();
    }

    /** {@inheritDoc} */
    @Override public SystemViewColumnDescriptor columnDescriptor(String fieldName) {
        return fieldName == null ? null : descriptorsMap.get(fieldName);
    }

    /** {@inheritDoc} */
    @Override public Collection<SystemViewColumnDescriptor> columnDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(descriptors));
    }

    /** Underlying system view. */
    public SystemView<ViewRow> systemView() {
        return sysView;
    }

    /** Is system view filttrable. */
    public boolean isFiltrable() {
        return !F.isEmpty(sysView.walker().filtrableAttributes());
    }

    /** */
    private static class SystemViewColumnDescriptorImpl implements SystemViewColumnDescriptor {
        /** */
        private final String sqlName;

        /** */
        private final String originalName;

        /** */
        private final int fieldIdx;

        /** */
        private final Class<?> type;

        /** */
        private final boolean isFiltrable;

        /** */
        private volatile RelDataType logicalType;

        /** */
        private SystemViewColumnDescriptorImpl(String name, Class<?> type, int fieldIdx, boolean isFiltrable) {
            originalName = name;
            sqlName = toSqlName(name);
            this.fieldIdx = fieldIdx;
            this.type = type;
            this.isFiltrable = isFiltrable;
        }

        /** {@inheritDoc} */
        @Override public boolean hasDefaultValue() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Object defaultValue() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return sqlName;
        }

        /** {@inheritDoc} */
        @Override public String originalName() {
            return originalName;
        }

        /** {@inheritDoc} */
        @Override public int fieldIndex() {
            return fieldIdx;
        }

        /** {@inheritDoc} */
        @Override public RelDataType logicalType(IgniteTypeFactory f) {
            // TODO: check
            if (logicalType == null)
                logicalType = TypeUtils.sqlType(f, type, PRECISION_NOT_SPECIFIED, SCALE_NOT_SPECIFIED, true);

            return logicalType;
        }

        /** {@inheritDoc} */
        @Override public Class<?> storageType() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public boolean isFiltrable() {
            return isFiltrable;
        }
    }
}
