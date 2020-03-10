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

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 *
 */
public class TableDescriptorImpl extends NullInitializerExpressionFactory implements TableDescriptor {
    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final GridQueryTypeDescriptor typeDesc;

    /** */
    private final Object affinityIdentity;

    /** */
    private final ColumnDescriptor[] descriptors;

    /** */
    private final Map<String, ColumnDescriptor> descriptorsMap;

    /** */
    private final int keyField;

    /** */
    private final int valField;

    /** */
    private final int affField;

    /** */
    private final BitSet virtualFlags;

    /** */
    public TableDescriptorImpl(GridCacheContext<?,?> cctx, GridQueryTypeDescriptor typeDesc, Object affinityIdentity) {
        this.cctx = cctx;
        this.typeDesc = typeDesc;
        this.affinityIdentity = affinityIdentity;

        Set<String> fields = this.typeDesc.fields().keySet();

        List<ColumnDescriptor> descriptors = new ArrayList<>(fields.size() + 2);

        int keyField = 0, valField = 1, affField = 0;

        // A _key/_val fields is virtual in case there is an alias or a property(es) mapped to _key/_val object fields.
        BitSet virtualFlags = new BitSet();

        descriptors.add(new KeyValDescriptor(QueryUtils.KEY_FIELD_NAME, typeDesc.keyClass(), true));
        descriptors.add(new KeyValDescriptor(QueryUtils.VAL_FIELD_NAME, typeDesc.valueClass(), false));

        for (String field : fields) {
            if (Objects.equals(field, typeDesc.affinityKey()))
                affField = descriptors.size();

            if (Objects.equals(field, typeDesc.keyFieldAlias())) {
                if (typeDesc.affinityKey() == null)
                    affField = descriptors.size();

                keyField = descriptors.size();

                virtualFlags.set(0);

                descriptors.add(new KeyValDescriptor(typeDesc.keyFieldAlias(), typeDesc.keyClass(), true));
            }
            else if (Objects.equals(field, typeDesc.valueFieldAlias())) {
                valField = descriptors.size();

                descriptors.add(new KeyValDescriptor(typeDesc.valueFieldAlias(), typeDesc.valueClass(), false));

                virtualFlags.set(1);
            }
            else {
                GridQueryProperty prop = typeDesc.property(field);

                virtualFlags.set(prop.key() ? 0 : 1);

                descriptors.add(new FieldDescriptor(prop));
            }
        }

        Map<String, ColumnDescriptor> descriptorsMap = U.newHashMap(fields.size() + 2);

        for (ColumnDescriptor descriptor : descriptors)
            descriptorsMap.put(descriptor.name(), descriptor);

        this.keyField = keyField;
        this.valField = valField;
        this.affField = affField;
        this.virtualFlags = virtualFlags;
        this.descriptors = descriptors.toArray(new ColumnDescriptor[0]);
        this.descriptorsMap = descriptorsMap;
    }

    /** {@inheritDoc} */
    @Override public RelDataType apply(RelDataTypeFactory factory) {
        return rowType((IgniteTypeFactory) factory, false);
    }

    /** {@inheritDoc} */
    @Override public RelDataType insertRowType(IgniteTypeFactory factory) {
        return rowType(factory, true);
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext<?, ?> cacheContext() {
        return cctx;
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        if (affinityIdentity == null)
            return IgniteDistributions.broadcast();

        return IgniteDistributions.affinity(affField, cctx.cacheId(), affinityIdentity);
    }

    /** {@inheritDoc} */
    @Override public List<RelCollation> collations() {
        return ImmutableList.of();
    }

    /** {@inheritDoc} */
    @Override public boolean match(CacheDataRow row) {
        return typeDesc.matchType(row.value());
    }

    /** {@inheritDoc} */
    @Override public <T> T toRow(ExecutionContext ectx, CacheDataRow row) throws IgniteCheckedException {
        Object[] res = new Object[descriptors.length];

        for (int i = 0; i < descriptors.length; i++)
            res[i] = descriptors[i].value(ectx, cctx, row);

        return (T) res;
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateAllowed(RelOptTable table, int iColumn) {
        final ColumnDescriptor descriptor = descriptors[iColumn];

        return !descriptor.key() && (descriptor.field() || QueryUtils.isSqlType(descriptor.javaType()));
    }

    /** {@inheritDoc} */
    @Override public ColumnStrategy generationStrategy(RelOptTable table, int iColumn) {
        if (descriptors[iColumn].hasDefaultValue())
            return ColumnStrategy.DEFAULT;

        return super.generationStrategy(table, iColumn);
    }

    /** {@inheritDoc} */
    @Override public RexNode newColumnDefaultValue(RelOptTable table, int iColumn, InitializerContext context) {
        final ColumnDescriptor descriptor = descriptors[iColumn];

        if (!descriptor.hasDefaultValue())
            return super.newColumnDefaultValue(table, iColumn, context);

        final RexBuilder rexBuilder = context.getRexBuilder();
        final IgniteTypeFactory typeFactory = (IgniteTypeFactory) rexBuilder.getTypeFactory();

        return rexBuilder.makeLiteral(descriptor.defaultValue(), descriptor.logicalType(typeFactory), false);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteBiTuple<?, ?> toTuple(ExecutionContext ectx, T row, TableModify.Operation op, Object arg) throws IgniteCheckedException {
        switch (op) {
            case INSERT:
                return insertTuple((Object[]) row);
            case DELETE:
                return deleteTuple((Object[]) row);
            case UPDATE:
                return updateTuple((Object[]) row, (List<String>) arg);
            case MERGE:
                throw new UnsupportedOperationException();
            default:
                throw new AssertionError();
        }
    }

    /** */
    private IgniteBiTuple<?, ?> insertTuple(Object[] row) throws IgniteCheckedException {
        Object key = insertKey(row);
        Object val = insertVal(row);

        if (cctx.binaryMarshaller()) {
            if (key instanceof BinaryObjectBuilder)
                key = ((BinaryObjectBuilder) key).build();

            if (val instanceof BinaryObjectBuilder)
                val = ((BinaryObjectBuilder) val).build();
        }

        typeDesc.validateKeyAndValue(key, val);

        return F.t(key, val);
    }

    /** */
    private Object insertKey(Object[] row) throws IgniteCheckedException {
        Object key = row[keyField];

        if (key == null) {
            key = newVal(typeDesc.keyTypeName(), typeDesc.keyClass());

            // skip _key and _val
            for (int i = 2; i < descriptors.length; i++) {
                final ColumnDescriptor descriptor = descriptors[i];

                if (descriptor.field() && descriptor.key() && row[i] != null)
                    descriptor.set(key, row[i]);
            }
        }

        return key;
    }

    /** */
    private Object insertVal(Object[] row) throws IgniteCheckedException {
        Object val = row[valField];

        if (val == null) {
            val = newVal(typeDesc.valueTypeName(), typeDesc.valueClass());

            // skip _key and _val
            for (int i = 2; i < descriptors.length; i++) {
                final ColumnDescriptor descriptor = descriptors[i];

                if (descriptor.field() && !descriptor.key() && row[i] != null)
                    descriptor.set(val, row[i]);
            }
        }

        return val;
    }

    /** */
    private Object newVal(String typeName, Class<?> typeClass) throws IgniteCheckedException {
        if (cctx.binaryMarshaller()) {
            BinaryObjectBuilder builder = cctx.grid().binary().builder(typeName);
            cctx.prepareAffinityField(builder);

            return builder;
        }

        Class<?> cls = U.classForName(typeName, typeClass);

        try {
            Constructor<?> ctor = cls.getDeclaredConstructor();
            ctor.setAccessible(true);

            return ctor.newInstance();
        }
        catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw instantiationException(typeName, e);
        }
        catch (NoSuchMethodException | SecurityException e) {
            try {
                return GridUnsafe.allocateInstance(cls);
            }
            catch (InstantiationException e0) {
                e0.addSuppressed(e);

                throw instantiationException(typeName, e0);
            }
        }
    }

    /** */
    private IgniteCheckedException instantiationException(String typeName, ReflectiveOperationException e) {
        return S.includeSensitive()
            ? new IgniteCheckedException("Failed to instantiate key [type=" + typeName + ']', e)
            : new IgniteCheckedException("Failed to instantiate key", e);
    }

    /** */
    private IgniteBiTuple<?, ?> updateTuple(Object[] row, List<String> updateColumnList) throws IgniteCheckedException {
        Object key = Objects.requireNonNull(row[0]);
        Object val = clone(Objects.requireNonNull(row[1]));

        for (int i = 0; i < updateColumnList.size(); i++) {
            final ColumnDescriptor descriptor = Objects.requireNonNull(descriptorsMap.get(updateColumnList.get(i)));

            assert !descriptor.key();

            if (descriptor.field())
                descriptor.set(val, row[i + 2]);
            else
                val = row[i + 2];
        }

        if (cctx.binaryMarshaller() && val instanceof BinaryObjectBuilder)
            val = ((BinaryObjectBuilder) val).build();

        typeDesc.validateKeyAndValue(key, val);

        return F.t(key, val);
    }

    /** */
    private Object clone(Object val) throws IgniteCheckedException {
        if (val == null || QueryUtils.isSqlType(val.getClass()))
            return val;

        if (!cctx.binaryMarshaller())
            return cctx.marshaller().unmarshal(cctx.marshaller().marshal(val), U.resolveClassLoader(cctx.gridConfig()));

        BinaryObjectBuilder builder = cctx.grid().binary().builder(
            cctx.grid().binary().<BinaryObject>toBinary(val));

        cctx.prepareAffinityField(builder);

        return builder;
    }

    /** */
    private IgniteBiTuple<?, ?> deleteTuple(Object[] row) throws IgniteCheckedException {
        return F.t(Objects.requireNonNull(row[0]), null);
    }

    /** */
    private RelDataType rowType(IgniteTypeFactory factory, boolean skipVirtual) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(factory);

        for (int i = 0; i < descriptors.length; i++) {
            if (skipVirtual && virtualFlags.get(i))
                continue;

            b.add(descriptors[i].name(), descriptors[i].logicalType(factory));
        }

        return b.build();
    }

    /** */
    private interface ColumnDescriptor {
        /** */
        boolean field();

        /** */
        boolean key();

        /** */
        boolean hasDefaultValue();

        /** */
        String name();

        /** */
        RelDataType logicalType(IgniteTypeFactory f);

        /** */
        Class<?> javaType();

        /** */
        Object value(ExecutionContext ectx, GridCacheContext<?,?> cctx, CacheDataRow src) throws IgniteCheckedException;

        /** */
        Object defaultValue();

        /** */
        void set(Object dst, Object val) throws IgniteCheckedException;
    }

    /** */
    private static class KeyValDescriptor implements ColumnDescriptor {
        /** */
        private final String name;

        /** */
        private final Class<?> type;

        /** */
        private final boolean isKey;

        /** */
        private KeyValDescriptor(String name, Class<?> type, boolean isKey) {
            this.name = name;
            this.type = type;
            this.isKey = isKey;
        }

        /** {@inheritDoc} */
        @Override public boolean field() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean key() {
            return isKey;
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
            return name;
        }

        /** {@inheritDoc} */
        @Override public RelDataType logicalType(IgniteTypeFactory f) {
            return f.createJavaType(javaType());
        }

        /** {@inheritDoc} */
        @Override public Class<?> javaType() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public Object value(ExecutionContext ectx, GridCacheContext<?,?> cctx, CacheDataRow src) throws IgniteCheckedException {
            return cctx.unwrapBinaryIfNeeded(isKey ? src.key() : src.value(), ectx.keepBinary());
        }

        /** {@inheritDoc} */
        @Override public void set(Object dst, Object val) throws IgniteCheckedException {
            throw new AssertionError();
        }
    }

    /** */
    private static class FieldDescriptor implements ColumnDescriptor {
        /** */
        private final GridQueryProperty desc;

        /** */
        private final Object defaultValue;

        /** */
        private FieldDescriptor(GridQueryProperty desc) {
            this.desc = desc;
            defaultValue = desc.defaultValue();
        }

        /** */
        @Override public boolean field() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean key() {
            return desc.key();
        }

        /** {@inheritDoc} */
        @Override public boolean hasDefaultValue() {
            return defaultValue != null;
        }

        /** {@inheritDoc} */
        @Override public Object defaultValue() {
            return defaultValue;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return desc.name();
        }

        /** {@inheritDoc} */
        @Override public RelDataType logicalType(IgniteTypeFactory f) {
            return f.createJavaType(javaType());
        }

        /** {@inheritDoc} */
        @Override public Class<?> javaType() {
            return desc.type();
        }

        /** {@inheritDoc} */
        @Override public Object value(ExecutionContext ectx, GridCacheContext<?,?> cctx, CacheDataRow src) throws IgniteCheckedException {
            return cctx.unwrapBinaryIfNeeded(desc.value(src.key(), src.value()), ectx.keepBinary());
        }

        /** {@inheritDoc} */
        @Override public void set(Object dst, Object val) throws IgniteCheckedException {
            final Object key0 = key() ? dst : null;
            final Object val0 = key() ? null : dst;

            desc.setValue(key0, val0, val);
        }
    }
}
