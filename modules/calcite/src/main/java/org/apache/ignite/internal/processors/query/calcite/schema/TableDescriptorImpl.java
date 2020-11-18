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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.metadata.CollocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 *
 */
@SuppressWarnings({"AssignmentOrReturnOfFieldWithMutableType", "rawtypes"})
public class TableDescriptorImpl extends NullInitializerExpressionFactory
    implements TableDescriptor {
    /** */
    private static final ColumnDescriptor[] DUMMY = new ColumnDescriptor[0];

    /** */
    private final GridCacheContext cctx;

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
    private final ImmutableBitSet insertFields;

    /** */
    public TableDescriptorImpl(GridCacheContext<?,?> cctx, GridQueryTypeDescriptor typeDesc, Object affinityIdentity) {
        this.cctx = cctx;
        this.typeDesc = typeDesc;
        this.affinityIdentity = affinityIdentity;

        Set<String> fields = this.typeDesc.fields().keySet();

        List<ColumnDescriptor> descriptors = new ArrayList<>(fields.size() + 2);

        // A _key/_val field is virtual in case there is an alias or a property(es) mapped to the _key/_val field.
        BitSet virtualFields = new BitSet();

        descriptors.add(
            new KeyValDescriptor(QueryUtils.KEY_FIELD_NAME, typeDesc.keyClass(), true, QueryUtils.KEY_COL));
        descriptors.add(
            new KeyValDescriptor(QueryUtils.VAL_FIELD_NAME, typeDesc.valueClass(), false, QueryUtils.VAL_COL));

        int fldIdx = QueryUtils.VAL_COL + 1;

        int keyField = QueryUtils.KEY_COL;
        int valField = QueryUtils.VAL_COL;
        int affField = QueryUtils.KEY_COL;

        for (String field : fields) {
            if (Objects.equals(field, typeDesc.affinityKey()))
                affField = descriptors.size();

            if (Objects.equals(field, typeDesc.keyFieldAlias())) {
                if (typeDesc.affinityKey() == null)
                    affField = descriptors.size();

                keyField = descriptors.size();

                virtualFields.set(0);

                descriptors.add(new KeyValDescriptor(typeDesc.keyFieldAlias(), typeDesc.keyClass(), true, fldIdx++));
            }
            else if (Objects.equals(field, typeDesc.valueFieldAlias())) {
                valField = descriptors.size();

                virtualFields.set(1);

                descriptors.add(new KeyValDescriptor(typeDesc.valueFieldAlias(), typeDesc.valueClass(), false, fldIdx++));
            }
            else {
                GridQueryProperty prop = typeDesc.property(field);

                virtualFields.set(prop.key() ? 0 : 1);

                descriptors.add(new FieldDescriptor(prop, fldIdx++));
            }
        }

        Map<String, ColumnDescriptor> descriptorsMap = U.newHashMap(descriptors.size());
        for (ColumnDescriptor descriptor : descriptors)
            descriptorsMap.put(descriptor.name(), descriptor);

        if (TypeUtils.isConvertableType(descriptors.get(affField).storageType()))
            affField = -1;

        this.keyField = keyField;
        this.valField = valField;
        this.affField = affField;
        this.descriptors = descriptors.toArray(DUMMY);
        this.descriptorsMap = descriptorsMap;

        virtualFields.flip(0, descriptors.size());
        insertFields = ImmutableBitSet.fromBitSet(virtualFields);
    }

    /** {@inheritDoc} */
    @Override public RelDataType insertRowType(IgniteTypeFactory factory) {
        return rowType(factory, insertFields);
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext cacheContext() {
        return cctx;
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        if (affinityIdentity == null)
            return IgniteDistributions.broadcast();
        else if (affField == -1)
            return IgniteDistributions.random();
        else
            return IgniteDistributions.affinity(affField, cctx.cacheId(), affinityIdentity);
    }

    /** {@inheritDoc} */
    @Override public boolean match(CacheDataRow row) {
        return typeDesc.matchType(row.value());
    }

    /** {@inheritDoc} */
    @Override public <Row> Row toRow(
        ExecutionContext<Row> ectx,
        CacheDataRow row,
        RowHandler.RowFactory<Row> factory,
        @Nullable ImmutableBitSet requiredColunms
    ) throws IgniteCheckedException {
        RowHandler<Row> handler = factory.handler();

        assert handler == ectx.rowHandler();

        Row res = factory.create();

        assert handler.columnCount(res) == (requiredColunms == null ? descriptors.length : requiredColunms.cardinality());

        if (requiredColunms == null) {
            for (int i = 0; i < descriptors.length; i++) {
                ColumnDescriptor desc = descriptors[i];

                handler.set(i, res, TypeUtils.toInternal(ectx,
                    desc.value(ectx, cctx, row), desc.storageType()));
            }
        }
        else {
            for (int i = 0, j = requiredColunms.nextSetBit(0); j != -1; j = requiredColunms.nextSetBit(j + 1), i++) {
                ColumnDescriptor desc = descriptors[j];

                handler.set(i, res, TypeUtils.toInternal(ectx,
                    desc.value(ectx, cctx, row), desc.storageType()));
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
        final ColumnDescriptor desc = descriptors[colIdx];

        return !desc.key() && (desc.field() || QueryUtils.isSqlType(desc.storageType()));
    }

    /** {@inheritDoc} */
    @Override public ColumnStrategy generationStrategy(RelOptTable tbl, int colIdx) {
        if (descriptors[colIdx].hasDefaultValue())
            return ColumnStrategy.DEFAULT;

        return super.generationStrategy(tbl, colIdx);
    }

    /** {@inheritDoc} */
    @Override public RexNode newColumnDefaultValue(RelOptTable tbl, int colIdx, InitializerContext ctx) {
        final ColumnDescriptor desc = descriptors[colIdx];

        if (!desc.hasDefaultValue())
            return super.newColumnDefaultValue(tbl, colIdx, ctx);

        final RexBuilder rexBuilder = ctx.getRexBuilder();
        final IgniteTypeFactory typeFactory = (IgniteTypeFactory) rexBuilder.getTypeFactory();

        return rexBuilder.makeLiteral(desc.defaultValue(), desc.logicalType(typeFactory), false);
    }

    /** {@inheritDoc} */
    @Override public <Row> IgniteBiTuple toTuple(ExecutionContext<Row> ectx, Row row,
        TableModify.Operation op, Object arg) throws IgniteCheckedException {
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

    /** */
    private <Row> IgniteBiTuple insertTuple(Row row, ExecutionContext<Row> ectx) throws IgniteCheckedException {
        Object key = insertKey(row, ectx);
        Object val = insertVal(row, ectx);

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
    private <Row> Object insertKey(Row row, ExecutionContext<Row> ectx) throws IgniteCheckedException {
        RowHandler<Row> handler = ectx.rowHandler();

        Object key = handler.get(keyField, row);

        if (key == null) {
            key = newVal(typeDesc.keyTypeName(), typeDesc.keyClass());

            // skip _key and _val
            for (int i = 2; i < descriptors.length; i++) {
                final ColumnDescriptor desc = descriptors[i];

                Object fieldVal = handler.get(i, row);

                if (desc.field() && desc.key() && fieldVal != null)
                    desc.set(key, TypeUtils.fromInternal(ectx, fieldVal, desc.storageType()));
            }
        }
        else
            key = TypeUtils.fromInternal(ectx, key, descriptors[QueryUtils.KEY_COL].storageType());

        return key;
    }

    /** */
    private <Row> Object insertVal(Row row, ExecutionContext<Row> ectx) throws IgniteCheckedException {
        RowHandler<Row> handler = ectx.rowHandler();

        Object val = handler.get(valField, row);

        if (val == null) {
            val = newVal(typeDesc.valueTypeName(), typeDesc.valueClass());

            // skip _key and _val
            for (int i = 2; i < descriptors.length; i++) {
                final ColumnDescriptor desc = descriptors[i];

                Object fieldVal = handler.get(i, row);

                if (desc.field() && !desc.key() && fieldVal != null)
                    desc.set(val, TypeUtils.fromInternal(ectx, fieldVal, desc.storageType()));
            }
        }
        else
            val = TypeUtils.fromInternal(ectx, val, descriptors[QueryUtils.VAL_COL].storageType());

        return val;
    }

    /** */
    private Object newVal(String typeName, Class<?> typeCls) throws IgniteCheckedException {
        if (cctx.binaryMarshaller()) {
            BinaryObjectBuilder builder = cctx.grid().binary().builder(typeName);
            cctx.prepareAffinityField(builder);

            return builder;
        }

        Class<?> cls = U.classForName(typeName, typeCls);

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
    private <Row> IgniteBiTuple updateTuple(Row row, List<String> updateColList, ExecutionContext<Row> ectx)
        throws IgniteCheckedException {
        RowHandler<Row> handler = ectx.rowHandler();

        Object key = Objects.requireNonNull(handler.get(QueryUtils.KEY_COL, row));
        Object val = clone(Objects.requireNonNull(handler.get(QueryUtils.VAL_COL, row)));

        for (int i = 0; i < updateColList.size(); i++) {
            final ColumnDescriptor desc = Objects.requireNonNull(descriptorsMap.get(updateColList.get(i)));

            assert !desc.key();

            Object fieldVal = handler.get(i + 2, row);

            if (desc.field())
                desc.set(val, TypeUtils.fromInternal(ectx, fieldVal, desc.storageType()));
            else
                val = TypeUtils.fromInternal(ectx, fieldVal, desc.storageType());
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
    private <Row> IgniteBiTuple deleteTuple(Row row, ExecutionContext<Row> ectx) {
        Object key = TypeUtils.fromInternal(ectx,
            ectx.rowHandler().get(QueryUtils.KEY_COL, row), descriptors[QueryUtils.KEY_COL].storageType());
        return F.t(Objects.requireNonNull(key), null);
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

        return TypeUtils.sqlType(factory, b.build());
    }

    /** {@inheritDoc} */
    @Override public ColumnDescriptor columnDescriptor(String fieldName) {
        return fieldName == null ? null : descriptorsMap.get(fieldName);
    }

    /** {@inheritDoc} */
    @Override public CollocationGroup colocationGroup(PlanningContext ctx) {
        if (!cctx.gate().enterIfNotStopped())
            throw U.convertException(new CacheStoppedException(cctx.name()));

        try {
            return cctx.isReplicated()
                ? replicatedGroup(ctx.topologyVersion())
                : partitionedGroup(ctx.topologyVersion());

        }
        finally {
            cctx.gate().leave();
        }
    }

    /** */
    private CollocationGroup partitionedGroup(@NotNull AffinityTopologyVersion topVer) {
        List<List<ClusterNode>> assignments = cctx.affinity().assignments(topVer);
        List<List<UUID>> assignments0;

        if (cctx.config().getWriteSynchronizationMode() != CacheWriteSynchronizationMode.PRIMARY_SYNC)
            assignments0 = Commons.transform(assignments, nodes -> Commons.transform(nodes, ClusterNode::id));
        else {
            assignments0 = new ArrayList<>(assignments.size());

            for (List<ClusterNode> partNodes : assignments)
                assignments0.add(F.isEmpty(partNodes) ? emptyList() : singletonList(F.first(partNodes).id()));
        }

        return CollocationGroup.forAssignments(assignments0);
    }

    /** */
    private CollocationGroup replicatedGroup(@NotNull AffinityTopologyVersion topVer) {
        GridDhtPartitionTopology top = cctx.topology();

        List<ClusterNode> nodes = cctx.discovery().discoCache(topVer).cacheGroupAffinityNodes(cctx.cacheId());
        List<UUID> nodes0;

        if (!top.rebalanceFinished(topVer)) {
            nodes0 = new ArrayList<>(nodes.size());

            int parts = top.partitions();

            for (ClusterNode node : nodes) {
                if (isOwner(node.id(), top, parts))
                    nodes0.add(node.id());
            }
        }
        else
            nodes0 = Commons.transform(nodes, ClusterNode::id);

        return CollocationGroup.forNodes(nodes0);
    }

    /** */
    private boolean isOwner(UUID nodeId, GridDhtPartitionTopology top, int parts) {
        for (int p = 0; p < parts; p++) {
            if (top.partitionState(nodeId, p) != GridDhtPartitionState.OWNING)
                return false;
        }
        return true;
    }

    /** */
    private static class KeyValDescriptor implements ColumnDescriptor {
        /** */
        private final String name;

        /** */
        private final boolean isKey;

        /** */
        private final int fieldIdx;

        /** */
        private final Class<?> storageType;

        /** */
        private KeyValDescriptor(String name, Class<?> type, boolean isKey, int fieldIdx) {
            this.name = name;
            this.isKey = isKey;
            this.fieldIdx = fieldIdx;

            storageType = type;
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
        @Override public int fieldIndex() {
            return fieldIdx;
        }

        /** {@inheritDoc} */
        @Override public RelDataType logicalType(IgniteTypeFactory f) {
            return f.createJavaType(storageType);
        }

        /** {@inheritDoc} */
        @Override public Class<?> storageType() {
            return storageType;
        }

        /** {@inheritDoc} */
        @Override public Object value(ExecutionContext<?> ectx, GridCacheContext<?, ?> cctx, CacheDataRow src) {
            return cctx.unwrapBinaryIfNeeded(isKey ? src.key() : src.value(), ectx.keepBinary());
        }

        /** {@inheritDoc} */
        @Override public void set(Object dst, Object val) {
            throw new AssertionError();
        }
    }

    /** */
    private static class FieldDescriptor implements ColumnDescriptor {
        /** */
        private final GridQueryProperty desc;

        /** */
        private final Object dfltVal;

        /** */
        private final int fieldIdx;

        /** */
        private final Class<?> storageType;

        /** */
        private FieldDescriptor(GridQueryProperty desc, int fieldIdx) {
            this.desc = desc;
            this.fieldIdx = fieldIdx;

            dfltVal = desc.defaultValue();
            storageType = desc.type();
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
            return dfltVal != null;
        }

        /** {@inheritDoc} */
        @Override public Object defaultValue() {
            return dfltVal;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return desc.name();
        }

        /** {@inheritDoc} */
        @Override public int fieldIndex() {
            return fieldIdx;
        }

        /** {@inheritDoc} */
        @Override public RelDataType logicalType(IgniteTypeFactory f) {
            return f.createJavaType(storageType);
        }

        /** {@inheritDoc} */
        @Override public Class<?> storageType() {
            return storageType;
        }

        /** {@inheritDoc} */
        @Override public Object value(ExecutionContext<?> ectx, GridCacheContext<?, ?> cctx, CacheDataRow src)
            throws IgniteCheckedException {
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
