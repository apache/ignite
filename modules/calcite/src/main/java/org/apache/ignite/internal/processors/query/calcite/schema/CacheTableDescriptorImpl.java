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
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.DataContext;
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
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilders;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseDataContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;

/**
 *
 */
public class CacheTableDescriptorImpl extends NullInitializerExpressionFactory
    implements CacheTableDescriptor {
    /** */
    private static final CacheColumnDescriptor[] DUMMY = new CacheColumnDescriptor[0];

    /** */
    private final GridCacheContextInfo<?, ?> cacheInfo;

    /** */
    private final GridQueryTypeDescriptor typeDesc;

    /** */
    private final Object affinityIdentity;

    /** */
    private final CacheColumnDescriptor[] descriptors;

    /** */
    private final Map<String, CacheColumnDescriptor> descriptorsMap;

    /** */
    private final int keyField;

    /** */
    private final int valField;

    /** */
    private final ImmutableIntList affFields;

    /** */
    private final ImmutableBitSet insertFields;

    /** */
    public CacheTableDescriptorImpl(GridCacheContextInfo<?, ?> cacheInfo, GridQueryTypeDescriptor typeDesc,
        Object affinityIdentity) {
        this.cacheInfo = cacheInfo;
        this.typeDesc = typeDesc;
        this.affinityIdentity = affinityIdentity;

        Set<String> fields = this.typeDesc.fields().keySet();

        List<CacheColumnDescriptor> descriptors = new ArrayList<>(fields.size() + 2);

        // A _key/_val field is virtual in case there is an alias or a property(es) mapped to the _key/_val field.
        BitSet virtualFields = new BitSet();

        if (typeDesc.implicitPk()) {
            // pk is not set, thus we need to provide default value for autogenerated key
            descriptors.add(
                new KeyValDescriptor(QueryUtils.KEY_FIELD_NAME, typeDesc.keyClass(), true, QueryUtils.KEY_COL) {
                    @Override public Object defaultValue() {
                        return IgniteUuid.randomUuid();
                    }
                }
            );

            virtualFields.set(0);
        }
        else {
            List<Class<?>> keyComponents = typeDesc.keyComponentClasses();
            List<Class<?>> keyFullType = F.isEmpty(keyComponents)
                ? Collections.singletonList(typeDesc.keyClass())
                : Stream.concat(Stream.of(typeDesc.keyClass()), keyComponents.stream()).collect(Collectors.toList());

            assert keyFullType.size() < 2 || Collection.class.isAssignableFrom(keyFullType.get(0))
                || Map.class.isAssignableFrom(keyFullType.get(0));

            descriptors.add(new KeyValDescriptor(QueryUtils.KEY_FIELD_NAME, keyFullType, true, QueryUtils.KEY_COL));
        }

        descriptors.add(
            new KeyValDescriptor(QueryUtils.VAL_FIELD_NAME, typeDesc.valueClass(), false, QueryUtils.VAL_COL));

        int fldIdx = QueryUtils.VAL_COL + 1;

        int keyField = QueryUtils.KEY_COL;
        int valField = QueryUtils.VAL_COL;

        for (String field : fields) {
            GridQueryProperty prop = typeDesc.property(field);

            if (Objects.equals(field, typeDesc.keyFieldAlias())) {
                keyField = descriptors.size();

                virtualFields.set(0);

                descriptors.add(new KeyValDescriptor(typeDesc.keyFieldAlias(), prop, true, fldIdx++));
            }
            else if (Objects.equals(field, typeDesc.valueFieldAlias())) {
                valField = descriptors.size();

                virtualFields.set(1);

                descriptors.add(new KeyValDescriptor(typeDesc.valueFieldAlias(), prop, false, fldIdx++));
            }
            else {
                virtualFields.set(prop.key() ? 0 : 1);

                descriptors.add(new FieldDescriptor(prop, fldIdx++));
            }
        }

        Map<String, CacheColumnDescriptor> descriptorsMap = U.newHashMap(descriptors.size());
        for (CacheColumnDescriptor descriptor : descriptors)
            descriptorsMap.put(descriptor.name(), descriptor);

        List<Integer> affFields = new ArrayList<>();
        if (!F.isEmpty(typeDesc.affinityKey()))
            affFields.add(descriptorsMap.get(typeDesc.affinityKey()).fieldIndex());
        else if (!F.isEmpty(typeDesc.keyFieldAlias()))
            affFields.add(descriptorsMap.get(typeDesc.keyFieldAlias()).fieldIndex());
        else if (!F.isEmpty(typeDesc.primaryKeyFields())) {
            affFields.addAll(
                descriptors.stream()
                    .filter(desc -> typeDesc.primaryKeyFields().contains(desc.name()))
                    .map(ColumnDescriptor::fieldIndex)
                    .collect(Collectors.toList())
            );
        }
        else {
            affFields.addAll(
                descriptors.stream()
                    .filter(desc -> typeDesc.fields().containsKey(desc.name()) && typeDesc.property(desc.name()).key())
                    .map(ColumnDescriptor::fieldIndex)
                    .collect(Collectors.toList())
            );
        }

        if (affFields.stream().map(descriptors::get).map(ColumnDescriptor::storageType)
            .anyMatch(TypeUtils::isConvertableType))
            affFields.clear();

        this.keyField = keyField;
        this.valField = valField;
        this.affFields = ImmutableIntList.copyOf(affFields);
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
        return cacheInfo.cacheContext();
    }

    /** {@inheritDoc} */
    @Override public GridCacheContextInfo cacheInfo() {
        return cacheInfo;
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        if (affinityIdentity == null)
            return IgniteDistributions.broadcast();
        else if (affFields.isEmpty())
            return IgniteDistributions.random();
        else
            return IgniteDistributions.affinity(affFields, cacheInfo.cacheId(), affinityIdentity);
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
        @Nullable ImmutableBitSet requiredColumns
    ) throws IgniteCheckedException {
        RowHandler<Row> hnd = factory.handler();

        assert hnd == ectx.rowHandler();

        Row res = factory.create();

        assert hnd.columnCount(res) == (requiredColumns == null ? descriptors.length : requiredColumns.cardinality());

        if (requiredColumns == null) {
            for (int i = 0; i < descriptors.length; i++) {
                CacheColumnDescriptor desc = descriptors[i];

                hnd.set(i, res, TypeUtils.toInternal(ectx,
                    desc.value(ectx, cacheContext(), row), desc.storageType()));
            }
        }
        else {
            for (int i = 0, j = requiredColumns.nextSetBit(0); j != -1; j = requiredColumns.nextSetBit(j + 1), i++) {
                CacheColumnDescriptor desc = descriptors[j];

                hnd.set(i, res, TypeUtils.toInternal(ectx,
                    desc.value(ectx, cacheContext(), row), desc.storageType()));
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
        final CacheColumnDescriptor desc = descriptors[colIdx];

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
        final IgniteTypeFactory typeFactory = (IgniteTypeFactory)rexBuilder.getTypeFactory();

        DataContext dataCtx = new BaseDataContext(typeFactory);

        return TypeUtils.toRexLiteral(desc.defaultValue(), desc.logicalType(typeFactory), dataCtx, rexBuilder);
    }

    /** {@inheritDoc} */
    @Override public <Row> ModifyTuple toTuple(ExecutionContext<Row> ectx, Row row,
        TableModify.Operation op, Object arg) throws IgniteCheckedException {
        switch (op) {
            case INSERT:
                return insertTuple(row, ectx);
            case DELETE:
                return deleteTuple(row, ectx);
            case UPDATE:
                return updateTuple(row, (List<String>)arg, 0, ectx);
            case MERGE:
                return mergeTuple(row, (List<String>)arg, ectx);
            default:
                throw new AssertionError();
        }
    }

    /** */
    private <Row> ModifyTuple insertTuple(Row row, ExecutionContext<Row> ectx) throws IgniteCheckedException {
        Object key = insertKey(row, ectx);
        Object val = insertVal(row, ectx);

        if (key instanceof BinaryObjectBuilder)
            key = ((BinaryObjectBuilder)key).build();

        if (val instanceof BinaryObjectBuilder)
            val = ((BinaryObjectBuilder)val).build();

        typeDesc.validateKeyAndValue(key, val);

        return new ModifyTuple(key, val, TableModify.Operation.INSERT);
    }

    /** */
    private <Row> Object insertKey(Row row, ExecutionContext<Row> ectx) throws IgniteCheckedException {
        RowHandler<Row> hnd = ectx.rowHandler();

        Object key = hnd.get(keyField, row);

        if (key != null)
            return TypeUtils.fromInternal(ectx, key, descriptors[QueryUtils.KEY_COL].storageType());

        // skip _key and _val
        for (int i = 2; i < descriptors.length; i++) {
            final CacheColumnDescriptor desc = descriptors[i];

            if (!desc.field() || !desc.key())
                continue;

            Object fieldVal = hnd.get(i, row);

            if (fieldVal != null) {
                if (key == null)
                    key = newVal(typeDesc.keyTypeName());

                desc.set(key, TypeUtils.fromInternal(ectx, fieldVal, desc.storageType()));
            }
        }

        if (key == null)
            key = descriptors[QueryUtils.KEY_COL].defaultValue();

        return key;
    }

    /** */
    private <Row> Object insertVal(Row row, ExecutionContext<Row> ectx) throws IgniteCheckedException {
        RowHandler<Row> hnd = ectx.rowHandler();

        Object val = hnd.get(valField, row);

        if (val == null) {
            val = newVal(typeDesc.valueTypeName());

            // skip _key and _val
            for (int i = 2; i < descriptors.length; i++) {
                final CacheColumnDescriptor desc = descriptors[i];

                Object fieldVal = hnd.get(i, row);

                if (desc.field() && !desc.key() && fieldVal != null)
                    desc.set(val, TypeUtils.fromInternal(ectx, fieldVal, desc.storageType()));
            }
        }
        else
            val = TypeUtils.fromInternal(ectx, val, descriptors[QueryUtils.VAL_COL].storageType());

        return val;
    }

    /** */
    private Object newVal(String typeName) throws IgniteCheckedException {
        GridCacheContext<?, ?> cctx = cacheContext();

        BinaryObjectBuilder builder = cctx.grid().binary().builder(typeName);
        BinaryObjectBuilders.prepareAffinityField(builder, cctx.cacheObjectContext());

        return builder;
    }

    /** */
    private <Row> ModifyTuple updateTuple(Row row, List<String> updateColList, int offset, ExecutionContext<Row> ectx)
        throws IgniteCheckedException {
        RowHandler<Row> hnd = ectx.rowHandler();

        Object key = Objects.requireNonNull(hnd.get(offset + QueryUtils.KEY_COL, row));
        Object val = clone(Objects.requireNonNull(hnd.get(offset + QueryUtils.VAL_COL, row)));

        offset += descriptorsMap.size();

        for (int i = 0; i < updateColList.size(); i++) {
            final CacheColumnDescriptor desc = Objects.requireNonNull(descriptorsMap.get(updateColList.get(i)));

            assert !desc.key();

            Object fieldVal = hnd.get(i + offset, row);

            if (desc.field())
                desc.set(val, TypeUtils.fromInternal(ectx, fieldVal, desc.storageType()));
            else
                val = TypeUtils.fromInternal(ectx, fieldVal, desc.storageType());
        }

        if (val instanceof BinaryObjectBuilder)
            val = ((BinaryObjectBuilder)val).build();

        typeDesc.validateKeyAndValue(key, val);

        return new ModifyTuple(key, val, TableModify.Operation.UPDATE);
    }

    /** */
    private <Row> ModifyTuple mergeTuple(Row row, List<String> updateColList, ExecutionContext<Row> ectx)
        throws IgniteCheckedException {
        RowHandler<Row> hnd = ectx.rowHandler();

        int rowColumnsCnt = hnd.columnCount(row);

        if (rowColumnsCnt == descriptors.length)
            return insertTuple(row, ectx); // Only WHEN NOT MATCHED clause in MERGE.
        else if (rowColumnsCnt == descriptors.length + updateColList.size())
            return updateTuple(row, updateColList, 0, ectx); // Only WHEN MATCHED clause in MERGE.
        else {
            // Both WHEN MATCHED and WHEN NOT MATCHED clauses in MERGE.
            assert rowColumnsCnt == descriptors.length * 2 + updateColList.size() : "Unexpected columns count: " +
                rowColumnsCnt;

            int updateOffset = descriptors.length; // Offset of fields for update statement.

            if (hnd.get(updateOffset + QueryUtils.KEY_COL, row) != null)
                return updateTuple(row, updateColList, updateOffset, ectx);
            else
                return insertTuple(row, ectx);
        }
    }

    /** */
    private Object clone(Object val) {
        if (val == null || QueryUtils.isSqlType(val.getClass()))
            return val;

        GridCacheContext<?, ?> cctx = cacheContext();

        BinaryObjectBuilder builder = cctx.grid().binary().builder(
            cctx.grid().binary().<BinaryObject>toBinary(val));

        BinaryObjectBuilders.prepareAffinityField(builder, cctx.cacheObjectContext());

        return builder;
    }

    /** */
    private <Row> ModifyTuple deleteTuple(Row row, ExecutionContext<Row> ectx) {
        Object key = TypeUtils.fromInternal(ectx,
            ectx.rowHandler().get(QueryUtils.KEY_COL, row), descriptors[QueryUtils.KEY_COL].storageType());
        return new ModifyTuple(Objects.requireNonNull(key), null, TableModify.Operation.DELETE);
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
    @Override public ColumnDescriptor columnDescriptor(String fieldName) {
        return fieldName == null ? null : descriptorsMap.get(fieldName);
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheColumnDescriptor> columnDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(descriptors));
    }

    /** {@inheritDoc} */
    @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
        GridCacheContext<?, ?> cctx = cacheContext();

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
    private ColocationGroup partitionedGroup(@NotNull AffinityTopologyVersion topVer) {
        GridCacheContext<?, ?> cctx = cacheContext();

        List<List<ClusterNode>> assignments = cctx.affinity().assignments(topVer);
        List<List<UUID>> assignments0;

        if (cctx.config().getWriteSynchronizationMode() != CacheWriteSynchronizationMode.PRIMARY_SYNC)
            assignments0 = Commons.transform(assignments, nodes -> Commons.transform(nodes, ClusterNode::id));
        else {
            assignments0 = new ArrayList<>(assignments.size());

            for (List<ClusterNode> partNodes : assignments)
                assignments0.add(F.isEmpty(partNodes) ? emptyList() : singletonList(F.first(partNodes).id()));
        }

        return ColocationGroup.forAssignments(assignments0);
    }

    /** */
    private ColocationGroup replicatedGroup(@NotNull AffinityTopologyVersion topVer) {
        GridCacheContext<?, ?> cctx = cacheContext();

        GridDhtPartitionTopology top = cctx.topology();

        List<ClusterNode> nodes = cctx.discovery().discoCache(topVer).cacheGroupAffinityNodes(cctx.groupId());
        List<UUID> nodes0;

        top.readLock();

        try {
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
        }
        finally {
            top.readUnlock();
        }

        return ColocationGroup.forNodes(nodes0);
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
    private static class KeyValDescriptor implements CacheColumnDescriptor {
        /** */
        private final GridQueryProperty desc;

        /** */
        private final String name;

        /** */
        private final boolean isKey;

        /** */
        private final int fieldIdx;

        /** */
        private final List<Class<?>> storageType;

        /** */
        private volatile RelDataType logicalType;

        /** */
        private KeyValDescriptor(String name, Class<?> type, boolean isKey, int fieldIdx) {
            this(name, Collections.singletonList(type), isKey, fieldIdx);

            assert !Collections.class.isAssignableFrom(type) && !Map.class.isAssignableFrom(type)
                : "Component types should be set for collection types";
        }

        /** */
        private KeyValDescriptor(String name, List<Class<?>> type, boolean isKey, int fieldIdx) {
            this.name = name;
            this.isKey = isKey;
            this.fieldIdx = fieldIdx;
            desc = null;

            storageType = type;
        }

        /** */
        private KeyValDescriptor(String name, GridQueryProperty desc, boolean isKey, int fieldIdx) {
            this.name = name;
            this.isKey = isKey;
            this.fieldIdx = fieldIdx;
            this.desc = desc;

            storageType = desc.type();
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
            if (logicalType == null) {
                logicalType = TypeUtils.sqlType(
                    f,
                    storageType,
                    desc != null && desc.precision() != -1 ? desc.precision() : PRECISION_NOT_SPECIFIED,
                    desc != null && desc.scale() != -1 ? desc.scale() : SCALE_NOT_SPECIFIED,
                    desc == null || !desc.notNull()
                );
            }

            return logicalType;
        }

        /** {@inheritDoc} */
        @Override public Class<?> storageType() {
            return storageType.get(0);
        }

        /** {@inheritDoc} */
        @Override public Object value(ExecutionContext<?> ectx, GridCacheContext<?, ?> cctx, CacheDataRow src) {
            return cctx.unwrapBinaryIfNeeded(isKey ? src.key() : src.value(), ectx.keepBinary(), null);
        }

        /** {@inheritDoc} */
        @Override public void set(Object dst, Object val) {
            throw new AssertionError();
        }
    }

    /** */
    private static class FieldDescriptor implements CacheColumnDescriptor {
        /** */
        private final GridQueryProperty desc;

        /** */
        private final int fieldIdx;

        /** */
        private volatile RelDataType logicalType;

        /** */
        private FieldDescriptor(GridQueryProperty desc, int fieldIdx) {
            this.desc = desc;
            this.fieldIdx = fieldIdx;
        }

        /** {@inheritDoc} */
        @Override public boolean field() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean key() {
            return desc.key();
        }

        /** {@inheritDoc} */
        @Override public boolean hasDefaultValue() {
            return desc.defaultValue() != null;
        }

        /** {@inheritDoc} */
        @Override public Object defaultValue() {
            return desc.defaultValue();
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
            if (logicalType == null) {
                logicalType = TypeUtils.sqlType(f, desc.type(),
                    desc.precision() == -1 ? PRECISION_NOT_SPECIFIED : desc.precision(),
                    desc.scale() == -1 ? SCALE_NOT_SPECIFIED : desc.scale(),
                    !desc.notNull()
                );
            }

            return logicalType;
        }

        /** {@inheritDoc} */
        @Override public Class<?> storageType() {
            return desc.type().get(0);
        }

        /** {@inheritDoc} */
        @Override public Object value(ExecutionContext<?> ectx, GridCacheContext<?, ?> cctx, CacheDataRow src)
            throws IgniteCheckedException {
            return cctx.unwrapBinaryIfNeeded(desc.value(src.key(), src.value()), ectx.keepBinary(), null);
        }

        /** {@inheritDoc} */
        @Override public void set(Object dst, Object val) throws IgniteCheckedException {
            final Object key0 = key() ? dst : null;
            final Object val0 = key() ? null : dst;

            desc.setValue(key0, val0, val);
        }
    }

    /** {@inheritDoc} */
    @Override public GridQueryTypeDescriptor typeDescription() {
        return typeDesc;
    }
}
