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

package org.apache.ignite.internal.processors.query.calcite.planner;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.client.ClientIndex;
import org.apache.ignite.internal.cache.query.index.sorted.client.ClientIndexDefinition;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.MappingQueryContext;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheIndexImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteStatisticsImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.ModifyTuple;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.stat.ObjectStatisticsImpl;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;
import org.mockito.Mockito;

import static org.apache.ignite.internal.processors.query.calcite.planner.AbstractPlannerTest.DEFAULT_SCHEMA;

/** */
public class TestTable implements IgniteCacheTable, Wrapper {
    /** */
    private final String name;

    /** */
    private final RelProtoDataType protoType;

    /** */
    private final Map<String, IgniteIndex> indexes = new HashMap<>();

    /** */
    private IgniteDistribution distribution;

    /** */
    private ColocationGroup colocationGrp;

    /** */
    private IgniteStatisticsImpl statistics;

    /** */
    private final CacheTableDescriptor desc;

    /** */
    private volatile boolean idxRebuildInProgress;

    /** */
    protected TestTable(RelDataType type) {
        this(type, 100.0);
    }

    /** */
    protected TestTable(RelDataType type, double rowCnt) {
        this(UUID.randomUUID().toString(), type, rowCnt);
    }

    /** */
    protected TestTable(String name, RelDataType type, double rowCnt) {
        protoType = RelDataTypeImpl.proto(type);
        statistics = new IgniteStatisticsImpl(new ObjectStatisticsImpl((long)rowCnt, Collections.emptyMap()));
        this.name = name;

        desc = new TestTableDescriptor(this::distribution, type);
    }

    /**
     * Set table distribution.
     *
     * @param distribution Table distribution to set.
     * @return TestTable for chaining.
     */
    public TestTable setDistribution(IgniteDistribution distribution) {
        this.distribution = distribution;

        return this;
    }

    /**
     * Set colocation group.
     *
     * @param colocationGrp Colocation group to set.
     * @return TestTable for chaining.
     */
    public TestTable setColocationGroup(ColocationGroup colocationGrp) {
        this.colocationGrp = colocationGrp;

        return this;
    }

    /**
     * Set table statistics;
     *
     * @param statistics Statistics to set.
     * @return TestTable for chaining.
     */
    public TestTable setStatistics(IgniteStatisticsImpl statistics) {
        this.statistics = statistics;

        return this;
    }

    /** {@inheritDoc} */
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory, ImmutableBitSet bitSet) {
        RelDataType rowType = protoType.apply(typeFactory);

        if (bitSet != null) {
            RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(typeFactory);
            for (int i = bitSet.nextSetBit(0); i != -1; i = bitSet.nextSetBit(i + 1))
                b.add(rowType.getFieldList().get(i));
            rowType = b.build();
        }

        return rowType;
    }

    /** {@inheritDoc} */
    @Override public Statistic getStatistic() {
        return statistics;
    }

    /** {@inheritDoc} */
    @Override public <Row> Iterable<Row> scan(
        ExecutionContext<Row> execCtx,
        ColocationGroup grp,
        ImmutableBitSet bitSet
    ) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public Schema.TableType getJdbcTableType() {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public boolean isRolledUp(String col) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean rolledUpColumnValidInsideAgg(
        String column,
        SqlCall call,
        SqlNode parent,
        CalciteConnectionConfig cfg
    ) {
        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
        if (colocationGrp != null)
            return colocationGrp;

        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public IgniteDistribution distribution() {
        if (distribution != null)
            return distribution;

        throw new AssertionError();
    }

    /** {@inheritDoc} */
    @Override public CacheTableDescriptor descriptor() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public Map<String, IgniteIndex> indexes() {
        return Collections.unmodifiableMap(indexes);
    }

    /** {@inheritDoc} */
    @Override public void addIndex(IgniteIndex idxTbl) {
        indexes.put(idxTbl.name(), idxTbl);
    }

    /** */
    public TestTable addIndex(RelCollation collation, String name) {
        LinkedHashMap<String, IndexKeyDefinition> keyDefs = new LinkedHashMap<>();

        RelDataType rowType = protoType.apply(Commons.typeFactory());

        for (RelFieldCollation fc : collation.getFieldCollations()) {
            RelDataTypeField field = rowType.getFieldList().get(fc.getFieldIndex());

            Type fieldType = Commons.typeFactory().getResultClass(field.getType());

            // For some reason IndexKeyType.forClass throw an exception for char classes, but we have such
            // classes in tests.
            IndexKeyType keyType = (fieldType == Character.class || fieldType == char.class) ? IndexKeyType.STRING_FIXED :
                fieldType instanceof Class ? IndexKeyType.forClass((Class<?>)fieldType) : IndexKeyType.UNKNOWN;

            keyDefs.put(field.getName(), new IndexKeyDefinition(keyType.code(), -1, !fc.direction.isDescending()));
        }

        IndexDefinition idxDef = new ClientIndexDefinition(
            new IndexName(QueryUtils.createTableCacheName(DEFAULT_SCHEMA, this.name), DEFAULT_SCHEMA, this.name, name),
            keyDefs
        );

        indexes.put(name, new CacheIndexImpl(collation, name, new ClientIndex(idxDef), this));

        return this;
    }

    /** */
    public TestTable addIndex(String name, int... keys) {
        addIndex(TraitUtils.createCollation(Arrays.stream(keys).boxed().collect(Collectors.toList())), name);

        return this;
    }

    /** {@inheritDoc} */
    @Override public IgniteIndex getIndex(String idxName) {
        return indexes.get(idxName);
    }

    /** {@inheritDoc} */
    @Override public void removeIndex(String idxName) {
        indexes.remove(idxName);
    }

    /** {@inheritDoc} */
    @Override public void ensureCacheStarted() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isModifiable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markIndexRebuildInProgress(boolean mark) {
        idxRebuildInProgress = mark;
    }

    /** {@inheritDoc} */
    @Override public boolean isIndexRebuildInProgress() {
        return idxRebuildInProgress;
    }

    /** */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public void authorize(Operation op) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public <C> C unwrap(Class<C> cls) {
        if (cls.isInstance(this))
            return cls.cast(this);

        if (cls.isInstance(desc))
            return cls.cast(desc);

        return null;
    }

    /** */
    static class TestTableDescriptor extends NullInitializerExpressionFactory implements CacheTableDescriptor {
        /** */
        private final Supplier<IgniteDistribution> distributionSupp;

        /** */
        private final RelDataType rowType;

        /** */
        private final GridCacheContextInfo<?, ?> cacheInfo;

        /** */
        public TestTableDescriptor(Supplier<IgniteDistribution> distribution, RelDataType rowType) {
            distributionSupp = distribution;
            this.rowType = rowType;
            cacheInfo = Mockito.mock(GridCacheContextInfo.class);

            CacheConfiguration cfg = Mockito.mock(CacheConfiguration.class);
            Mockito.when(cfg.isEagerTtl()).thenReturn(true);

            Mockito.when(cacheInfo.cacheId()).thenReturn(CU.cacheId("TEST"));
            Mockito.when(cacheInfo.config()).thenReturn(cfg);
        }

        /** {@inheritDoc} */
        @Override public GridCacheContextInfo<?, ?> cacheInfo() {
            return cacheInfo;
        }

        /** {@inheritDoc} */
        @Override public GridCacheContext<?, ?> cacheContext() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public IgniteDistribution distribution() {
            return distributionSupp.get();
        }

        /** {@inheritDoc} */
        @Override public ColocationGroup colocationGroup(MappingQueryContext ctx) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public RelDataType rowType(IgniteTypeFactory factory, @Nullable ImmutableBitSet usedColumns) {
            if (usedColumns == null)
                return rowType;
            else {
                RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(factory);

                for (int i = usedColumns.nextSetBit(0); i != -1; i = usedColumns.nextSetBit(i + 1))
                    b.add(rowType.getFieldList().get(i));

                return b.build();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isUpdateAllowed(RelOptTable tbl, int colIdx) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean match(CacheDataRow row) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public <Row> Row toRow(
            ExecutionContext<Row> ectx,
            CacheDataRow cacheDataRow,
            Row outputRow,
            int[] fieldColMapping
        ) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public <Row> ModifyTuple toTuple(ExecutionContext<Row> ectx, Row row, TableModify.Operation op,
            @Nullable Object arg) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public ColumnDescriptor columnDescriptor(String fieldName) {
            RelDataTypeField field = rowType.getField(fieldName, false, false);
            return new TestColumnDescriptor(field.getIndex(), fieldName);
        }

        /** {@inheritDoc} */
        @Override public Collection<ColumnDescriptor> columnDescriptors() {
            return Commons.transform(rowType.getFieldList(), f -> new TestColumnDescriptor(f.getIndex(), f.getName()));
        }

        /** {@inheritDoc} */
        @Override public GridQueryTypeDescriptor typeDescription() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public RelDataType insertRowType(IgniteTypeFactory factory) {
            ImmutableBitSet.Builder bitSetBuilder = ImmutableBitSet.builder();

            for (int i = 0; i < rowType.getFieldCount(); i++) {
                String fldName = rowType.getFieldList().get(i).getName();

                if (!QueryUtils.KEY_FIELD_NAME.equals(fldName) && !QueryUtils.VAL_FIELD_NAME.equals(fldName))
                    bitSetBuilder.set(i);
            }

            return rowType(factory, bitSetBuilder.build());
        }
    }

    /** */
    static class TestColumnDescriptor implements ColumnDescriptor {
        /** */
        private final int idx;

        /** */
        private final String name;

        /** */
        public TestColumnDescriptor(int idx, String name) {
            this.idx = idx;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean hasDefaultValue() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public int fieldIndex() {
            return idx;
        }

        /** {@inheritDoc} */
        @Override public RelDataType logicalType(IgniteTypeFactory f) {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public Class<?> storageType() {
            throw new AssertionError();
        }

        /** {@inheritDoc} */
        @Override public Object defaultValue() {
            throw new AssertionError();
        }
    }
}
