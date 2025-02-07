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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteScalarFunction;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.IgniteTableFunction;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Holds actual schema and mutates it on schema change, requested by Ignite.
 */
public class SchemaHolderImpl extends AbstractService implements SchemaHolder, SchemaChangeListener {
    /** */
    private final Map<String, IgniteSchema> igniteSchemas = new HashMap<>();

    /** */
    private final GridKernalContext ctx;

    /** */
    private final FrameworkConfig frameworkCfg;

    /** */
    private GridInternalSubscriptionProcessor subscriptionProcessor;

    /** */
    private volatile SchemaPlus calciteSchema;

    /** */
    private static class AffinityIdentity {
        /** */
        private final Class<?> affFuncCls;

        /** */
        private final int backups;

        /** */
        private final int partsCnt;

        /** */
        private final Class<?> filterCls;

        /** */
        private final int hash;

        /** */
        public AffinityIdentity(AffinityFunction aff, int backups, IgnitePredicate<ClusterNode> nodeFilter) {
            affFuncCls = aff.getClass();

            this.backups = backups;

            partsCnt = aff.partitions();

            filterCls = nodeFilter == null ? CacheConfiguration.IgniteAllNodesPredicate.class : nodeFilter.getClass();

            int hash = backups;
            hash = 31 * hash + affFuncCls.hashCode();
            hash = 31 * hash + filterCls.hashCode();
            hash = 31 * hash + partsCnt;

            this.hash = hash;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (o == this)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            AffinityIdentity identity = (AffinityIdentity)o;

            return backups == identity.backups &&
                affFuncCls == identity.affFuncCls &&
                filterCls == identity.filterCls &&
                partsCnt == identity.partsCnt;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AffinityIdentity.class, this);
        }
    }

    /**
     * @param ctx Kernal context.
     */
    public SchemaHolderImpl(GridKernalContext ctx, FrameworkConfig frameworkCfg) {
        super(ctx);

        this.ctx = ctx;
        this.frameworkCfg = frameworkCfg;

        subscriptionProcessor(ctx.internalSubscriptionProcessor());

        init();
    }

    /**
     * @param subscriptionProcessor Subscription processor.
     */
    public void subscriptionProcessor(GridInternalSubscriptionProcessor subscriptionProcessor) {
        this.subscriptionProcessor = subscriptionProcessor;
    }

    /** {@inheritDoc} */
    @Override public void init() {
        subscriptionProcessor.registerSchemaChangeListener(this);
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSchemaCreated(String schemaName) {
        igniteSchemas.putIfAbsent(schemaName, new IgniteSchema(schemaName));
        rebuild();
    }

    /** {@inheritDoc} */
    @Override public void onSchemaDropped(String schemaName) {
        igniteSchemas.remove(schemaName);
        rebuild();
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeCreated(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo
    ) {
        publishTable(schemaName, typeDesc.tableName(), createTable(typeDesc, cacheInfo));
    }

    /** {@inheritDoc} */
    @Override public void onColumnsAdded(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        List<QueryField> cols
    ) {
        IgniteCacheTable oldTbl = table(schemaName, typeDesc.tableName());
        assert oldTbl != null;

        IgniteCacheTable newTbl = createTable(typeDesc, cacheInfo);

        // Recreate indexes for the new table without columns shift.
        for (IgniteIndex idx : oldTbl.indexes().values()) {
            CacheIndexImpl idx0 = (CacheIndexImpl)idx;

            newTbl.addIndex(new CacheIndexImpl(idx0.collation(), idx0.name(), idx0.queryIndex(), newTbl));
        }

        publishTable(schemaName, typeDesc.tableName(), newTbl);
    }

    /** {@inheritDoc} */
    @Override public void onColumnsDropped(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        List<String> cols
    ) {
        IgniteCacheTable oldTbl = table(schemaName, typeDesc.tableName());
        assert oldTbl != null;

        IgniteCacheTable newTbl = createTable(typeDesc, cacheInfo);

        // Recreate indexes for the new table with columns shift.
        int colsCnt = oldTbl.descriptor().columnDescriptors().size();
        ImmutableBitSet.Builder retainedCols = ImmutableBitSet.builder();
        retainedCols.set(0, colsCnt);

        for (String droppedCol : cols)
            retainedCols.clear(oldTbl.descriptor().columnDescriptor(droppedCol).fieldIndex());

        Mappings.TargetMapping mapping = Commons.mapping(retainedCols.build(), colsCnt);

        for (IgniteIndex idx : oldTbl.indexes().values()) {
            CacheIndexImpl idx0 = (CacheIndexImpl)idx;

            newTbl.addIndex(new CacheIndexImpl(RelCollations.permute(idx0.collation(), mapping), idx0.name(),
                idx0.queryIndex(), newTbl));
        }

        publishTable(schemaName, typeDesc.tableName(), newTbl);
    }

    /** */
    private IgniteCacheTable createTable(
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo
    ) {
        CacheTableDescriptorImpl desc =
            new CacheTableDescriptorImpl(cacheInfo, typeDesc, affinityIdentity(cacheInfo.config()));

        return new CacheTableImpl(ctx, desc);
    }

    /** */
    private void publishTable(
        String schemaName,
        String tblName,
        IgniteTable tbl
    ) {
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        schema.addTable(tblName, tbl);

        rebuild();
    }

    /** */
    private static Object affinityIdentity(CacheConfiguration<?, ?> ccfg) {
        if (ccfg.getCacheMode() == CacheMode.PARTITIONED)
            return new AffinityIdentity(ccfg.getAffinity(), ccfg.getBackups(), ccfg.getNodeFilter());
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeDropped(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        boolean destroy
    ) {
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        schema.removeTable(typeDesc.tableName());

        rebuild();
    }

    /** {@inheritDoc} */
    @Override public void onIndexCreated(
        String schemaName,
        String tblName,
        String idxName,
        IndexDescriptor idxDesc
    ) {
        IgniteCacheTable tbl = table(schemaName, tblName);
        assert tbl != null;

        RelCollation idxCollation = deriveSecondaryIndexCollation(idxDesc, tbl);

        IgniteIndex idx = new CacheIndexImpl(idxCollation, idxName, idxDesc.index(), tbl);
        tbl.addIndex(idx);
    }

    /**
     * @return Index collation.
     */
    @NotNull private static RelCollation deriveSecondaryIndexCollation(
        IndexDescriptor idxDesc,
        IgniteCacheTable tbl
    ) {
        CacheTableDescriptor tblDesc = tbl.descriptor();
        List<RelFieldCollation> collations = new ArrayList<>(idxDesc.keyDefinitions().size());

        for (Map.Entry<String, IndexKeyDefinition> keyDef : idxDesc.keyDefinitions().entrySet()) {
            ColumnDescriptor fieldDesc = tblDesc.columnDescriptor(keyDef.getKey());

            assert fieldDesc != null;

            boolean descending = keyDef.getValue().order().sortOrder() == SortOrder.DESC;
            int fieldIdx = fieldDesc.fieldIndex();

            collations.add(TraitUtils.createFieldCollation(fieldIdx, !descending));
        }

        return RelCollations.of(collations);
    }

    /** {@inheritDoc} */
    @Override public void onIndexDropped(String schemaName, String tblName, String idxName) {
        IgniteTable tbl = table(schemaName, tblName);
        assert tbl != null;

        tbl.removeIndex(idxName);

        rebuild();
    }

    /** {@inheritDoc} */
    @Override public void onIndexRebuildStarted(String schemaName, String tblName) {
        IgniteTable tbl = table(schemaName, tblName);
        assert tbl != null;

        tbl.markIndexRebuildInProgress(true);
    }

    /** {@inheritDoc} */
    @Override public void onIndexRebuildFinished(String schemaName, String tblName) {
        IgniteTable tbl = table(schemaName, tblName);
        assert tbl != null;

        tbl.markIndexRebuildInProgress(false);
    }

    /** {@inheritDoc} */
    @Override public void onFunctionCreated(String schemaName, String name, boolean deterministic, Method method) {
        if (!checkNewUserDefinedFunction(schemaName, name))
            return;

        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        schema.addFunction(name.toUpperCase(), IgniteScalarFunction.create(method));

        rebuild();
    }

    /** {@inheritDoc} */
    @Override public void onTableFunctionCreated(
        String schemaName,
        String name,
        Method method,
        Class<?>[] colTypes,
        String[] colNames
    ) {
        if (!checkNewUserDefinedFunction(schemaName, name))
            return;

        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        schema.addFunction(name.toUpperCase(), IgniteTableFunction.create(method, colTypes, colNames));

        rebuild();
    }

    /** */
    private boolean checkNewUserDefinedFunction(String schName, String funName) {
        if (F.eq(schName, QueryUtils.DFLT_SCHEMA)) {
            List<SqlOperator> operators = new ArrayList<>();

            frameworkCfg.getOperatorTable().lookupOperatorOverloads(
                new SqlIdentifier(funName, SqlParserPos.ZERO),
                null,
                SqlSyntax.FUNCTION,
                operators,
                SqlNameMatchers.withCaseSensitive(false)
            );

            if (!operators.isEmpty()) {
                log.error("Unable to add user-defined SQL function '" + funName + "'. Default schema '" + QueryUtils.DFLT_SCHEMA +
                    "' already has a standard function with the same name.");

                return false;
            }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void onSystemViewCreated(String schemaName, SystemView<?> sysView) {
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        SystemViewTableDescriptorImpl<?> desc = new SystemViewTableDescriptorImpl<>(sysView);

        schema.addTable(desc.name(), new SystemViewTableImpl(desc));

        rebuild();
    }

    /** {@inheritDoc} */
    @Override public void onViewCreated(String schemaName, String viewName, String viewSql) {
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        schema.addView(viewName, viewSql);

        rebuild();
    }

    /** {@inheritDoc} */
    @Override public void onViewDropped(String schemaName, String viewName) {
        IgniteSchema schema = igniteSchemas.get(schemaName);

        if (schema != null) {
            schema.removeView(viewName);

            rebuild();
        }
    }

    /** {@inheritDoc} */
    @Override public SchemaPlus schema(@Nullable String schema) {
        return schema != null ? calciteSchema.getSubSchema(schema) : calciteSchema;
    }

    /** */
    private IgniteCacheTable table(String schemaName, String tableName) {
        IgniteSchema schema = igniteSchemas.get(schemaName);

        if (schema != null)
            return (IgniteCacheTable)schema.getTable(tableName);

        return null;
    }

    /** */
    private void rebuild() {
        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);

        newCalciteSchema.add("UUID", typeFactory -> ((IgniteTypeFactory)typeFactory).createCustomType(UUID.class));
        newCalciteSchema.add("OTHER", typeFactory -> ((IgniteTypeFactory)typeFactory).createCustomType(Object.class));
        newCalciteSchema.add(QueryUtils.DFLT_SCHEMA, new IgniteSchema(QueryUtils.DFLT_SCHEMA));

        for (IgniteSchema schema : igniteSchemas.values())
            schema.register(newCalciteSchema, frameworkCfg);

        calciteSchema = newCalciteSchema;
    }
}
