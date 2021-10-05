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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
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
    public SchemaHolderImpl(GridKernalContext ctx) {
        super(ctx);

        this.ctx = ctx;

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
    @Override public SchemaPlus schema() {
        return calciteSchema;
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSchemaCreated(String schemaName) {
        igniteSchemas.putIfAbsent(schemaName, new IgniteSchema(schemaName));
        rebuild();
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSchemaDropped(String schemaName) {
        igniteSchemas.remove(schemaName);
        rebuild();
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSqlTypeCreated(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo
    ) {
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        String tblName = typeDesc.tableName();

        TableDescriptorImpl desc =
            new TableDescriptorImpl(cacheInfo, typeDesc, affinityIdentity(cacheInfo.config()));

        schema.addTable(tblName, new IgniteTableImpl(ctx, desc));

        rebuild();
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeUpdated(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo
    ) {
        onSqlTypeCreated(schemaName, typeDesc, cacheInfo);
    }

    /** */
    private static Object affinityIdentity(CacheConfiguration<?, ?> ccfg) {
        if (ccfg.getCacheMode() == CacheMode.PARTITIONED)
            return new AffinityIdentity(ccfg.getAffinity(), ccfg.getBackups(), ccfg.getNodeFilter());
        return null;
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSqlTypeDropped(
        String schemaName,
        GridQueryTypeDescriptor typeDesc
    ) {
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        schema.removeTable(typeDesc.tableName());

        rebuild();
    }

    /** {@inheritDoc} */
    @Override public synchronized void onIndexCreated(String schemaName, String tblName, String idxName,
        GridQueryIndexDescriptor idxDesc, @Nullable Index gridIdx) {
        IgniteSchema schema = igniteSchemas.get(schemaName);
        assert schema != null;

        IgniteTable tbl = (IgniteTable)schema.getTable(tblName);
        assert tbl != null;

        RelCollation idxCollation = deriveSecondaryIndexCollation(idxDesc, tbl);

        IgniteIndex idx = new IgniteIndex(idxCollation, idxName, gridIdx, tbl);
        tbl.addIndex(idx);
    }

    /**
     * @return Index collation.
     */
    @NotNull private static RelCollation deriveSecondaryIndexCollation(
        GridQueryIndexDescriptor idxDesc,
        IgniteTable tbl
    ) {
        TableDescriptor tblDesc = tbl.descriptor();
        List<RelFieldCollation> collations = new ArrayList<>(idxDesc.fields().size());

        for (String idxField : idxDesc.fields()) {
            ColumnDescriptor fieldDesc = tblDesc.columnDescriptor(idxField);

            assert fieldDesc != null;

            boolean descending = idxDesc.descending(idxField);
            int fieldIdx = fieldDesc.fieldIndex();

            collations.add(
                createFieldCollation(fieldIdx, !descending)
            );
        }

        return RelCollations.of(collations);
    }

    /** {@inheritDoc} */
    @Override public synchronized void onIndexDropped(String schemaName, String tblName, String idxName) {
        IgniteSchema schema = igniteSchemas.get(schemaName);
        assert schema != null;

        IgniteTable tbl = (IgniteTable)schema.getTable(tblName);
        assert tbl != null;

        tbl.removeIndex(idxName);

        rebuild();
    }

    /** {@inheritDoc} */
    @Override public SchemaPlus getDefaultSchema(String schema) {
        return schema != null ? schema().getSubSchema(schema) : schema();
    }

    /** */
    private void rebuild() {
        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);
        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));
        igniteSchemas.forEach(newCalciteSchema::add);
        calciteSchema = newCalciteSchema;
    }

    /** */
    private static RelFieldCollation createFieldCollation(int fieldIdx, boolean asc) {
        return asc
            ? new RelFieldCollation(fieldIdx, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST)
            : new RelFieldCollation(fieldIdx, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST);
    }
}
