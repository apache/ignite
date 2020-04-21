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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridIndex;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.processors.query.schema.SchemaChangeListener;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.jetbrains.annotations.NotNull;

/**
 * Holds actual schema and mutates it on schema change, requested by Ignite.
 */
public class SchemaHolderImpl extends AbstractService implements SchemaHolder, SchemaChangeListener {

    /** */
    private final Map<String, IgniteSchema> igniteSchemas = new HashMap<>();

    /** */
    private GridInternalSubscriptionProcessor subscriptionProcessor;

    /** */
    private volatile SchemaPlus calciteSchema;

    /**
     * @param ctx Kernal context.
     */
    public SchemaHolderImpl(GridKernalContext ctx) {
        super(ctx);

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
    @Override public synchronized void onSchemaCreate(String schemaName) {
        igniteSchemas.putIfAbsent(schemaName, new IgniteSchema(schemaName));
        rebuild();
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSchemaDrop(String schemaName) {
        igniteSchemas.remove(schemaName);
        rebuild();
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSqlTypeCreate(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        GridIndex pk) {
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        String tblName = typeDesc.tableName();

        TableDescriptorImpl desc =
            new TableDescriptorImpl(cacheInfo.cacheContext(), typeDesc, affinityIdentity(cacheInfo));

        RelCollation pkCollation = RelCollations.of(new RelFieldCollation(QueryUtils.KEY_COL));

        IgniteTable tbl = new IgniteTable(tblName, desc, pkCollation);
        schema.addTable(tblName, tbl);

        IgniteIndex pkIdx = new IgniteIndex(pkCollation, IgniteTable.PK_INDEX_NAME, pk, tbl);
        tbl.addIndex(pkIdx);

        if (desc.keyField() != QueryUtils.KEY_COL) {
            RelCollation pkAliasCollation = RelCollations.of(new RelFieldCollation(desc.keyField()));
            IgniteIndex pkAliasIdx = new IgniteIndex(pkAliasCollation, IgniteTable.PK_ALIAS_INDEX_NAME, pk, tbl);
            tbl.addIndex(pkAliasIdx);
        }

        rebuild();
    }


    /** */
    private static Object affinityIdentity(GridCacheContextInfo<?, ?> cacheInfo) {
        return cacheInfo.config().getCacheMode() == CacheMode.PARTITIONED ?
            cacheInfo.cacheContext().group().affinity().similarAffinityKey() : null;
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSqlTypeDrop(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?,?> cacheInfo
    ) {
        IgniteSchema schema = igniteSchemas.computeIfAbsent(schemaName, IgniteSchema::new);

        schema.removeTable(typeDesc.tableName());

        rebuild();
    }

    /** {@inheritDoc} */
    @Override public synchronized void onIndexCreate(String schemaName, String tblName, String idxName,
        GridQueryIndexDescriptor idxDesc, GridIndex gridIdx) {
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
    @NotNull private static RelCollation deriveSecondaryIndexCollation(GridQueryIndexDescriptor idxDesc, IgniteTable tbl) {
        Map<String, ColumnDescriptor> tblFields = tbl.columnDescriptorsMap();

        List<RelFieldCollation> collations = new ArrayList<>(idxDesc.fields().size());

        for (String idxField : idxDesc.fields()) {
            ColumnDescriptor fieldDesc = tblFields.get(idxField);

            boolean descending = idxDesc.descending(idxField);
            int fieldIdx = fieldDesc.fieldIndex();

            RelFieldCollation collation = new RelFieldCollation(fieldIdx,
                descending ? RelFieldCollation.Direction.DESCENDING : RelFieldCollation.Direction.ASCENDING);

            collations.add(collation);
        }

        return RelCollations.of(collations);
    }

    /** {@inheritDoc} */
    @Override public synchronized void onIndexDrop(String schemaName, String tblName, String idxName) {
        IgniteSchema schema = igniteSchemas.get(schemaName);
        assert schema != null;

        IgniteTable tbl = (IgniteTable)schema.getTable(tblName);
        assert tbl != null;

        tbl.removeIndex(idxName);

        rebuild();
    }

    /** */
    private void rebuild() {
        SchemaPlus newCalciteSchema = Frameworks.createRootSchema(false);
        newCalciteSchema.add("PUBLIC", new IgniteSchema("PUBLIC"));
        igniteSchemas.forEach(newCalciteSchema::add);
        this.calciteSchema = newCalciteSchema;
    }
}
