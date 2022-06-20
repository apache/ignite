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

package org.apache.ignite.internal.processors.query.schema.management;

import java.util.LinkedHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.QueryIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.client.ClientIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.client.ClientIndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexFactory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** Factory to create sorted index descriptors. */
public class SortedIndexDescriptorFactory extends AbstractIndexDescriptorFactory {
    /** */
    private static final InlineIndexFactory SORTED_IDX_FACTORY = InlineIndexFactory.INSTANCE;

    /** */
    private final IgniteLogger log;

    /** */
    public SortedIndexDescriptorFactory(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public IndexDescriptor create(
        GridKernalContext ctx,
        GridQueryIndexDescriptor idxDesc,
        TableDescriptor tbl,
        @Nullable SchemaIndexCacheVisitor cacheVisitor
    ) {
        GridCacheContextInfo<?, ?> cacheInfo = tbl.cacheInfo();
        GridQueryTypeDescriptor typeDesc = tbl.descriptor();
        String idxName = idxDesc.name();
        boolean isPk = QueryUtils.PRIMARY_KEY_INDEX.equals(idxName);
        boolean isAffKey = QueryUtils.AFFINITY_KEY_INDEX.equals(idxName);

        if (log.isDebugEnabled())
            log.debug("Creating cache index [cacheId=" + cacheInfo.cacheId() + ", idxName=" + idxName + ']');

        LinkedHashMap<String, IndexKeyDefinition> originalIdxCols = indexDescriptorToKeysDefinition(idxDesc, typeDesc);
        LinkedHashMap<String, IndexKeyDefinition> wrappedCols = new LinkedHashMap<>(originalIdxCols);

        // Enrich wrapped columns collection with key and affinity key.
        addKeyColumn(wrappedCols, tbl);
        addAffinityColumn(wrappedCols, tbl);

        LinkedHashMap<String, IndexKeyDefinition> unwrappedCols = new LinkedHashMap<>(originalIdxCols);

        // Enrich unwrapped columns collection with unwrapped key fields and affinity key.
        addUnwrappedKeyColumns(unwrappedCols, tbl);
        addAffinityColumn(unwrappedCols, tbl);

        LinkedHashMap<String, IndexKeyDefinition> idxCols = unwrappedCols;

        Index idx;

        if (cacheInfo.affinityNode()) {
            GridCacheContext<?, ?> cctx = cacheInfo.cacheContext();

            int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

            String treeName = BPlusTree.treeName(typeId + "_" + idxName, "H2Tree");

            if (!ctx.indexProcessor().useUnwrappedPk(cctx, treeName))
                idxCols = wrappedCols;

            QueryIndexDefinition idxDef = new QueryIndexDefinition(
                typeDesc,
                cacheInfo,
                new IndexName(cacheInfo.name(), typeDesc.schemaName(), typeDesc.tableName(), idxName),
                treeName,
                ctx.indexProcessor().rowCacheCleaner(cacheInfo.groupId()),
                isPk,
                isAffKey,
                idxCols,
                idxDesc.inlineSize(),
                ctx.indexProcessor().keyTypeSettings()
            );

            if (cacheVisitor != null)
                idx = ctx.indexProcessor().createIndexDynamically(cctx, SORTED_IDX_FACTORY, idxDef, cacheVisitor);
            else
                idx = ctx.indexProcessor().createIndex(cctx, SORTED_IDX_FACTORY, idxDef);
        }
        else {
            ClientIndexDefinition d = new ClientIndexDefinition(
                new IndexName(tbl.cacheInfo().name(), tbl.descriptor().schemaName(), tbl.descriptor().tableName(), idxName),
                idxCols,
                idxDesc.inlineSize(),
                tbl.cacheInfo().config().getSqlIndexMaxInlineSize());

            idx = ctx.indexProcessor().createIndex(tbl.cacheInfo().cacheContext(), new ClientIndexFactory(log), d);
        }

        assert idx instanceof InlineIndex : idx;

        return new IndexDescriptor(tbl, idxName, idxDesc.type(), idxCols, isPk, isAffKey,
            ((InlineIndex)idx).inlineSize(), idx);
    }

    /** Split key into simple components and add to columns list. */
    private static void addUnwrappedKeyColumns(LinkedHashMap<String, IndexKeyDefinition> cols, TableDescriptor tbl) {
        // Key unwrapping possible only for SQL created tables.
        if (!tbl.isSql() || QueryUtils.isSqlType(tbl.descriptor().keyClass())) {
            addKeyColumn(cols, tbl);

            return;
        }

        int oldColsSize = cols.size();

        if (!tbl.descriptor().primaryKeyFields().isEmpty()) {
            for (String keyName : tbl.descriptor().primaryKeyFields())
                cols.put(keyName, keyDefinition(tbl.descriptor(), keyName, true));
        }
        else {
            for (String propName : tbl.descriptor().fields().keySet()) {
                GridQueryProperty prop = tbl.descriptor().property(propName);

                if (prop.key())
                    cols.put(propName, keyDefinition(tbl.descriptor(), propName, true));
            }
        }

        // If key is object but the user has not specified any particular columns,
        // we have to fall back to whole-key index.
        if (cols.size() == oldColsSize)
            addKeyColumn(cols, tbl);
    }

    /** Add key column, if it (or it's alias) wasn't added before. */
    private static void addKeyColumn(LinkedHashMap<String, IndexKeyDefinition> cols, TableDescriptor tbl) {
        if (!cols.containsKey(QueryUtils.KEY_FIELD_NAME)
            && (F.isEmpty(tbl.descriptor().keyFieldName()) || !cols.containsKey(tbl.descriptor().keyFieldAlias())))
            cols.put(QueryUtils.KEY_FIELD_NAME, keyDefinition(tbl.descriptor(), QueryUtils.KEY_FIELD_NAME, true));
    }

    /** Add affinity column, if it wasn't added before. */
    private static void addAffinityColumn(LinkedHashMap<String, IndexKeyDefinition> cols, TableDescriptor tbl) {
        if (tbl.affinityKey() != null)
            cols.put(tbl.affinityKey(), keyDefinition(tbl.descriptor(), tbl.affinityKey(), true));
    }
}
