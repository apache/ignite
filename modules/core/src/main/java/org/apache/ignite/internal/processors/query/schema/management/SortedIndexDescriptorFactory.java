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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.QueryIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.client.ClientIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.client.ClientIndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
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
    public static final String H2_TREE = "H2Tree";

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
        GridQueryTypeDescriptor typeDesc = tbl.type();
        String idxName = idxDesc.name();
        boolean isPk = QueryUtils.PRIMARY_KEY_INDEX.equals(idxName);
        boolean isAff = QueryUtils.AFFINITY_KEY_INDEX.equals(idxName);

        if (log.isDebugEnabled())
            log.debug("Creating cache index [cacheId=" + cacheInfo.cacheId() + ", idxName=" + idxName + ']');

        LinkedHashMap<String, IndexKeyDefinition> originalIdxCols = indexDescriptorToKeysDefinition(idxDesc, typeDesc);
        LinkedHashMap<String, IndexKeyDefinition> wrappedCols = new LinkedHashMap<>(originalIdxCols);

        // Columns conditions below is caused by legacy implementation, to maintain backward compatibility.

        // Enrich wrapped columns collection with key and affinity key.
        if (isAff || F.isEmpty(tbl.type().keyFieldName()) || !wrappedCols.containsKey(tbl.type().keyFieldAlias()))
            addKeyColumn(wrappedCols, tbl);

        if (!(isPk && QueryUtils.KEY_FIELD_NAME.equals(tbl.affinityKey())))
            addAffinityColumn(wrappedCols, tbl);

        LinkedHashMap<String, IndexKeyDefinition> unwrappedCols = new LinkedHashMap<>(originalIdxCols);

        // Enrich unwrapped columns collection with unwrapped key fields and affinity key.
        addUnwrappedKeyColumns(unwrappedCols, tbl);

        if (!(isPk && QueryUtils.KEY_FIELD_NAME.equals(tbl.affinityKey())))
            addAffinityColumn(unwrappedCols, tbl);

        LinkedHashMap<String, IndexKeyDefinition> idxCols = unwrappedCols;
        IndexName idxFullName = new IndexName(cacheInfo.name(), typeDesc.schemaName(), typeDesc.tableName(), idxName);

        Index idx;
        int inlineSize;

        if (cacheInfo.affinityNode()) {
            GridCacheContext<?, ?> cctx = cacheInfo.cacheContext();

            int typeId = cctx.binaryMarshaller() ? typeDesc.typeId() : typeDesc.valueClass().hashCode();

            String treeName = BPlusTree.treeName(typeId + "_" + idxName, H2_TREE);

            if (!ctx.indexProcessor().useUnwrappedPk(cctx, treeName))
                idxCols = wrappedCols;

            QueryIndexDefinition idxDef = new QueryIndexDefinition(
                typeDesc,
                cacheInfo,
                idxFullName,
                treeName,
                ctx.indexProcessor().rowCacheCleaner(cacheInfo.groupId()),
                isPk,
                isAff,
                idxCols,
                idxDesc.inlineSize(),
                ctx.indexProcessor().keyTypeSettings()
            );

            if (cacheVisitor != null)
                idx = ctx.indexProcessor().createIndexDynamically(cctx, SORTED_IDX_FACTORY, idxDef, cacheVisitor);
            else
                idx = ctx.indexProcessor().createIndex(cctx, SORTED_IDX_FACTORY, idxDef);

            assert idx instanceof InlineIndex : idx;

            inlineSize = ((InlineIndex)idx).inlineSize();
        }
        else {
            ClientIndexDefinition def = new ClientIndexDefinition(idxFullName, idxCols);

            idx = ctx.indexProcessor().createIndex(cacheInfo.cacheContext(), new ClientIndexFactory(), def);

            // Here inline size is just for information (to be shown in system view).
            inlineSize = InlineIndexTree.computeInlineSize(
                idxFullName.fullName(),
                InlineIndexKeyTypeRegistry.types(idxCols.values(), new IndexKeyTypeSettings()),
                new ArrayList<>(idxCols.values()),
                idxDesc.inlineSize(),
                tbl.cacheInfo().config().getSqlIndexMaxInlineSize(),
                log
            );
        }

        return new IndexDescriptor(tbl, idxName, idxDesc.type(), idxCols, isPk, isAff, inlineSize, idx);
    }

    /** Split key into simple components and add to columns list. */
    private static void addUnwrappedKeyColumns(LinkedHashMap<String, IndexKeyDefinition> cols, TableDescriptor tbl) {
        // Key unwrapping possible only for SQL created tables.
        if (!tbl.isSql() || QueryUtils.isSqlType(tbl.type().keyClass())) {
            addKeyColumn(cols, tbl);

            return;
        }

        if (!tbl.type().primaryKeyFields().isEmpty()) {
            for (String keyName : tbl.type().primaryKeyFields())
                cols.putIfAbsent(keyName, keyDefinition(tbl.type(), keyName, true));
        }
        else {
            boolean haveKeyFields = false;

            for (String propName : tbl.type().fields().keySet()) {
                GridQueryProperty prop = tbl.type().property(propName);

                if (prop.key()) {
                    cols.putIfAbsent(propName, keyDefinition(tbl.type(), propName, true));
                    haveKeyFields = true;
                }
            }

            // If key is object but the user has not specified any particular columns,
            // we have to fall back to whole-key index.
            if (!haveKeyFields)
                addKeyColumn(cols, tbl);
        }
    }

    /** Add key column, if it (or it's alias) wasn't added before. */
    private static void addKeyColumn(LinkedHashMap<String, IndexKeyDefinition> cols, TableDescriptor tbl) {
        cols.putIfAbsent(QueryUtils.KEY_FIELD_NAME, keyDefinition(tbl.type(), QueryUtils.KEY_FIELD_NAME, true));
    }

    /** Add affinity column, if it wasn't added before. */
    private static void addAffinityColumn(LinkedHashMap<String, IndexKeyDefinition> cols, TableDescriptor tbl) {
        if (tbl.affinityKey() != null)
            cols.putIfAbsent(tbl.affinityKey(), keyDefinition(tbl.type(), tbl.affinityKey(), true));
    }
}
