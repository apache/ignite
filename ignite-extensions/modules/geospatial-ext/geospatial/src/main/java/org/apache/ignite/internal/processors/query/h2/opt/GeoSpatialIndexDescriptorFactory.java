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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.IndexFactory;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.QueryIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.management.AbstractIndexDescriptorFactory;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Index descriptor factory for creating geo spatial indexes.
 */
public class GeoSpatialIndexDescriptorFactory extends AbstractIndexDescriptorFactory {
    /** Dummy key types. */
    private static final IndexKeyTypeSettings DUMMY_SETTINGS = new IndexKeyTypeSettings();

    /** Instance of the factory. */
    public static final GeoSpatialIndexDescriptorFactory INSTANCE = new GeoSpatialIndexDescriptorFactory();

    /** {@inheritDoc} */
    @Override public IndexDescriptor create(
        GridKernalContext ctx,
        GridQueryIndexDescriptor idxDesc,
        TableDescriptor tbl,
        @Nullable SchemaIndexCacheVisitor cacheVisitor
    ) {
        GridCacheContextInfo<?, ?> cacheInfo = tbl.cacheInfo();
        GridQueryTypeDescriptor typeDesc = tbl.type();
        LinkedHashMap<String, IndexKeyDefinition> keyDefs = indexDescriptorToKeysDefinition(idxDesc, typeDesc);
        IndexName name = new IndexName(typeDesc.cacheName(), typeDesc.schemaName(), typeDesc.tableName(), idxDesc.name());

        IndexDefinition idxDef;
        IndexFactory idxFactory;

        if (cacheInfo.affinityNode()) {
            idxFactory = GeoSpatialIndexFactory.INSTANCE;
            List<InlineIndexKeyType> idxKeyTypes = InlineIndexKeyTypeRegistry.types(keyDefs.values(), DUMMY_SETTINGS);

            QueryIndexRowHandler rowHnd = new QueryIndexRowHandler(
                tbl.type(), tbl.cacheInfo(), keyDefs, idxKeyTypes, DUMMY_SETTINGS);

            final int segments = tbl.cacheInfo().config().getQueryParallelism();

            idxDef = new GeoSpatialIndexDefinition(name, keyDefs, rowHnd, segments);
        }
        else {
            idxFactory = GeoSpatialClientIndexFactory.INSTANCE;

            idxDef = new GeoSpatialClientIndexDefinition(name, keyDefs);
        }

        Index idx = ctx.indexProcessor().createIndex(cacheInfo.cacheContext(), idxFactory, idxDef);

        return new IndexDescriptor(tbl, idxDesc.name(), idxDesc.type(), keyDefs, false, false, -1, idx);
    }
}
