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
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.query.h2.index.QueryIndexKeyDefinitionProvider;
import org.apache.ignite.internal.processors.query.h2.index.QueryIndexRowHandler;
import org.h2.table.IndexColumn;
import org.locationtech.jts.geom.Geometry;

/**
 * This class is entrypoint for creating Geo-Spatial index.
 */
public class GeoSpatialUtils {
    /** Dummy key types. */
    private static final IndexKeyTypeSettings DUMMY_SETTINGS = new IndexKeyTypeSettings();

    static {
        IndexKeyFactory.register(IndexKeyTypes.GEOMETRY, k -> new GeometryIndexKey((Geometry)k));
    }

    /** */
    public static GridH2IndexBase createIndex(GridH2Table tbl, String idxName, List<IndexColumn> cols) {
        try {
            IndexName name = new IndexName(tbl.cacheName(), tbl.getSchema().getName(), tbl.getName(), idxName);

            LinkedHashMap<String, IndexKeyDefinition> keyDefs = new QueryIndexKeyDefinitionProvider(tbl, cols).keyDefinitions();

            if (tbl.cacheInfo().affinityNode())
                return createIndex(tbl, name, keyDefs, cols);
            else
                return createClientIndex(tbl, name, keyDefs, cols);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate", e);
        }
    }

    /** Creates index for server Ignite nodes. */
    private static GridH2SpatialIndex createIndex(
        GridH2Table tbl,
        IndexName name,
        LinkedHashMap<String, IndexKeyDefinition> keyDefs,
        List<IndexColumn> cols
    ) {
        List<InlineIndexKeyType> idxKeyTypes = InlineIndexKeyTypeRegistry.types(keyDefs.values(), DUMMY_SETTINGS);

        QueryIndexRowHandler rowHnd = new QueryIndexRowHandler(tbl, cols, keyDefs, idxKeyTypes, DUMMY_SETTINGS);

        final int segments = tbl.rowDescriptor().cacheInfo().config().getQueryParallelism();

        IndexDefinition def = new GeoSpatialIndexDefinition(name, keyDefs, rowHnd, segments);

        Index idx = tbl.idxProc().createIndex(tbl.cacheContext(), GeoSpatialIndexFactory.INSTANCE, def);

        return new GridH2SpatialIndex(idx.unwrap(GeoSpatialIndexImpl.class));
    }

    /** Creates index for client Ignite nodes. */
    private static GridH2SpatialClientIndex createClientIndex(
        GridH2Table tbl,
        IndexName name,
        LinkedHashMap<String, IndexKeyDefinition> keyDefs,
        List<IndexColumn> cols
    ) {
        IndexDefinition def = new GeoSpatialClientIndexDefinition(tbl, name, keyDefs, cols);

        Index idx = tbl.idxProc().createIndex(tbl.cacheContext(), GeoSpatialClientIndexFactory.INSTANCE, def);

        return new GridH2SpatialClientIndex(idx.unwrap(GeoSpatialClientIndex.class));
    }
}
