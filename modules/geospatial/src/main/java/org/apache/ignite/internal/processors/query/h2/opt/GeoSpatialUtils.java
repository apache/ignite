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

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.cache.query.index.IndexDefinition;
import org.apache.ignite.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.processors.query.h2.index.QueryIndexKeyDefinitionProvider;
import org.apache.ignite.internal.processors.query.h2.index.QueryIndexRowHandler;
import org.h2.table.IndexColumn;

/**
 * This class is entrypoint for creating geo spatial index.
 */
public class GeoSpatialUtils {
    /** */
    public static GridH2SpatialIndex createIndex(GridH2Table tbl, String idxName, List<IndexColumn> cols) {
        try {
            IndexName name = new IndexName(tbl.cacheName(), tbl.getSchema().getName(), tbl.getName(), idxName);

            QueryIndexKeyDefinitionProvider keyProvider = new QueryIndexKeyDefinitionProvider(tbl, cols);

            List<InlineIndexKeyType> idxKeyType = keyProvider.getTypes();

            QueryIndexRowHandler rowHnd = new QueryIndexRowHandler(tbl, cols, keyProvider.get(), idxKeyType);

            final int segments = tbl.rowDescriptor().cacheInfo().config().getQueryParallelism();

            IndexDefinition def = new GeoSpatialIndexDefinition(name, rowHnd, segments);

            Index idx = tbl.cacheContext().kernalContext().indexing().createIndex(
                tbl.cacheContext(), GeoSpatialIndexFactory.INSTANCE, def);

            return new GridH2SpatialIndex(idx.unwrap(GeoSpatialIndexImpl.class));
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate", e);
        }
    }
}
