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
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.IndexFactory;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.h2.message.DbException;
import org.jetbrains.annotations.Nullable;

/**
 * Index factory for creating geo spatial indexes.
 */
public class GeoSpatialIndexFactory implements IndexFactory {
    /** Instance of the factory. */
    public static final GeoSpatialIndexFactory INSTANCE = new GeoSpatialIndexFactory();

    /** {@inheritDoc} */
    @Override public Index createIndex(@Nullable GridCacheContext<?, ?> cctx, IndexDefinition definition) {
        GeoSpatialIndexDefinition def = (GeoSpatialIndexDefinition)definition;

        try {
            List<IndexKeyDefinition> keyDefs = def.rowHandler().indexKeyDefinitions();

            if (keyDefs.size() > 1)
                throw DbException.getUnsupportedException("can only do one column");

            if ((keyDefs.get(0).order().sortOrder() == SortOrder.DESC))
                throw DbException.getUnsupportedException("cannot do descending");

            if (keyDefs.get(0).order().nullsOrder() != null)
                throw DbException.getUnsupportedException("cannot do nulls ordering");

            if (keyDefs.get(0).idxType() != IndexKeyType.GEOMETRY)
                throw DbException.getUnsupportedException("spatial index on non-geometry column");

            return new GeoSpatialIndexImpl(cctx, def);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate: GridH2SpatialIndex", e);
        }
    }
}
