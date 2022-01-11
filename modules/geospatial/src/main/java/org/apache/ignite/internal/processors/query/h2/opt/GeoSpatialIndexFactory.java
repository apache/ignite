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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
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
            List<IndexColumn> cols = def.rowHandler().getH2IdxColumns();

            if (cols.size() > 1)
                throw DbException.getUnsupportedException("can only do one column");

            if ((cols.get(0).sortType & SortOrder.DESCENDING) != 0)
                throw DbException.getUnsupportedException("cannot do descending");

            if ((cols.get(0).sortType & SortOrder.NULLS_FIRST) != 0)
                throw DbException.getUnsupportedException("cannot do nulls first");

            if ((cols.get(0).sortType & SortOrder.NULLS_LAST) != 0)
                throw DbException.getUnsupportedException("cannot do nulls last");

            if (cols.get(0).column.getType() != Value.GEOMETRY) {
                throw DbException.getUnsupportedException("spatial index on non-geometry column, " +
                    cols.get(0).column.getCreateSQL());
            }

            return new GeoSpatialIndexImpl(cctx, def);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate: GridH2SpatialIndex", e);
        }
    }
}
