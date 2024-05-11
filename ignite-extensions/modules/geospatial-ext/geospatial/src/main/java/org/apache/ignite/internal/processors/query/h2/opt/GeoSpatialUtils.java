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
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.h2.table.IndexColumn;
import org.locationtech.jts.geom.Geometry;

/**
 * This class is entrypoint for creating Geo-Spatial index.
 */
public class GeoSpatialUtils {
    static {
        SchemaManager.registerIndexDescriptorFactory(QueryIndexType.GEOSPATIAL, GeoSpatialIndexDescriptorFactory.INSTANCE);
        IndexKeyFactory.register(IndexKeyType.GEOMETRY, k -> new GeometryIndexKey((Geometry)k));
    }

    /** */
    public static GridH2IndexBase createIndex(GridH2Table tbl, IndexDescriptor idxDesc, List<IndexColumn> cols) {
        try {
            if (tbl.cacheInfo().affinityNode())
                return new GridH2SpatialIndex(tbl, cols, idxDesc.index().unwrap(GeoSpatialIndexImpl.class));
            else
                return new GridH2SpatialClientIndex(tbl, cols, idxDesc.index().unwrap(GeoSpatialClientIndex.class));
        }
        catch (Exception e) {
            throw new IgniteException("Failed to instantiate", e);
        }
    }
}
