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

import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.h2.value.ValueGeometry;
import org.locationtech.jts.geom.Geometry;

/** */
public class GeometryIndexKey implements IndexKey {
    /** */
    private final ValueGeometry geometry;

    /** */
    public GeometryIndexKey(Geometry g) {
        geometry = ValueGeometry.getFromGeometry(g);
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        return geometry.getGeometry();
    }

    /** {@inheritDoc} */
    @Override public IndexKeyType type() {
        return IndexKeyType.GEOMETRY;
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        return geometry.compareTo(((GeometryIndexKey)o).geometry, null);
    }
}
