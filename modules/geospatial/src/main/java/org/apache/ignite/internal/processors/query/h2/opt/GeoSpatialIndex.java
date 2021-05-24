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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.h2.table.TableFilter;
import org.locationtech.jts.geom.Geometry;

/**
 * Interface for geo spatial indexes.
 */
public interface GeoSpatialIndex extends Index {
    /**
     * Finds index rows in specifed segment with table filtering.
     *
     * @param segment Number of segment to find.
     * @param filter Table filter.
     * @return Cursor of found index rows.
     */
    public GridCursor<IndexRow> find(int segment, TableFilter filter);

    /**
     * Finds index rows in specifed segment with table filtering.
     *
     * @param segment Number of segment to find.
     * @param filter Table filter.
     * @param intersection Intersection geometry to find rows within it.
     * @return Cursor of found index rows.
     */
    public GridCursor<IndexRow> findByGeometry(int segment, TableFilter filter, Geometry intersection);

    /**
     * Finds first or last index row for specified segment.
     *
     * @param firstOrLast if {@code true} then return first index row or otherwise last row.
     * @param segment     Number of segment to find.
     * @return Cursor of found index rows.
     */
    public GridCursor<IndexRow> findFirstOrLast(int segment, boolean firstOrLast);

    /**
     * Counts index rows for all segments.
     *
     * @return total count of index rows.
     */
    public long totalCount() throws IgniteCheckedException;
}
