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
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.AbstractIndex;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.jetbrains.annotations.Nullable;
import org.locationtech.jts.geom.Geometry;

import static org.apache.ignite.internal.processors.query.h2.index.client.ClientInlineIndex.unsupported;

/**
 * Mock for client nodes to support Geo-Spatial indexes.
 */
public class GeoSpatialClientIndex extends AbstractIndex implements GeoSpatialIndex {
    /** Index unique ID. */
    private final UUID id = UUID.randomUUID();

    /** */
    private final String name;

    /** */
    private final GridH2Table tbl;

    /** */
    private final List<IndexColumn> cols;

    /**
     * @param def Index definition.
     */
    public GeoSpatialClientIndex(GeoSpatialClientIndexDefinition def) {
        name = def.idxName().idxName();
        tbl = def.tbl();
        cols = def.cols();
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** */
    public GridH2Table tbl() {
        return tbl;
    }

    /** */
    public List<IndexColumn> cols() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public boolean canHandle(CacheDataRow row) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(
        @Nullable CacheDataRow oldRow,
        @Nullable CacheDataRow newRow,
        boolean prevRowAvailable
    ) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public <T extends Index> T unwrap(Class<T> clazz) {
        if (clazz == null)
            return null;

        if (clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException(
            String.format("Cannot unwrap [%s] to [%s]", getClass().getName(), clazz.getName())
        );
    }

    /** {@inheritDoc} */
    @Override public void destroy(boolean softDelete) {
        // No-op.
    }

    /**
     * @param filter Table filter.
     * @return Cursor.
     */
    @Override public GridCursor<IndexRow> find(int seg, TableFilter filter) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findFirstOrLast(int seg, boolean first) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public long totalCount() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findByGeometry(int seg, TableFilter filter, Geometry intersection) {
        throw unsupported();
    }
}
