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
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexValueCursor;
import org.apache.ignite.internal.processors.query.h2.H2Cursor;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexLookupBatch;
import org.h2.index.IndexType;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.locationtech.jts.geom.Geometry;

/**
 * H2 wrapper for a Geo-Spatial index.
 */
public class GridH2SpatialIndex extends GridH2SpatialBaseIndex {
    /** */
    private final GeoSpatialIndexImpl delegate;

    /** */
    public GridH2SpatialIndex(GridH2Table tbl, List<IndexColumn> cols, GeoSpatialIndexImpl idx) {
        super(tbl,
            idx.name(),
            cols.toArray(new IndexColumn[0]),
            IndexType.createNonUnique(false, false, true));

        delegate = idx;
    }

    /** {@inheritDoc} */
    @Override public IndexLookupBatch createLookupBatch(TableFilter[] filters, int filter) {
        return delegate.createLookupBatch(filters, filter);
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        return delegate.def.segmentsCnt();
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        delegate.destroy(false);

        super.destroy();
    }

    /** {@inheritDoc} */
    @Override public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        GridCursor<IndexRow> cursor = delegate.find(segment(H2Utils.context(filter.getSession())), filter);

        GridCursor<H2Row> h2cursor = new IndexValueCursor<>(cursor, this::mapIndexRow);

        return new H2Cursor(h2cursor);
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow first, SearchRow last) {
        GridCursor<IndexRow> cursor = delegate.find(segment(H2Utils.context(ses)), null);

        GridCursor<H2Row> h2cursor = new IndexValueCursor<>(cursor, this::mapIndexRow);

        return new H2Cursor(h2cursor);
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        GridCursor<IndexRow> cursor = delegate.findFirstOrLast(H2Utils.context(ses).segment(), first);

        GridCursor<H2Row> h2cursor = new IndexValueCursor<>(cursor, this::mapIndexRow);

        return new H2Cursor(h2cursor);
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        return delegate.totalCount();
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return delegate.totalCount();
    }

    /** {@inheritDoc} */
    @Override public long totalRowCount(IndexingQueryCacheFilter partsFilter) {
        return delegate.totalCount();
    }

    /** {@inheritDoc} */
    @Override public Cursor findByGeometry(TableFilter filter, SearchRow first, SearchRow last,
        SearchRow intersection) {

        Value v = intersection.getValue(columnIds[0]);
        Geometry g = ((ValueGeometry)v.convertTo(Value.GEOMETRY)).getGeometry();

        int seg = segmentsCount() == 1 ? 0 : H2Utils.context(filter.getSession()).segment();

        GridCursor<IndexRow> cursor = delegate.findByGeometry(seg, filter, g);

        GridCursor<H2Row> h2cursor = new IndexValueCursor<>(cursor, this::mapIndexRow);

        return new H2Cursor(h2cursor);
    }

    /** */
    private H2Row mapIndexRow(IndexRow row) {
        if (row == null)
            return null;

        return new H2CacheRow(rowDescriptor(), row.cacheDataRow());
    }
}
