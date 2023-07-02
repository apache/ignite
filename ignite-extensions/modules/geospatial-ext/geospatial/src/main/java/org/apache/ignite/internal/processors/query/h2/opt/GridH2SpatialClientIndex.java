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
import org.apache.ignite.internal.cache.query.index.sorted.client.AbstractClientIndex;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;

/** Mock for registering Geo-Spatial indexes on client nodes in the H2 engine. */
public class GridH2SpatialClientIndex extends GridH2SpatialBaseIndex {
    /** */
    private final AbstractClientIndex delegate;

    /** */
    public GridH2SpatialClientIndex(GridH2Table tbl, List<IndexColumn> cols, GeoSpatialClientIndex delegate) {
        super(tbl, delegate.name(), cols.toArray(new IndexColumn[0]),
            IndexType.createNonUnique(false, false, true));

        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public H2CacheRow put(H2CacheRow row) {
        throw delegate.unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean putx(H2CacheRow row) {
        throw delegate.unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean removex(SearchRow row) {
        throw delegate.unsupported();
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        throw delegate.unsupported();
    }

    /** {@inheritDoc} */
    @Override public long totalRowCount(IndexingQueryCacheFilter partsFilter) {
        throw delegate.unsupported();
    }

    /** {@inheritDoc} */
    @Override public Cursor find(Session ses, SearchRow first, SearchRow last) {
        throw delegate.unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        throw delegate.unsupported();
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        throw delegate.unsupported();
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        throw delegate.unsupported();
    }

    /** {@inheritDoc} */
    @Override public Cursor findByGeometry(TableFilter filter, SearchRow first, SearchRow last, SearchRow intersection) {
        throw delegate.unsupported();
    }
}
