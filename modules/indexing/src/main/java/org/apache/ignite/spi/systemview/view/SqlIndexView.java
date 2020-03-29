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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.database.H2IndexType;
import org.apache.ignite.internal.processors.query.h2.database.H2PkHashIndex;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ProxyIndex;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2TableScanIndex;
import org.h2.index.Index;

/**
 * Sql index representation for a {@link SystemView}.
 */
public class SqlIndexView {
    /** Table. */
    private final GridH2Table tbl;

    /** Index. */
    private final Index idx;

    /** Index type. */
    private final H2IndexType type;

    /** */
    public SqlIndexView(GridH2Table tbl, Index idx) {
        this.tbl = tbl;
        this.idx = idx;
        this.type = type(idx);
    }

    /** @return Cache id. */
    public int cacheId() {
        return tbl.cacheId();
    }

    /** @return Cache name. */
    @Order(5)
    public String cacheName() {
        return tbl.cacheName();
    }

    /** @return Schema name. */
    @Order(3)
    public String schemaName() {
        return tbl.getSchema().getName();
    }

    /** @return Table name. */
    @Order(4)
    public String tableName() {
        return tbl.identifier().table();
    }

    /** @return Index name. */
    @Order()
    public String indexName() {
        return idx.getName();
    }

    /** @return Index type. */
    @Order(1)
    public H2IndexType indexType() {
        return type;
    }

    /** @return Indexed columns. */
    @Order(2)
    public String columns() {
        switch (type) {
            case HASH:
            case BTREE:
                return H2Utils.indexColumnsSql(H2Utils.unwrapKeyColumns(tbl, idx.getIndexColumns()));

            case SPATIAL:
                return H2Utils.indexColumnsSql(idx.getIndexColumns());

            case SCAN:
                return null;

            default:
                return "???";
        }
    }

    /** @return {@code True} if primary key index, {@code false} otherwise. */
    public boolean isPk() {
        return idx.getIndexType().isPrimaryKey();
    }

    /** @return {@code True} if unique index, {@code false} otherwise. */
    public boolean isUnique() {
        return idx.getIndexType().isUnique();
    }

    /** @return Inline size. */
    public int inlineSize() {
        return idx instanceof H2TreeIndexBase ? ((H2TreeIndexBase)idx).inlineSize() : 0;
    }

    /**
     * @param idx Inde.
     * @return Index type.
     */
    private static H2IndexType type(Index idx) {
        if (idx instanceof H2TreeIndexBase) {
            return H2IndexType.BTREE;
        } else if (idx instanceof H2PkHashIndex)
            return H2IndexType.HASH;
        else if (idx instanceof H2TableScanIndex)
            return H2IndexType.SCAN;
        else if (idx instanceof GridH2ProxyIndex)
            return type(((GridH2ProxyIndex)idx).underlyingIndex());
        else if (idx.getIndexType().isSpatial())
            return H2IndexType.SPATIAL;

        return null;
    }
}
