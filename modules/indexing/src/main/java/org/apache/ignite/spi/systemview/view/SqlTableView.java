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
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.table.IndexColumn;

/**
 * Sql table representation for a {@link SystemView}.
 */
public class SqlTableView {
    /** Table. */
    private final GridH2Table tbl;

    /** Affinity column name. */
    private String affColName;

    /**
     * @param tbl Table.
     */
    public SqlTableView(GridH2Table tbl) {
        this.tbl = tbl;

        IndexColumn affCol = tbl.getAffinityKeyColumn();

        if (affCol != null) {
            // Only explicit affinity column should be shown. Do not do this for _KEY or it's alias.
            if (!tbl.rowDescriptor().isKeyColumn(affCol.column.getColumnId())) {
                affColName = affCol.columnName;
            }
        }
    }

    /** @return Cache id. */
    @Order(3)
    public int cacheId() {
        return tbl.cacheId();
    }

    /** @return Cache name. */
    @Order(2)
    public String cacheName() {
        return tbl.cacheName();
    }

    /** @return Schema name. */
    @Order(1)
    public String schemaName() {
        return tbl.getSchema().getName();
    }

    /** @return Table name. */
    @Order
    public String tableName() {
        return tbl.identifier().table();
    }

    /** @return Affinity key column. */
    @Order(4)
    public String affinityKeyColumn() {
        return affColName;
    }

    /** @return Key alias. */
    @Order(5)
    public String keyAlias() {
        return tbl.rowDescriptor().type().keyFieldAlias();
    }

    /** @return Value alias. */
    @Order(6)
    public String valueAlias() {
        return tbl.rowDescriptor().type().valueFieldAlias();
    }

    /** @return Key type name. */
    @Order(7)
    public String keyTypeName() {
        return tbl.rowDescriptor().type().keyTypeName();
    }

    /** @return Value type name. */
    @Order(8)
    public String valueTypeName() {
        return tbl.rowDescriptor().type().valueTypeName();
    }

    /** @return {@code True} if index rebuild is in progress. */
    public boolean isIndexRebuildInProgress() {
        return tbl.rebuildFromHashInProgress();
    }
}
