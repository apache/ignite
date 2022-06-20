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

package org.apache.ignite.spi.systemview.view.sql;

import java.util.stream.Collectors;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * Sql index representation for a {@link SystemView}.
 */
public class SqlIndexView {
    /** Table. */
    private final TableDescriptor tbl;

    /** Index. */
    private final IndexDescriptor idx;

    /**
     * @param tbl Table information.
     * @param idx Index information.
     */
    public SqlIndexView(TableDescriptor tbl, IndexDescriptor idx) {
        this.tbl = tbl;
        this.idx = idx;
    }

    /**
     * Returns cache group ID.
     *
     * @return Cache group ID.
     */
    @Order()
    public int cacheGroupId() {
        return tbl.cacheInfo().groupId();
    }

    /**
     * Returns Cache group name.
     *
     * @return Cache group name.
     */
    @Order(1)
    public String cacheGroupName() {
        return tbl.cacheInfo().groupName();
    }

    /**
     * Returns cache ID.
     * @return Cache ID.
     */
    @Order(2)
    public int cacheId() {
        return tbl.cacheInfo().cacheId();
    }

    /**
     * Returns cache name.
     *
     * @return Cache name.
     */
    @Order(3)
    public String cacheName() {
        return tbl.cacheInfo().name();
    }

    /**
     *  Returns schema name.
     *
     * @return Schema name.
     */
    @Order(4)
    public String schemaName() {
        return tbl.descriptor().schemaName();
    }

    /**
     * Returns table name.
     *
     * @return Table name.
     */
    @Order(5)
    public String tableName() {
        return tbl.descriptor().tableName();
    }

    /**
     * Returns index name.
     *
     * @return Index name.
     */
    @Order(6)
    public String indexName() {
        return idx.name();
    }

    /**
     * Returns index type.
     *
     * @return Index type.
     */
    @Order(7)
    public String indexType() {
        return idx.type().name();
    }

    /**
     * Returns all columns on which index is built.
     *
     * @return Coma separated indexed columns.
     */
    @Order(8)
    public String columns() {
        return idx.keyDefinitions().entrySet().stream()
            .map(fld -> '"' + fld.getKey() + '"' +
                (fld.getValue().order().sortOrder() == SortOrder.DESC ? " DESC" : " ASC"))
            .collect(Collectors.joining(", "));
    }

    /**
     * Returns boolean value which indicates whether this index is for primary key or not.
     *
     * @return {@code True} if primary key index, {@code false} otherwise.
     */
    @Order(9)
    public boolean isPk() {
        return idx.isPk();
    }

    /**
     * Returns boolean value which indicates whether this index is unique or not.
     *
     * @return {@code True} if unique index, {@code false} otherwise.
     */
    @Order(10)
    public boolean isUnique() {
        return idx.isPk();
    }

    /**
     * Returns inline size in bytes.
     *
     * @return Inline size.
     */
    @Order(11)
    public Integer inlineSize() {
        return idx.inlineSize();
    }
}
