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

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * Sql table representation for a {@link SystemView}.
 */
public class SqlTableView {
    /** Table. */
    private final TableDescriptor tbl;

    /** Affinity column name. */
    private String affColName;

    /**
     * @param tbl Table.
     */
    public SqlTableView(TableDescriptor tbl) {
        this.tbl = tbl;

        String affCol = tbl.descriptor().affinityKey();

        if (affCol != null) {
            // Only explicit affinity column should be shown. Do not do this for _KEY or it's alias.
            if (!F.eq(tbl.descriptor().keyFieldName(), affCol) && !F.eq(tbl.descriptor().keyFieldAlias(), affCol))
                affColName = affCol;
        }
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
     *
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
     * Returns schema name.
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
     * Returns name of affinity key column.
     *
     * @return Affinity key column name.
     */
    @Order(6)
    public String affinityKeyColumn() {
        return affColName;
    }

    /**
     * Returns alias for key column.
     *
     * @return Key alias.
     */
    @Order(7)
    public String keyAlias() {
        return tbl.descriptor().keyFieldAlias();
    }

    /**
     * Returns alias for value column.
     *
     * @return Value alias.
     */
    @Order(8)
    public String valueAlias() {
        return tbl.descriptor().valueFieldAlias();
    }

    /**
     * Returns name of key type.
     *
     * @return Key type name.
     */
    @Order(9)
    public String keyTypeName() {
        return tbl.descriptor().keyTypeName();
    }

    /**
     * Returns name of value type.
     *
     * @return Value type name.
     */
    @Order(10)
    public String valueTypeName() {
        return tbl.descriptor().valueTypeName();
    }

    /** @return {@code True} if index rebuild is in progress. */
    public boolean isIndexRebuildInProgress() {
        return tbl.isIndexRebuildInProgress();
    }
}
