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

package org.apache.ignite.internal.table.event;

import org.apache.ignite.internal.manager.EventParameters;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Table event parameters. There are properties which associate with a concrete table.
 */
public class TableEventParameters implements EventParameters {
    /** Table identifier. */
    private final IgniteUuid tableId;

    /** Table name. */
    private final String tableName;

    /** Table instance. */
    private final TableImpl table;

    /**
     * @param table Table instance.
     */
    public TableEventParameters(TableImpl table) {
        this(table.tableId(), table.tableName(), table);
    }

    /**
     * @param tableId   Table identifier.
     * @param tableName Table name.
     */
    public TableEventParameters(IgniteUuid tableId, String tableName) {
        this(tableId, tableName, null);
    }

    /**
     * @param tableId   Table identifier.
     * @param tableName Table name.
     * @param table     Table instance.
     */
    public TableEventParameters(IgniteUuid tableId, String tableName, TableImpl table) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.table = table;
    }

    /**
     * Get the table identifier.
     *
     * @return Table id.
     */
    public IgniteUuid tableId() {
        return tableId;
    }

    /**
     * Gets the table name.
     *
     * @return Table name.
     */
    public String tableName() {
        return tableName;
    }

    /**
     * Gets a table instance associated with the event.
     *
     * @return Table.
     */
    public TableImpl table() {
        return table;
    }
}
