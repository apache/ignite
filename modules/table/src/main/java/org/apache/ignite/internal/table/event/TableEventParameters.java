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

import java.util.UUID;
import org.apache.ignite.internal.manager.EventParameters;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.schema.SchemaRegistry;

/**
 * Table event parameters.
 * There are properties which associate with a concrete table.
 */
public class TableEventParameters implements EventParameters {
    /** Table identifier. */
    private final UUID tableId;

    /** Table name. */
    private final String tableName;

    /** Table schema view. */
    private final SchemaRegistry schemaRegistry;

    /** Internal table. */
    private final InternalTable internalTable;

    /**
     * @param tableId Table identifier.
     * @param tableName Table name.
     * @param schemaRegistry Table schema view.
     * @param internalTable Internal table.
     */
    public TableEventParameters(
        UUID tableId,
        String tableName,
        SchemaRegistry schemaRegistry,
        InternalTable internalTable
    ) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.schemaRegistry = schemaRegistry;
        this.internalTable = internalTable;
    }

    /**
     * Get the table identifier.
     *
     * @return Table id.
     */
    public UUID tableId() {
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
     * Gets a schema view for the table.
     *
     * @return Schema descriptor.
     */
    public SchemaRegistry tableSchemaView() {
        return schemaRegistry;
    }

    /**
     * Gets an internal table associated with the table.
     *
     * @return Internal table.
     */
    public InternalTable internalTable() {
        return internalTable;
    }
}
