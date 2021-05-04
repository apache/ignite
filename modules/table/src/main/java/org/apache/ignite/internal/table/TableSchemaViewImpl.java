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

package org.apache.ignite.internal.table;

import java.util.UUID;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;

/**
 * Schema view implementation.
 */
public class TableSchemaViewImpl implements TableSchemaView {
    /** Table identifier. */
    private final UUID tableId;

    /** Schema manager. */
    private final SchemaManager schemaManager;

    /**
     * @param tableId Table identifier.
     * @param schemaManager Schema manager.
     */
    public TableSchemaViewImpl(UUID tableId, SchemaManager schemaManager) {
        this.tableId = tableId;
        this.schemaManager = schemaManager;
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor schema() {
        return schemaManager.schema(tableId);
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor schema(int ver) {
        return schemaManager.schema(tableId, ver);
    }
}
