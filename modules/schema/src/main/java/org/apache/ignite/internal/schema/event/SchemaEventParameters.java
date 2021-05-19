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

package org.apache.ignite.internal.schema.event;

import java.util.UUID;
import org.apache.ignite.internal.manager.EventParameters;
import org.apache.ignite.internal.schema.SchemaRegistry;

/**
 * Schema event parameters. There are properties which associate with a concrete schema.
 */
public class SchemaEventParameters implements EventParameters {
    /** Table identifier. */
    private final UUID tableId;

    /** Schema registry. */
    private final SchemaRegistry reg;

    /**
     * @param tableId Table identifier.
     * @param reg Schema registry for the table.
     */
    public SchemaEventParameters(UUID tableId, SchemaRegistry reg) {
        this.tableId = tableId;
        this.reg = reg;
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
     * Get schema registry for the table.
     *
     * @return Schema registry.
     */
    public SchemaRegistry schemaRegistry() {
        return reg;
    }
}