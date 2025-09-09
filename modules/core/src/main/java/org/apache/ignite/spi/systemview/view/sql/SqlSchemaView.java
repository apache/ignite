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
import org.apache.ignite.internal.processors.query.schema.management.SchemaDescriptor;

/**
 * Sql schema system view representation.
 */
public class SqlSchemaView {
    /** Schema. */
    private final SchemaDescriptor schema;

    /**
     * @param schema Schema.
     */
    public SqlSchemaView(SchemaDescriptor schema) {
        this.schema = schema;
    }

    /** @return Schema name. */
    @Order
    public String schemaName() {
        return schema.schemaName();
    }

    /** @return {@code True} if schema is predefined, {@code false} otherwise. */
    public boolean predefined() {
        return schema.predefined();
    }
}
