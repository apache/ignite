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
import org.apache.ignite.internal.processors.query.schema.management.ViewDescriptor;

/**
 * Sql view representation for a {@link ViewDescriptor}.
 */
public class SqlViewView {
    /** Schema descriptor. */
    private final SchemaDescriptor schema;

    /** View descriptor. */
    private final ViewDescriptor view;

    /**
     * @param schema Schema descriptor.
     * @param view View descriptor.
     */
    public SqlViewView(SchemaDescriptor schema, ViewDescriptor view) {
        this.schema = schema;
        this.view = view;
    }

    /** @return View schema. */
    @Order
    public String schema() {
        return schema.schemaName();
    }

    /** @return View name. */
    @Order(1)
    public String name() {
        return view.name();
    }

    /** @return View SQL. */
    @Order(2)
    public String sql() {
        return view.sql();
    }

    /** @return View description. */
    @Order(3)
    public String description() {
        return view.description();
    }
}
