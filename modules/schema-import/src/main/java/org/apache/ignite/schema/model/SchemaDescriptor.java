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

package org.apache.ignite.schema.model;

import javafx.beans.property.*;

/**
 * Descriptor for schema.
 */
public class SchemaDescriptor {
    /** Schema name */
    private final String schema;

    /** State of schema selection. */
    private final BooleanProperty selected;

    /**
     * Constructor of schema descriptor.
     *
     * @param schema Schema.
     * @param selected Selection state.
     */
    public SchemaDescriptor(String schema, boolean selected) {
        this.schema = schema;
        this.selected = new SimpleBooleanProperty(selected);
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Boolean property support for {@code selected} property.
     */
    public BooleanProperty selected() {
        return selected;
    }

    @Override
    public String toString() {
        return schema;
    }
}
