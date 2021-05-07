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

package org.apache.ignite.internal.table.impl;

import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.TableSchemaView;
import org.jetbrains.annotations.NotNull;

/**
 * Dummy schema manager for tests.
 */
public class DummySchemaManagerImpl implements TableSchemaView {
    /** Schema. */
    private final SchemaDescriptor schema;

    /**
     * Constructor.
     *
     * @param schema Schema descriptor.
     */
    public DummySchemaManagerImpl(@NotNull SchemaDescriptor schema) {
        assert schema != null;

        this.schema = schema;
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor schema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public SchemaDescriptor schema(int ver) {
        assert ver >= 0;

        assert schema.version() == ver;

        return schema;
    }
}
