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

package org.apache.ignite.internal.schema.modification;

import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.modification.AlterColumnBuilder;
import org.apache.ignite.schema.modification.TableModificationBuilder;

/**
 * Alter column builder.
 */
class AlterColumnBuilderImpl implements AlterColumnBuilder {
    /** Table modification builder. */
    private final TableModificationBuilderImpl tableBuilder;

    /**
     * Constructor.
     *
     * @param tableBuilder Table modification builder.
     */
    AlterColumnBuilderImpl(TableModificationBuilderImpl tableBuilder) {
        this.tableBuilder = tableBuilder;
    }

    /** {@inheritDoc} */
    @Override public AlterColumnBuilder withNewName(String newName) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public AlterColumnBuilder convertTo(ColumnType newType) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public AlterColumnBuilder withNewDefault(Object defaultValue) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public AlterColumnBuilder asNullable() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public AlterColumnBuilder asNonNullable(Object replacement) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public TableModificationBuilder done() {
        return tableBuilder;
    }
}
