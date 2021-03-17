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

import org.apache.ignite.internal.schema.SchemaTableImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.PrimaryIndex;
import org.apache.ignite.schema.TableIndex;
import org.apache.ignite.schema.modification.AlterColumnBuilder;
import org.apache.ignite.schema.modification.TableModificationBuilder;

/**
 * Table modification builder.
 */
public class TableModificationBuilderImpl implements TableModificationBuilder {
    /** Table. */
    private final SchemaTableImpl table;

    /**
     * Constructor.
     *
     * @param table Table.
     */
    public TableModificationBuilderImpl(SchemaTableImpl table) {
        this.table = table;
    }

    /** {@inheritDoc} */
    @Override public TableModificationBuilder addColumn(Column column) {
        if (table.hasColumn(column.name()))
            throw new IllegalStateException("Duplicate column: name='" + column.name() + '\'');

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableModificationBuilder addKeyColumn(Column column) {
        if (table.hasColumn(column.name()))
            throw new IllegalStateException("Duplicate column: name=" + column.name() + '\'');

        return this;
    }

    /** {@inheritDoc} */
    @Override public AlterColumnBuilder alterColumn(String columnName) {
        return new AlterColumnBuilderImpl(this);
    }

    /** {@inheritDoc} */
    @Override public TableModificationBuilder dropColumn(String columnName) {
        if (table.hasKeyColumn(columnName))
            throw new IllegalStateException("Can't drop key column: name=" + columnName);

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableModificationBuilder addIndex(TableIndex index) {
        assert !PrimaryIndex.PRIMARY_KEY_INDEX_NAME.equals(index.name());

        if (table.indices().stream().anyMatch(i -> i.name().equals(index.name())))
            throw new IllegalStateException("Index already exists: name=" + index.name() + '\'');

        return this;
    }

    /** {@inheritDoc} */
    @Override public TableModificationBuilder dropIndex(String indexName) {
        if (PrimaryIndex.PRIMARY_KEY_INDEX_NAME.equals(indexName))
            throw new IllegalStateException("Can't drop primary key index: name=" + indexName);

        return this;
    }

    /** {@inheritDoc} */
    @Override public void apply() {

    }
}
