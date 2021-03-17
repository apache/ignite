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

package org.apache.ignite.schema.modification;

import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.TableIndex;

/**
 * Collect schema modification commands and pass them to manager to create a schema upgrade script.
 */
public interface TableModificationBuilder {
    /**
     * Adds new value column.
     *
     * @param column Column.
     * @return {@code this} for chaining.
     */
    TableModificationBuilder addColumn(Column column);

    /**
     * Adds new non-affinity key column.
     *
     * @param column Column.
     * @return {@code this} for chaining.
     */
    TableModificationBuilder addKeyColumn(Column column);

    /**
     * Creates alter column builder..
     *
     * @param columnName Column name.
     * @return Alter column builder.
     */
    AlterColumnBuilder alterColumn(String columnName);

    /**
     * Drops value column.
     * <p>
     * Note: Key column drop is not allowed.
     *
     * @param columnName Column.
     * @return {@code this} for chaining.
     */
    TableModificationBuilder dropColumn(String columnName);

    /**
     * Adds new table index.
     *
     * @param index Table index.
     * @return {@code this} for chaining.
     */
    TableModificationBuilder addIndex(TableIndex index);

    /**
     * Drops table index.
     * <p>
     * Note: PK can't be dropped.
     *
     * @param indexName Index name.
     * @return {@code this} for chaining.
     */
    TableModificationBuilder dropIndex(String indexName);

    /**
     * Apply changes.
     */
    void apply();
}
