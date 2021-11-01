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

package org.apache.ignite.schema.definition.builder;

import java.util.Map;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.PrimaryKeyDefinition;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.schema.definition.index.IndexDefinition;

/**
 * Table definition builder.
 */
public interface TableDefinitionBuilder extends SchemaObjectBuilder {
    /**
     * Adds columns to the table.
     *
     * @param columns Table columns definitions.
     * @return {@code This} for chaining.
     */
    TableDefinitionBuilder columns(ColumnDefinition... columns);

    /**
     * Adds an index.
     *
     * @param indexDefinition Table index definition.
     * @return {@code This} for chaining.
     */
    TableDefinitionBuilder withIndex(IndexDefinition indexDefinition);

    /**
     * Shortcut method for adding {@link PrimaryKeyDefinition} of single column.
     *
     * @param colName Key column name.
     * @return {@code This} for chaining.
     */
    TableDefinitionBuilder withPrimaryKey(String colName);

    /**
     * Adds primary key constraint to the table.
     *
     * @param primaryKeyDefinition Primary key definition.
     * @return {@code This} for chaining.
     */
    TableDefinitionBuilder withPrimaryKey(PrimaryKeyDefinition primaryKeyDefinition);

    /** {@inheritDoc} */
    @Override TableDefinitionBuilder withHints(Map<String, String> hints);

    /**
     * Builds table definition.
     *
     * @return Table definition.
     */
    @Override TableDefinition build();
}
