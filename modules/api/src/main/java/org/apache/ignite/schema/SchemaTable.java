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

package org.apache.ignite.schema;

import java.util.Collection;
import org.apache.ignite.schema.modification.TableModificationBuilder;

/**
 * Schema table descriptor.
 */
public interface SchemaTable extends SchemaNamedObject {
    /** Default schema name. */
    String DEFAULT_SCHEMA_NAME = "PUBLIC";

    /**
     * Returns table name.
     *
     * @return Table name.
     */
    @Override String name();

    /**
     * Returns key columns.
     *
     * @return List of columns.
     */
    Collection<Column> keyColumns();

    /**
     * Returns affinity columns.
     *
     * @return List of columns.
     */
    Collection<Column> affinityColumns();

    /**
     * Returns value columns.
     *
     * @return List of columns.
     */
    Collection<Column> valueColumns();

    /**
     * Returns canonical table name (Concatenation of schema name and table name).
     *
     * @return Canonical table name.
     */
    String canonicalName();

    /**
     * Returns table indices.
     *
     * @return Collection of indexes.
     */
    Collection<TableIndex> indices();

    /**
     * Converts table descriptor to table modification builder.
     *
     * @return Table modification builder.
     */
    TableModificationBuilder toBuilder();
}
