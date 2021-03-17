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
public interface SchemaTable extends SchemaObject {
    /** Default schema name. */
    String DEFAULT_SCHEMA_NAME = "PUBLIC";

    /**
     * @return Table name.
     */
    @Override String name();

    /**
     * @return Key columns.
     */
    Collection<Column> keyColumns();

    /**
     * @return Affinity columns.
     */
    Collection<Column> affinityColumns();

    /**
     * @return Value columns.
     */
    Collection<Column> valueColumns();

    /**
     * Schema name + Table name
     *
     * @return Canonical table name.
     */
    String canonicalName();

    /**
     * @return Table indexes.
     */
    Collection<TableIndex> indices();

    /**
     * @return Schema modification builder.
     */
    TableModificationBuilder toBuilder();
}
