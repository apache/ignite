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

package org.apache.ignite.schema.parser.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.schema.parser.DbColumn;
import org.apache.ignite.schema.parser.DbTable;

/**
 * Base class for database metadata dialect.
 */
public abstract class DatabaseMetadataDialect {
    /**
     * Gets schemas from database.
     *
     * @param conn Database connection.
     * @return Collection of schema descriptors.
     * @throws SQLException If failed to get schemas.
     */
    public abstract Collection<String> schemas(Connection conn) throws SQLException;

    /**
     * Gets tables from database.
     *
     * @param conn Database connection.
     * @param schemas Collection of schema names to load.
     * @param tblsOnly If {@code true} then gets only tables otherwise gets tables and views.
     * @return Collection of table descriptors.
     * @throws SQLException If failed to get tables.
     */
    public abstract Collection<DbTable> tables(Connection conn, List<String> schemas, boolean tblsOnly)
        throws SQLException;

    /**
     * @return Collection of database system schemas.
     */
    public Set<String> systemSchemas() {
        return Collections.singleton("INFORMATION_SCHEMA");
    }

    /**
     * Create table descriptor.
     *
     * @param schema Schema name.
     * @param tbl Table name.
     * @param cols Table columns.
     * @param idxs Table indexes.
     * @return New {@code DbTable} instance.
     */
    protected DbTable table(String schema, String tbl, Collection<DbColumn> cols, Collection<QueryIndex>idxs) {
        return new DbTable(schema, tbl, cols, idxs);
    }
}
