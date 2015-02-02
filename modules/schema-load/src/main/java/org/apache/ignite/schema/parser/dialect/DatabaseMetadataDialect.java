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

import org.apache.ignite.schema.parser.*;

import java.sql.*;
import java.util.*;

/**
 * Base class for database metadata dialect.
 */
public abstract class DatabaseMetadataDialect {
    /**
     * Gets tables from database.
     *
     * @param conn Database connection.
     * @param tblsOnly If {@code true} then gets only tables otherwise gets tables and views.
     * @return Collection of table descriptors.
     * @throws SQLException If failed to get tables.
     */
    public abstract Collection<DbTable> tables(Connection conn, boolean tblsOnly) throws SQLException;

    /**
     * @return Collection of database system schemas.
     */
    public Set<String> systemSchemas() {
        return Collections.singleton("INFORMATION_SCHEMA");
    }
}
