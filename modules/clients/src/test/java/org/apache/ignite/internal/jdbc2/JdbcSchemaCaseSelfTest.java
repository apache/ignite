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

package org.apache.ignite.internal.jdbc2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 * Jdbc v2 test for schema name case (in)sensitivity.
 */
public class JdbcSchemaCaseSelfTest extends JdbcAbstractSchemaCaseTest {
    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX + "cache=test0@modules/clients/src/test/config/jdbc-config.xml";

    /** {@inheritDoc} */
    @Override protected Connection connect(String schema) throws SQLException {
        Connection conn = DriverManager.getConnection(BASE_URL);

        conn.setSchema(schema);

        return conn;
    }
}
