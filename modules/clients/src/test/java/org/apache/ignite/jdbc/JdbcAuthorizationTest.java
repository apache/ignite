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

package org.apache.ignite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.client.Config;
import org.apache.ignite.internal.processors.security.AbstractSqlQueryAuthorizationTest;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 * Tests permissions required for execution of the basic SQL requests through JDBC client.
 */
public class JdbcAuthorizationTest extends AbstractSqlQueryAuthorizationTest {
    /** JDBC URL prefix. */
    public static final String JDBC_URL_PREFIX = "jdbc:ignite:thin://";

    /** {@inheritDoc} */
    @Override protected void executeWithPermissions(String sql, SecurityPermissionSet perms) throws Exception {
        String login = "jdbc-client";

        registerUser(login, perms);

        try {
            try (Connection conn = getConnection(login)) {
                Statement stmt = conn.createStatement();

                stmt.execute(sql);
            }
        }
        finally {
            removeUser(login);
        }
    }

    /**
     * Establishes connection on behalf of user with the specified login.
     *
     * @param login User login.
     * @return Connection to server node.
     */
    protected Connection getConnection(String login) throws SQLException {
        return DriverManager.getConnection(JDBC_URL_PREFIX + Config.SERVER, login, "");
    }
}
