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

package org.apache.ignite.internal.processors.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/** Verifies custom sql schema through JDBC API. */
public class JdbcSqlCustomSchemaTest extends AbstractCustomSchemaTest {
    /** SELECT query pattern. */
    private static final Pattern SELECT_QRY_PATTERN = Pattern.compile("^SELECT .*");

    /** Connection. */
    private Connection conn;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (conn != null && !conn.isClosed())
            conn.close();

        super.afterTestsStopped();
    }

    /** */
    private Connection getOrCreateConnection() throws SQLException {
        if (conn == null)
            conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");

        return conn;
    }

    /** {@inheritDoc} */
    @Override protected List<List<?>> execSql(String qry) {
        try (Statement stmt = getOrCreateConnection().createStatement()) {
            if (SELECT_QRY_PATTERN.matcher(qry).matches())
                return resultSetToList(stmt.executeQuery(qry));
            else {
                stmt.execute(qry);

                return Collections.emptyList();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** Converts {@link ResultSet} to list of lists. */
    private List<List<?>> resultSetToList(ResultSet rs) throws SQLException {
        List<List<?>> res = new ArrayList<>();

        while (rs.next()) {
            List<Object> row = new ArrayList<>();

            res.add(row);

            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
                row.add(rs.getObject(i));
        }

        return res;
    }
}
