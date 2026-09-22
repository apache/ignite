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

package org.apache.ignite.internal.processors.query.calcite.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Abstract JDBC test.
 */
public abstract class AbstractJdbcTest extends GridCommonAbstractTest {
    /** URL. */
    protected static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** */
    protected void execute(Statement stmt, String sql) {
        try {
            stmt.execute(sql);
        }
        catch (SQLException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /** */
    protected List<List<Object>> executeQuery(Connection conn, String sql) throws SQLException {
        return executeQuery(conn.createStatement(), sql);
    }

    /** */
    protected List<List<Object>> executeQuery(Statement stmt, String sql) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            List<List<Object>> res = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();

                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
                    row.add(rs.getObject(i));

                res.add(row);
            }

            return res;
        }
        catch (SQLException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }
}
