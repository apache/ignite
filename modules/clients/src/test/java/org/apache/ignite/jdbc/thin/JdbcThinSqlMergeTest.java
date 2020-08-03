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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.processors.query.SqlMergeTest;

/**
 * Tests SQL MERGE command via JDBC thin driver.
 */
public class JdbcThinSqlMergeTest extends SqlMergeTest {
    /** JDBC thin URL. */
    private static final String JDBC_THIN_URL = "jdbc:ignite:thin://127.0.0.1";

    /** Connection. */
    private static Connection conn;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        conn = DriverManager.getConnection(JDBC_THIN_URL);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        conn.close();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected List<List<?>> sql(String sql) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);

            int updCnt = stmt.getUpdateCount();

            if (updCnt == -1) {
                ResultSet rs = stmt.getResultSet();

                int cols = rs.getMetaData().getColumnCount();

                List<List<?>> res = new ArrayList<>();

                while (rs.next()) {
                    List<Object> row = new ArrayList<>();

                    for (int i = 0; i < cols; ++i)
                        row.add(rs.getObject(i + 1));

                    res.add(row);
                }

                return res;
            }
            else
                return Collections.singletonList(Collections.singletonList((long)updCnt));
        }
    }
}
