/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Supplier;

/**
 * Query runner. Runs query and checks it's execution time.
 */
public class QueryExecutionTimer implements Supplier<Long> {
    /** */
    private final String qry;

    /** */
    private final SimpleConnectionPool connPool; // TODO check query result.

    /** */
    public QueryExecutionTimer(String qry, SimpleConnectionPool connPool) {
        this.qry = qry;
        this.connPool = connPool;
    }

    /** {@inheritDoc} */
    @Override public Long get() {
        long start = System.currentTimeMillis();

        Connection conn = connPool.getConnection();

        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(qry)) {
                int cnt = 0;
                while (rs.next()) { // TODO check for empty result
                    cnt++;
                }

                ResultSetMetaData md = rs.getMetaData();
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 0)
                        sb.append(", ");

                    sb.append(md.getColumnName(i));
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            connPool.releaseConnection(conn);
        }

        return System.currentTimeMillis() - start;
    }
}
