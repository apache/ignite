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
package org.apache.ignite.compatibility.sql.runner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Supplier;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Query runner. Runs query and checks it's execution time.
 */
public class QueryExecutionTimer implements Supplier<Long> {
    /** */
    private final String qry;

    /** */
    private final SimpleConnectionPool connPool;

    /** */
    public QueryExecutionTimer(String qry, SimpleConnectionPool connPool) {
        this.qry = qry;
        this.connPool = connPool;
    }

    /** {@inheritDoc} */
    @Override public Long get() {
        long start = System.currentTimeMillis();

        Connection conn = connPool.getConnection();
        int cnt = 0;
        try (Statement stmt = conn.createStatement()) {
            //System.out.println(Thread.currentThread().getName() + " Start query=" + qry);

            try (ResultSet rs = stmt.executeQuery(qry)) {

                while (rs.next()) {
                    // Just read the full result set.
                    cnt++;
                }

            }
        }
        catch (SQLException e) {
            System.out.println("qry ERRR=" + qry + "\nErrr!=" + X.getFullStackTrace(e));
            throw new RuntimeException(e);
        }
        finally {
            connPool.releaseConnection(conn);
        }

        System.out.println(Thread.currentThread().getName() + " Rs.size=" + cnt + ", time=" + (System.currentTimeMillis() - start) + ", qry=" + qry);

        return System.currentTimeMillis() - start;
    }
}
