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

package org.apache.ignite.yardstick.cache.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import org.apache.ignite.IgniteException;

/** JDBC benchmark that performs raw SQL insert */
public class JdbcPutBenchmark extends JdbcAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int newKey = nextRandom(args.range());
        int newVal = nextRandom(args.range());

        try (PreparedStatement stmt = createUpsertStatement(conn.get(), newKey, newVal)) {
            return stmt.executeUpdate() > 0;
        }
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (!args.createTempDatabase())
            clearTable("SAMPLE");

        super.tearDown();
    }

    /**
     * Get upsert statement depending on the type of database
     * @param conn {@link Connection} to get metadata from to determine storage type
     * @param newKey key to insert
     * @return upsert statement with params set
     * @throws SQLException if failed
     */
    static PreparedStatement createUpsertStatement(Connection conn, int newKey, int newVal) throws SQLException {
        PreparedStatement stmt;
        switch (conn.getMetaData().getDatabaseProductName()) {
            case "H2":
                stmt = conn.prepareStatement("merge into SAMPLE(id, val) values(?, ?)");

                break;

            case "MySQL":
                stmt = conn.prepareStatement("insert into SAMPLE(id, val) values(?, ?) on duplicate key update val = ?");

                stmt.setInt(3, newVal);

                break;

            case "PostgreSQL":
                stmt = conn.prepareStatement("insert into SAMPLE(id, val) values(?, ?) on conflict(id) do " +
                    "update set val = ?");

                stmt.setInt(3, newVal);

                break;

            default:
                throw new IgniteException("Unexpected database type [databaseProductName=" +
                    conn.getMetaData().getDatabaseProductName() + ']');
        }

        stmt.setInt(1, newKey);
        stmt.setInt(2, newVal);

        return stmt;
    }
}
