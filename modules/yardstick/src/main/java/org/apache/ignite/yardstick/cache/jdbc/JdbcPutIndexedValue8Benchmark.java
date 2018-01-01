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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import org.apache.ignite.IgniteException;

/**
 * JDBC benchmark that performs raw SQL insert into a table with 8 btree indexed columns
 */
public class JdbcPutIndexedValue8Benchmark extends JdbcAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int newKey = nextRandom(args.range());
        int newVal = nextRandom(args.range());

        try (PreparedStatement stmt = createUpsertStatement(newKey, newVal)) {
            return stmt.executeUpdate() > 0;
        }
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (!args.createTempDatabase())
            clearTable("VALUE8");
        super.tearDown();
    }

    /**
     * Get upsert statement depending on the type of database
     * @param newKey the value that the new row is based upon
     * @return update portion of upsert statement
     * @throws SQLException if failed
     */
    private PreparedStatement createUpsertStatement(int newKey, int newVal) throws SQLException {
        PreparedStatement stmt;
        boolean explicitUpdate;
        switch (conn.get().getMetaData().getDatabaseProductName()) {
            case "H2":
                stmt = conn.get().prepareStatement("merge into VALUE8(val1, val2, val3, val4, val5, val6, val7, val8) " +
                    "values (?, ?, ?, ?, ?, ?, ?, ?)");

                explicitUpdate = false;

                break;

            case "MySQL":
                stmt = conn.get().prepareStatement("insert into VALUE8(val1, val2, val3, val4, val5, " +
                    "val6, val7, val8) values(?, ?, ?, ?, ?, ?, ?, ?) on duplicate key update val2 = ?, val3 = ?, " +
                    "val4 = ?, val5 = ?, val6 = ?, val7 = ?, val8 = ?");

                explicitUpdate = true;

                break;

            case "PostgreSQL":
                stmt = conn.get().prepareStatement("insert into VALUE8(val1, val2, val3, val4, val5, " +
                    "val6, val7, val8) values(?, ?, ?, ?, ?, ?, ?, ?) on conflict(val1) do update set " +
                    "val2 = ?, val3 = ?, val4 = ?, val5 = ?, val6 = ?, val7 = ?, val8 = ?");

                explicitUpdate = true;

                break;

            default:
                throw new IgniteException("Unexpected database type [databaseProductName=" +
                    conn.get().getMetaData().getDatabaseProductName() + ']');
        }

        for (int i = 0; i < 8; i++) {
            if (i == 0)
                stmt.setInt(1, newKey);
            else
                stmt.setInt(i + 1, newVal + i);
        }

        if (explicitUpdate) {
            for (int i = 1; i < 8; i++)
                stmt.setInt(i + 8, newVal + i);
        }

        return stmt;
    }
}
