/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
