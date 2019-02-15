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
