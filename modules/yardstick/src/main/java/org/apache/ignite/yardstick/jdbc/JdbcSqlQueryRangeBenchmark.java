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

package org.apache.ignite.yardstick.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * JDBC benchmark that performs select operations
 */
public class JdbcSqlQueryRangeBenchmark extends AbstractJdbcBenchmark {
    /** Statement with range. */
    private ThreadLocal<PreparedStatement> stmtRange;

    /** Statement full scan. */
    private ThreadLocal<PreparedStatement> stmtSingle;

    /** Prepares SELECT query and it's parameters. */
    private SelectCommand select;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        select = args.selectCommand();

        stmtRange = newStatement(select.selectRange());

        stmtSingle = newStatement(select.selectOne());
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long expRsSize;

        PreparedStatement stmt;

        if (args.sqlRange() == 1) {
            stmt = stmtSingle.get();

            long id = ThreadLocalRandom.current().nextLong(args.range()) + 1;

            stmt.setLong(1, select.fieldByPK(id));

            expRsSize = 1;
        }
        else {
            stmt = stmtRange.get();

            long id = ThreadLocalRandom.current().nextLong(args.range() - args.sqlRange()) + 1;
            long maxId = id + args.sqlRange() - 1;

            stmt.setLong(1, select.fieldByPK(id));
            stmt.setLong(2, select.fieldByPK(maxId));

            expRsSize = args.sqlRange();
        }

        long rsSize = 0;

        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                if (rs.getLong(1) + 1 != rs.getLong(2))
                    throw new Exception("Invalid result retrieved");

                rsSize++;
            }
        }

        if (rsSize != expRsSize)
            throw new Exception("Invalid result set size [actual=" + rsSize + ", expected=" + expRsSize + ']');

        return true;
    }
}
