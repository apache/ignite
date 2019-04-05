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
package org.apache.ignite.internal.processors.cache;

import java.sql.SQLException;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class ColumnStatisticsTest extends GridCommonAbstractTest {

    /** */
    private static final int SIZE = 5000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteEx grid = (IgniteEx)startGridsMultiThreaded(1, false);

        grid.createCache(DEFAULT_CACHE_NAME);

        runSql(grid, false, "CREATE TABLE A (id INT PRIMARY KEY, a INT, b INT, c INT)");
        runSql(grid, false, "CREATE TABLE B (id INT PRIMARY KEY, a INT, b INT, c INT)");

        runSql(grid, false, "CREATE INDEX Aa ON A(a)");
        runSql(grid, false, "CREATE INDEX Ab ON A(b)");
        runSql(grid, false, "CREATE INDEX Ac ON A(c)");

        runSql(grid, false, "CREATE INDEX Ba ON B(a)");
        runSql(grid, false, "CREATE INDEX Bb ON B(b)");
        runSql(grid, false, "CREATE INDEX Bc ON B(c)");

        for (int i = 0; i < SIZE; i++) {
            runSql(grid, false, "INSERT INTO A(id, a, b, c) VALUES(" + i + "," + i + ",1, 1)");

            runSql(grid, false, "INSERT INTO B(id, a, b, c) VALUES(" + i + "," + i + ",1, 1)");
        }

        // Update stats.
        IgniteH2Indexing indexing = (IgniteH2Indexing)grid.context().query().getIndexing();

        indexing.updateTableStatistics();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoin() throws Exception {
        H2TreeIndexBase.useStats = true;

        runJoin();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinStatsNotUsed() throws Exception {
        H2TreeIndexBase.useStats = false;

        runJoin();
    }

    private void runJoin() throws SQLException {
        IgniteEx grid = grid(0);

        List res = runSql(grid, true, "EXPLAIN ANALYZE SELECT COUNT(*) FROM A JOIN B ON A.b=B.b WHERE A.c>2 AND B.c=1" );

        System.err.println();
        System.err.println("================EXPLAIN ANALYZE RESULT===============");
        System.err.println(res);
    }

    /**
     * @param sql Statement.
     * @throws SQLException if failed.
     */
    public static List<List<?>> runSql(Ignite grid, boolean loc, String sql) throws SQLException {
        return grid.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql)
            .setLocal(loc)
        ).getAll();
    }

}
