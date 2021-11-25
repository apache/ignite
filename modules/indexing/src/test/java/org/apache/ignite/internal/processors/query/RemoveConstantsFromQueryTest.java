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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.SqlQueryExecutionEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.cleanPerformanceStatisticsDir;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.startCollectStatistics;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.stopCollectStatisticsAndRead;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.DFLT_TO_STRING_INCLUDE_SENSITIVE;

/**
 * Tests check that with {@link IgniteSystemProperties#IGNITE_TO_STRING_INCLUDE_SENSITIVE} == false literals from query
 * will be deleted from query before logging it to history, events, profiling tool.
 */
public class RemoveConstantsFromQueryTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setIncludeEventTypes(EventType.EVT_SQL_QUERY_EXECUTION);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPerformanceStatisticsDir();

        QueryUtils.INCLUDE_SENSITIVE = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        QueryUtils.INCLUDE_SENSITIVE = DFLT_TO_STRING_INCLUDE_SENSITIVE;
    }

    /**  */
    @Test
    public void testConstantRemoved() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ACTIVE);

        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        String john = "John Connor";
        String sarah = "Sarah Connor";

        // SQL query, Regexp to find, Constant that should be removed.
        List<GridTuple3<String, String, String>> qries = Arrays.asList(
            F.t("CREATE TABLE TST(id INTEGER PRIMARY KEY, name VARCHAR, age integer)", null, null),

            F.t("CREATE TABLE TST2(id INTEGER PRIMARY KEY, name VARCHAR, age integer)", null, null),

            F.t("INSERT INTO TST(id, name, age) VALUES(1, '" + john + "', 16)",
                "INSERT INTO .*TST.*VALUES.*",
                john),

            F.t("INSERT INTO TST SELECT id, name, age FROM TST2 WHERE name = 'John Connor'",
                "INSERT INTO .*TST.*SELECT.*FROM .*TST2 WHERE.*",
                john),

            F.t("UPDATE TST SET name = '" + sarah + "' WHERE id = 1",
                "UPDATE .*TST SET NAME.*WHERE ID.*",
                sarah),

            F.t("DELETE FROM TST WHERE name = '" + sarah + "'",
                "DELETE FROM .*TST WHERE NAME = ?",
                sarah),

            F.t("SELECT * FROM TST WHERE name = '" + sarah + "'",
                "SELECT .* FROM .*TST.*WHERE .*NAME = ?",
                sarah),

            F.t("SELECT * FROM TST WHERE name = SUBSTR('" + sarah + "', 0, 2)",
                "SELECT .* FROM .*TST.*WHERE .*NAME = ?",
                sarah.substring(0, 2)),

            F.t("SELECT * FROM TST GROUP BY id HAVING name = '" + john + "'",
                "SELECT .* FROM .*TST.*GROUP BY .*ID HAVING .*NAME = ?",
                john),

            F.t("SELECT * FROM TST GROUP BY id HAVING name = '" + sarah + "' UNION " +
                    "SELECT * FROM TST GROUP BY id HAVING name = '" + john + "'",
                ".*SELECT .* FROM .*TST .* GROUP BY .*ID HAVING .*NAME = ?.* UNION " +
                    ".*SELECT .* FROM .*TST .* GROUP BY .*ID HAVING .*NAME = ?.*",
                sarah),

            F.t("SELECT CONCAT(name, '" + sarah + "') FROM TST",
                "SELECT CONCAT(.*) FROM .*TST",
                sarah),

            F.t("ALTER TABLE TST ADD COLUMN department VARCHAR(200)", null, null),

            F.t("DROP TABLE TST", null, null),

            F.t("KILL SERVICE 'my_service'", null, null)
        );

        AtomicReference<String> lastQryFromEvt = new AtomicReference<>();

        ignite.events().localListen(evt -> {
            assertTrue(evt instanceof SqlQueryExecutionEvent);

            lastQryFromEvt.set(((SqlQueryExecutionEvent)evt).text());

            return true;
        }, EventType.EVT_SQL_QUERY_EXECUTION);

        for (GridTuple3<String, String, String> qry : qries) {
            execSql(ignite, qry.get1());

            String expHist = qry.get2() == null ? qry.get1() : qry.get2();
            String qryFromEvt = lastQryFromEvt.get();

            List<List<?>> hist = execSql(ignite, "SELECT SQL FROM SYS.SQL_QUERIES_HISTORY WHERE SQL = ?", qryFromEvt);

            assertNotNull(hist);
            assertEquals(1, hist.size());

            String qryFromHist = hist.get(0).get(0).toString();

            if (qry.get2() != null) {
                Pattern ptrn = Pattern.compile(qry.get2());

                assertTrue(qry.get2() + " should match " + qryFromHist, ptrn.matcher(qryFromHist).find());
                assertTrue(qry.get2() + " should match " + qryFromEvt, ptrn.matcher(qryFromEvt).find());
            }
            else {
                assertEquals(qryFromHist, expHist);
                assertEquals(qryFromEvt, expHist);
            }

            if (qry.get3() != null) {
                assertFalse(qryFromHist.contains(qry.get3()));
                assertFalse(qryFromEvt.contains(qry.get3()));
            }
        }

        Set<String> qriesFromStats = new HashSet<>();

        stopCollectStatisticsAndRead(new AbstractPerformanceStatisticsTest.TestHandler() {
            @Override public void query(
                UUID nodeId,
                GridCacheQueryType type,
                String text,
                long id,
                long startTime,
                long duration,
                boolean success
            ) {
                qriesFromStats.add(text);
            }
        });

        // `SELECT ... WHERE name  = 'X'` and `SELECT ... WHERE name = SUBSTR(...)` produces the same query text
        // and we have one extra query `SELECT sql FROM SYS.SQL_QUERIES_HISTORY WHERE sql = ?`
        // so the sizes of two collection should be equal.
        assertEquals(qries.size(), qriesFromStats.size());

        assertTrue(qriesFromStats.contains("SELECT SQL FROM SYS.SQL_QUERIES_HISTORY WHERE SQL = ?1"));

        for (GridTuple3<String, String, String> qry : qries) {
            boolean found = false;

            for (String qryFromStat : qriesFromStats) {
                if (qry.get2() != null) {
                    if (!Pattern.compile(qry.get2()).matcher(qryFromStat).find())
                        continue;
                }
                else if (!qryFromStat.equals(qry.get1()))
                    continue;

                found = qry.get3() == null || !qryFromStat.contains(qry.get3());

                if (found)
                    break;
            }

            assertTrue(qry.get1() + " should be in statistics", found);
        }
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     */
    private List<List<?>> execSql(Ignite ignite, String sql, Object... args) {
        IgniteCache<?, ?> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return cache.query(qry).getAll();
    }
}
