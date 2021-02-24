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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.cleanPerformanceStatisticsDir;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.startCollectStatistics;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.stopCollectStatisticsAndRead;

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
    }

    /**  */
    @Test
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "false")
    public void testConstantRemoved() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        List<IgniteBiTuple<String, String>> qries = Arrays.asList(
            F.t("CREATE TABLE TST(id INTEGER PRIMARY KEY, name VARCHAR, age integer)", null),

            F.t("CREATE TABLE TST2(id INTEGER PRIMARY KEY, name VARCHAR, age integer)", null),

            F.t("INSERT INTO TST(id, name, age) VALUES(1, 'John Connor', 16)",
                "INSERT INTO PUBLIC.TST( ID, NAME, AGE ) VALUES (?, ?, ?)"),

            F.t("INSERT INTO TST SELECT id, name, age FROM TST2 WHERE name = 'John Connor'",
                "INSERT INTO PUBLIC.TST( ID, NAME, AGE )  SELECT ID, NAME, AGE FROM PUBLIC.TST2 WHERE NAME = ?"),

            F.t("UPDATE TST SET name = 'Sarah Connor' WHERE id = 1",
                "UPDATE PUBLIC.TST SET NAME = ? WHERE ID = ?"),

            F.t("DELETE FROM TST WHERE name = 'Sarah Connor'",
                "DELETE FROM PUBLIC.TST WHERE NAME = ?"),

            F.t("SELECT * FROM TST WHERE name = 'Sarah Connor'",
                "SELECT __Z0.ID, __Z0.NAME, __Z0.AGE FROM PUBLIC.TST __Z0 WHERE __Z0.NAME = ?"),

            F.t("SELECT * FROM TST WHERE name = SUBSTR('Sarah Connor', 0, 2)",
                "SELECT __Z0.ID, __Z0.NAME, __Z0.AGE FROM PUBLIC.TST __Z0 WHERE __Z0.NAME = ?"),

            F.t("SELECT * FROM TST GROUP BY id HAVING name = 'X'",
                "SELECT __Z0.ID, __Z0.NAME, __Z0.AGE FROM PUBLIC.TST __Z0 GROUP BY __Z0.ID HAVING __Z0.NAME = ?"),

            F.t("SELECT * FROM TST GROUP BY id HAVING name = 'X' UNION SELECT * FROM TST GROUP BY id HAVING name = 'Y'",
                "(SELECT __Z0.ID, __Z0.NAME, __Z0.AGE FROM PUBLIC.TST __Z0 GROUP BY __Z0.ID HAVING __Z0.NAME = ?)" +
                    " UNION " +
                    "(SELECT __Z1.ID, __Z1.NAME, __Z1.AGE FROM PUBLIC.TST __Z1 GROUP BY __Z1.ID HAVING __Z1.NAME = ?)"),

            F.t("SELECT CONCAT(name, 'xxx') FROM TST",
                "SELECT CONCAT(__Z0.NAME, ?) FROM PUBLIC.TST __Z0"),

            F.t("ALTER TABLE TST ADD COLUMN department VARCHAR(200)", null),

            F.t("DROP TABLE TST", null),

            F.t("KILL SERVICE 'my_service'", null)
        );

        AtomicReference<String> lastQryFromEvt = new AtomicReference<>();

        ignite.events().localListen(evt -> {
            assertTrue(evt instanceof SqlQueryExecutionEvent);

            lastQryFromEvt.set(((SqlQueryExecutionEvent)evt).text());

            return true;
        }, EventType.EVT_SQL_QUERY_EXECUTION);

        for (IgniteBiTuple<String, String> qry : qries) {
            execSql(ignite, qry.get1());

            String expHist = qry.get2() == null ? qry.get1() : qry.get2();

            assertEquals(expHist, lastQryFromEvt.get());

            List<List<?>> hist = execSql(ignite, "SELECT sql FROM SYS.SQL_QUERIES_HISTORY WHERE sql = ?", expHist);

            assertNotNull(hist);
            assertEquals(1, hist.size());
            assertEquals(hist.get(0).get(0), expHist);
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

        assertEquals(qries.size(), qriesFromStats.size());

        assertTrue(qriesFromStats.contains("SELECT SQL FROM SYS.SQL_QUERIES_HISTORY WHERE SQL = ?1"));

        for (IgniteBiTuple<String, String> qry : qries)
            assertTrue(qriesFromStats.contains(qry.get2() == null ? qry.get1() : qry.get2()));
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     */
    @SuppressWarnings("unchecked")
    private List<List<?>> execSql(Ignite ignite, String sql, Object... args) {
        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return cache.query(qry).getAll();
    }
}
