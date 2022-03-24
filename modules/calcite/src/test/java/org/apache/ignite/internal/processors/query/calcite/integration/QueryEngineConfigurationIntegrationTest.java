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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.QueryEngineConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.processors.query.NoOpQueryEngine;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.QueryEngineConfigurationEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class QueryEngineConfigurationIntegrationTest extends GridCommonAbstractTest {
    /** */
    private static final String jdbcUrl = "jdbc:ignite:thin://127.0.0.1";

    /** */
    private static final String INDEXING_ENGINE = IndexingQueryEngineConfiguration.ENGINE_NAME;

    /** */
    private static final String CALCITE_ENGINE = CalciteQueryEngineConfiguration.ENGINE_NAME;

    /** */
    @Test
    public void testInvalidConfiguration() {
        // Not supported engine configuration.
        assertConfigurationThrown(new QueryEngineConfiguration() {
            @Override public boolean isDefault() {
                return false;
            }

            @Override public QueryEngineConfiguration setDefault(boolean isDflt) {
                return null;
            }
        });

        // Duplicated engine configuration.
        assertConfigurationThrown(new IndexingQueryEngineConfiguration(), new CalciteQueryEngineConfiguration(),
            new CalciteQueryEngineConfiguration());

        assertConfigurationThrown(new IndexingQueryEngineConfiguration(), new IndexingQueryEngineConfiguration(),
            new CalciteQueryEngineConfiguration());

        // Two defaults.
        assertConfigurationThrown(new IndexingQueryEngineConfiguration().setDefault(true),
            new CalciteQueryEngineConfiguration().setDefault(true));

        // Unknown query engine class.
        assertConfigurationThrown(new QueryEngineConfigurationEx() {
            @Override public String engineName() {
                return "NoOp";
            }

            @Override public boolean isDefault() {
                return false;
            }

            @Override public QueryEngineConfiguration setDefault(boolean isDflt) {
                return null;
            }

            @Override public Class<? extends QueryEngine> engineClass() {
                return NoOpQueryEngine.class;
            }
        });
    }

    /** */
    @Test
    public void testDefaultEngineConfiguration() throws Exception {
        checkDefaultEngine(INDEXING_ENGINE);

        checkDefaultEngine(INDEXING_ENGINE, new IndexingQueryEngineConfiguration(),
            new CalciteQueryEngineConfiguration());

        checkDefaultEngine(INDEXING_ENGINE, new CalciteQueryEngineConfiguration(),
            new IndexingQueryEngineConfiguration());

        checkDefaultEngine(CALCITE_ENGINE, new CalciteQueryEngineConfiguration().setDefault(true),
            new IndexingQueryEngineConfiguration());

        checkDefaultEngine(CALCITE_ENGINE, new CalciteQueryEngineConfiguration());
    }

    /** */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testEngineByHint() throws Exception {
        try (Ignite ignite = startGrid(createConfiguration(new IndexingQueryEngineConfiguration(),
            new CalciteQueryEngineConfiguration()))) {
            IgniteCache<?, ?> cache = ignite.createCache("test");

            checkEngine(cache, CALCITE_ENGINE, "SELECT /*+ QUERY_ENGINE('" + CALCITE_ENGINE +
                "') */ QUERY_ENGINE()");

            // Combination of hints.
            checkEngine(cache, CALCITE_ENGINE, "SELECT /*+ DISABLE_RULE('LogicalTableScanConverterRule')," +
                "QUERY_ENGINE('" + CALCITE_ENGINE + "') */ QUERY_ENGINE()");

            checkEngine(cache, CALCITE_ENGINE, "SELECT /*+ QUERY_ENGINE('" + CALCITE_ENGINE.toUpperCase() +
                "') */ QUERY_ENGINE()");

            checkEngine(cache, INDEXING_ENGINE, "SELECT /*+ QUERY_ENGINE('" + INDEXING_ENGINE +
                "') */ QUERY_ENGINE()");

            // NL in hints processing.
            checkEngine(cache, CALCITE_ENGINE, "SELECT /*+ \n QUERY_ENGINE  ( \n'" + CALCITE_ENGINE +
                "'\n)\n */ QUERY_ENGINE()");

            // Not hint comments processing.
            checkEngine(cache, INDEXING_ENGINE, "SELECT /*+ SOME_HINT('') */ /* QUERY_ENGINE('" + CALCITE_ENGINE +
                "') */ QUERY_ENGINE()");

            // Unknown engine.
            GridTestUtils.assertThrowsWithCause(
                () -> cache.query(new SqlFieldsQuery("SELECT /*+ QUERY_ENGINE('noop') */ QUERY_ENGINE()")).getAll(),
                IgniteException.class);
        }
    }

    /** */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testEngineWithoutConfiguration() throws Exception {
        try (Ignite ignite = startGrid(createConfiguration())) {
            IgniteCache<?, ?> cache = ignite.createCache("test");

            // Hint can't be used when engines is not configured.
            GridTestUtils.assertThrowsWithCause(
                () -> cache.query(new SqlFieldsQuery(
                    "SELECT /*+ QUERY_ENGINE('" + INDEXING_ENGINE + "') */ QUERY_ENGINE()")).getAll(),
                IgniteException.class);

            // JDBC connection property can't be used when engines is not configured.
            GridTestUtils.assertThrowsWithCause(
                () -> DriverManager.getConnection(jdbcUrl + "?queryEngine=" + INDEXING_ENGINE),
                SQLException.class);
        }
    }

    /** */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testEngineByJdbcProperty() throws Exception {
        try (Ignite ignored = startGrid(createConfiguration(new IndexingQueryEngineConfiguration(),
            new CalciteQueryEngineConfiguration()))) {

            String urlCalcite = jdbcUrl + "?queryEngine=" + CALCITE_ENGINE;
            String urlIndexing = jdbcUrl + "?queryEngine=" + INDEXING_ENGINE;

            checkJdbcEngine(CALCITE_ENGINE, urlCalcite, "SELECT QUERY_ENGINE()");

            checkJdbcEngine(INDEXING_ENGINE, urlIndexing, "SELECT QUERY_ENGINE()");

            // Hint has priority over JDBC property.
            checkJdbcEngine(INDEXING_ENGINE, urlCalcite,
                "SELECT /*+ QUERY_ENGINE('" + INDEXING_ENGINE + "') */ QUERY_ENGINE()");

            checkJdbcEngine(CALCITE_ENGINE, urlIndexing,
                "SELECT /*+ QUERY_ENGINE('" + CALCITE_ENGINE + "') */ QUERY_ENGINE()");

            // Unknown engine.
            GridTestUtils.assertThrowsWithCause(
                () -> DriverManager.getConnection(jdbcUrl + "?queryEngine=noop"),
                SQLException.class);
        }
    }

    /** */
    private IgniteConfiguration createConfiguration(QueryEngineConfiguration... enginesCfgs) throws Exception {
        return getConfiguration().setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(enginesCfgs));
    }

    /** */
    private void assertConfigurationThrown(QueryEngineConfiguration... enginesCfgs) {
        try {
            GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(createConfiguration(enginesCfgs)),
                IgniteCheckedException.class, null);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private void checkDefaultEngine(String engineName, QueryEngineConfiguration... enginesCfgs) throws Exception {
        try (Ignite ignite = startGrid(createConfiguration(enginesCfgs))) {
            IgniteCache<?, ?> cache = ignite.createCache("test");

            checkEngine(cache, engineName, "SELECT QUERY_ENGINE()");
        }
    }

    /** */
    private void checkEngine(IgniteCache<?, ?> cache, String engineName, String sql) throws Exception {
        // Check by cache API.
        List<List<?>> res = cache.query(new SqlFieldsQuery(sql)).getAll();

        assertEquals(1, res.size());
        assertEquals(1, res.get(0).size());
        assertEquals(engineName, res.get(0).get(0));

        // Check by JDBC.
        checkJdbcEngine(engineName, jdbcUrl, sql);
    }

    /** */
    private void checkJdbcEngine(String engineName, String url, String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setSchema("PUBLIC");
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    assertTrue(rs.next());
                    assertEquals(engineName, rs.getString(1));
                    assertFalse(rs.next());
                }
            }
        }
    }
}
