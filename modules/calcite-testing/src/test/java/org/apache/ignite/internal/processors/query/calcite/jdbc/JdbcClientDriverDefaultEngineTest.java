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

package org.apache.ignite.internal.processors.query.calcite.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;

/** Checks that the JDBC Client Driver ({@code jdbc:ignite:cfg://}) works with any default SQL engine of the client node it starts. */
@ParameterizedClass(name = "engine={0}")
@ValueSource(strings = {IndexingQueryEngineConfiguration.ENGINE_NAME, CalciteQueryEngineConfiguration.ENGINE_NAME})
public class JdbcClientDriverDefaultEngineTest extends AbstractJdbcTest {
    /** Directory with the Spring XML configurations of the client node started by the driver. */
    private static final String CFG_DIR = "modules/calcite-testing/src/test/config/";

    /** Name of the default engine. */
    @Parameter(0)
    private String dfltEngine;

    /** */
    private Connection conn;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (dfltEngine.equals(IndexingQueryEngineConfiguration.ENGINE_NAME)) {
            cfg.setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(
                new IndexingQueryEngineConfiguration().setDefault(true),
                new CalciteQueryEngineConfiguration()));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @BeforeEach
    @Override protected void beforeTest() throws Exception {
        startGrid(0);

        serverSql("CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");
        serverSql("INSERT INTO test VALUES (1, 'val1'), (2, 'val2')");

        conn = DriverManager.getConnection("jdbc:ignite:cfg://" + CFG_DIR + "jdbc-client-driver-" + dfltEngine + ".xml");

        conn.setSchema("PUBLIC");
    }

    /** {@inheritDoc} */
    @AfterEach
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(conn);

        stopAllGrids();
    }

    /** */
    @Test
    public void testSelect() throws Exception {
        checkEngine();

        List<List<Object>> res = executeQuery(conn, "SELECT id, val FROM test ORDER BY id");

        assertEquals(2, res.size());
        assertEquals(1, res.get(0).get(0));
        assertEquals("val1", res.get(0).get(1));
        assertEquals(2, res.get(1).get(0));
        assertEquals("val2", res.get(1).get(1));
    }

    /** */
    @Test
    public void testDml() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            assertEquals(1, stmt.executeUpdate("INSERT INTO test VALUES (3, 'val3')"));
        }

        assertEquals(3L, serverSql("SELECT COUNT(*) FROM test").get(0).get(0));
    }

    /** Executes a query on the server node with its default engine. */
    private List<List<?>> serverSql(String sql) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql), false).getAll();
    }

    /** */
    private void checkEngine() throws SQLException {
        List<List<Object>> res = executeQuery(conn, "SELECT QUERY_ENGINE()");

        assertEquals(1, res.size());
        assertEquals(dfltEngine, res.get(0).get(0));
    }
}
