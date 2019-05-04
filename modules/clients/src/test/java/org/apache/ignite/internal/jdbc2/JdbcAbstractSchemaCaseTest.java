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

package org.apache.ignite.internal.jdbc2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

/**
 * Base (for v2 and thin drivers) test for the case (in)sensitivity of schema name.
 */
public abstract class JdbcAbstractSchemaCaseTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            cacheConfiguration("test0", "test0"),
            cacheConfiguration("test1", "tEst1"),
            cacheConfiguration("test2", "\"TestCase\""));

        return cfg;
    }

    /**
     * Set up connection with specified schema as default JDBC connection schema.
     */
    protected abstract Connection connect(String schema) throws SQLException;

    /**
     * @param name Cache name.
     * @param schema Schema name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfiguration(@NotNull String name, @NotNull String schema) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setIndexedTypes(Integer.class, Integer.class);

        cfg.setName(name);

        cfg.setSqlSchema(schema);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRID_CNT);
    }

    /**
     * Cleanup.
     */
    @Before
    public void dropTables() throws Exception {
        List<String> schemas = getSchemasWithTestTable();

        try (Connection conn = connect("PUBLIC")) {
            Statement stmt = conn.createStatement();

            for (String schema : schemas)
                stmt.executeUpdate("DROP TABLE IF EXISTS \"" + schema + "\".TAB");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"unused"})
    @Test
    public void testSchemaName() throws Exception {
        checkSchemaConnection("test0");
        checkSchemaConnection("test1");
        checkSchemaConnection("\"TestCase\"");
        checkSchemaConnection("\"TEST0\"");
        checkSchemaConnection("\"TEST1\"");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                checkSchemaConnection("TestCase");

                return null;
            }
        }, SQLException.class, null);
    }

    /**
     * Check case (in)sensitivity of schema name that is specified in the connection url.
     */
    @Test
    public void testSchemaNameWithCreateTableIfNotExists() throws Exception {
        createTableWithImplicitSchema("test0");

        assertTablesInSchemasPresented("TEST0"); // due to its normalized.

        createTableWithImplicitSchema("test1");

        assertTablesInSchemasPresented("TEST0", "TEST1");

        createTableWithImplicitSchema("\"TestCase\"");

        assertTablesInSchemasPresented("TEST0", "TEST1", "TestCase");

        createTableWithImplicitSchema("\"TEST0\"");

        assertTablesInSchemasPresented("TEST0", "TEST1", "TestCase");

        createTableWithImplicitSchema("\"TEST1\"");

        assertTablesInSchemasPresented("TEST0", "TEST1", "TestCase");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                createTableWithImplicitSchema("TestCase"); // due to normalization it is converted to "TESTCASE".

                return null;
            }
        }, SQLException.class, null);
    }

    /**
     * @param schema Schema name.
     * @throws SQLException If failed.
     */
    private void checkSchemaConnection(String schema) throws SQLException {
        try (Connection conn = connect(schema)) {
            Statement stmt = conn.createStatement();

            assertNotNull(stmt);
            assertFalse(stmt.isClosed());

            stmt.execute("select t._key, t._val from Integer t");
        }
    }

    /**
     * Create table with schema that is specified in connection properties.
     *
     * @param schema Schema.
     */
    private void createTableWithImplicitSchema(String schema) throws SQLException {
        try (Connection conn = connect(schema)) {
            execute(conn, "CREATE TABLE IF NOT EXISTS TAB (id INT PRIMARY KEY, val INT);");
            execute(conn, "CREATE TABLE IF NOT EXISTS TAB (id INT PRIMARY KEY, val INT);");
            execute(conn, "CREATE TABLE IF NOT EXISTS TAB (newId VARCHAR PRIMARY KEY, val VARCHAR);");
            execute(conn, "CREATE TABLE IF NOT EXISTS TAB (newId VARCHAR PRIMARY KEY, val VARCHAR);");
        }
    }

    /**
     * Shortcut to execute prepared statement.
     */
    private void execute(Connection conn, String sql) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.execute();
        }
    }

    /**
     * Retrieves list of schemas, each schema contain test table "TAB".
     */
    List<String> getSchemasWithTestTable() throws SQLException {
        try (Connection conn = connect("PUBLIC")) {
            Statement stmt = conn.createStatement();

            ArrayList<String> schemasWithTab = new ArrayList<>();

            try (ResultSet tabs = stmt.executeQuery(
                "SELECT SCHEMA_NAME, TABLE_NAME FROM SYS.TABLES " +
                    "WHERE TABLE_NAME = 'TAB' ORDER BY SCHEMA_NAME;")) {
                while (tabs.next())
                    schemasWithTab.add(tabs.getString("SCHEMA_NAME"));
            }

            return schemasWithTab;
        }
    }

    /**
     * Assert that table with name "TAB" is presented exactly in specified schemas. Order of specified schemas is
     * ignored.
     *
     * @param schemas Schemas.
     */
    void assertTablesInSchemasPresented(String... schemas) throws SQLException {
        Arrays.sort(schemas);

        List<String> exp = Arrays.asList(schemas);

        assertEqualsCollections(exp, getSchemasWithTestTable());
    }
}
