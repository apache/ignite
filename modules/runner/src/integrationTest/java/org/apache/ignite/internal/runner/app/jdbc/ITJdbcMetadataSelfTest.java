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

package org.apache.ignite.internal.runner.app.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.TableDefinition;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Metadata tests.
 */
public class ITJdbcMetadataSelfTest extends AbstractJdbcSelfTest {
    /** URL. */
    protected static final String URL = "jdbc:ignite:thin://127.0.1.1:10800";

    /** Creates tables. */
    @BeforeAll
    public static void createTables() {
        assert !clusterNodes.isEmpty();

        TableDefinition perTbl = SchemaBuilders.tableBuilder("PUBLIC", "PERSON").columns(
            SchemaBuilders.column("NAME", ColumnType.string()).asNullable().build(),
            SchemaBuilders.column("AGE", ColumnType.INT32).asNullable().build(),
            SchemaBuilders.column("ORGID", ColumnType.INT32).asNonNull().build()
        ).withPrimaryKey("ORGID").build();

        TableDefinition orgTbl = SchemaBuilders.tableBuilder("PUBLIC", "ORGANIZATION").columns(
            SchemaBuilders.column("ID", ColumnType.INT32).asNonNull().build(),
            SchemaBuilders.column("NAME", ColumnType.string()).asNullable().build(),
            SchemaBuilders.column("BIGDATA", ColumnType.decimalOf(20, 10)).asNullable().build()
        ).withPrimaryKey("ID").build();

        clusterNodes.get(0).tables().createTable(perTbl.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(perTbl, tblCh)
                .changeReplicas(1)
                .changePartitions(10)
        );

        clusterNodes.get(0).tables().createTable(orgTbl.canonicalName(), tblCh ->
            SchemaConfigurationConverter.convert(orgTbl, tblCh)
                .changeReplicas(1)
                .changePartitions(10)
        );

        Table tbl1 = clusterNodes.get(0).tables().table(perTbl.canonicalName());
        Table tbl2 = clusterNodes.get(0).tables().table(orgTbl.canonicalName());

        tbl1.recordView().insert(Tuple.create().set("ORGID", 1).set("NAME", "111").set("AGE", 111));
        tbl2.recordView().insert(Tuple.create().set("ID", 1).set("NAME", "AAA").set("BIGDATA", BigDecimal.valueOf(10)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testResultSetMetaData() throws Exception {
        Statement stmt = DriverManager.getConnection(URL).createStatement();

        ResultSet rs = stmt.executeQuery(
            "select p.name, o.id as orgId, p.age from PERSON p, ORGANIZATION o where p.orgId = o.id");

        assertNotNull(rs);

        ResultSetMetaData meta = rs.getMetaData();

        assertNotNull(meta);

        assertEquals(3, meta.getColumnCount());

        assertEquals("Person".toUpperCase(), meta.getTableName(1).toUpperCase());
        assertEquals("name".toUpperCase(), meta.getColumnName(1).toUpperCase());
        assertEquals("name".toUpperCase(), meta.getColumnLabel(1).toUpperCase());
        assertEquals(VARCHAR, meta.getColumnType(1));
        assertEquals(meta.getColumnTypeName(1), "VARCHAR");
        assertEquals(meta.getColumnClassName(1), "java.lang.String");

        assertEquals("Organization".toUpperCase(), meta.getTableName(2).toUpperCase());
        assertEquals("id".toUpperCase(), meta.getColumnName(2).toUpperCase());
        assertEquals("orgId".toUpperCase(), meta.getColumnLabel(2).toUpperCase());
        assertEquals(INTEGER, meta.getColumnType(2));
        assertEquals(meta.getColumnTypeName(2), "INTEGER");
        assertEquals(meta.getColumnClassName(2), "java.lang.Integer");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testDecimalAndDateTypeMetaData() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(
                    "select t.decimal, t.date from \"metaTest\".MetaTest as t");

            assertNotNull(rs);

            ResultSetMetaData meta = rs.getMetaData();

            assertNotNull(meta);

            assertEquals(2, meta.getColumnCount());

            assertEquals("METATEST", meta.getTableName(1).toUpperCase());
            assertEquals("DECIMAL", meta.getColumnName(1).toUpperCase());
            assertEquals("DECIMAL", meta.getColumnLabel(1).toUpperCase());
            assertEquals(DECIMAL, meta.getColumnType(1));
            assertEquals(meta.getColumnTypeName(1), "DECIMAL");
            assertEquals(meta.getColumnClassName(1), "java.math.BigDecimal");

            assertEquals("METATEST", meta.getTableName(2).toUpperCase());
            assertEquals("DATE", meta.getColumnName(2).toUpperCase());
            assertEquals("DATE", meta.getColumnLabel(2).toUpperCase());
            assertEquals(DATE, meta.getColumnType(2));
            assertEquals(meta.getColumnTypeName(2), "DATE");
            assertEquals(meta.getColumnClassName(2), "java.sql.Date");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetTables() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getTables("IGNITE", "PUBLIC", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            rs = meta.getTables("IGNITE", "PUBLIC", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            rs = meta.getTables("IGNITE", "PUBLIC", "", new String[]{"WRONG"});
            assertFalse(rs.next());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns("IGNITE", "PUBLIC", "PERSON", "%");

            assertNotNull(rs);

            Collection<String> names = new ArrayList<>(2);

            names.add("NAME");
            names.add("AGE");
            names.add("ORGID");

            int cnt = 0;

            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");

                assertTrue(names.remove(name));

                if ("NAME".equals(name)) {
                    assertEquals(VARCHAR, rs.getInt("DATA_TYPE"));
                    assertEquals(rs.getString("TYPE_NAME"), "VARCHAR");
                    assertEquals(1, rs.getInt("NULLABLE"));
                } else if ("AGE".equals(name)) {
                    assertEquals(INTEGER, rs.getInt("DATA_TYPE"));
                    assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
                    assertEquals(1, rs.getInt("NULLABLE"));
                } else if ("ORGID".equals(name)) {
                    assertEquals(INTEGER, rs.getInt("DATA_TYPE"));
                    assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
                    assertEquals(0, rs.getInt("NULLABLE"));

                }
                cnt++;
            }

            assertTrue(names.isEmpty());
            assertEquals(3, cnt);

            rs = meta.getColumns("IGNITE", "PUBLIC", "ORGANIZATION", "%");

            assertNotNull(rs);

            names.add("ID");
            names.add("NAME");
            names.add("BIGDATA");

            cnt = 0;

            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");

                assertTrue(names.remove(name));

                if ("ID".equals(name)) {
                    assertEquals(INTEGER, rs.getInt("DATA_TYPE"));
                    assertEquals(rs.getString("TYPE_NAME"), "INTEGER");
                    assertEquals(0, rs.getInt("NULLABLE"));
                } else if ("NAME".equals(name)) {
                    assertEquals(VARCHAR, rs.getInt("DATA_TYPE"));
                    assertEquals(rs.getString("TYPE_NAME"), "VARCHAR");
                    assertEquals(1, rs.getInt("NULLABLE"));
                } else if ("BIGDATA".equals(name)) {
                    assertEquals(DECIMAL, rs.getInt("DATA_TYPE"));
                    assertEquals(rs.getString("TYPE_NAME"), "DECIMAL");
                    assertEquals(1, rs.getInt("NULLABLE"));
                    assertEquals(10, rs.getInt("DECIMAL_DIGITS"));
                    assertEquals(20, rs.getInt("COLUMN_SIZE"));
                }

                cnt++;
            }

            assertTrue(names.isEmpty());
            assertEquals(3, cnt);
        }
    }

    /**
     * Check JDBC support flags.
     */
    @Test
    public void testCheckSupports() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertTrue(meta.supportsANSI92EntryLevelSQL());
            assertTrue(meta.supportsAlterTableWithAddColumn());
            assertTrue(meta.supportsAlterTableWithDropColumn());
            assertTrue(meta.nullPlusNonNullIsNull());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersions() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertEquals(conn.getMetaData().getDatabaseProductVersion(), ProtocolVersion.LATEST_VER.toString(),
                "Unexpected ignite database product version.");
            assertEquals(conn.getMetaData().getDriverVersion(), ProtocolVersion.LATEST_VER.toString(),
                "Unexpected ignite driver version.");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSchemasMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getSchemas();

            Set<String> expectedSchemas = new HashSet<>(Arrays.asList("PUBLIC", "PUBLIC"));

            Set<String> schemas = new HashSet<>();

            while (rs.next())
                schemas.add(rs.getString(1));

            assertEquals(schemas, expectedSchemas);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEmptySchemasMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getSchemas(null, "qqq");

            assertFalse(rs.next(), "Empty result set is expected");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrimaryKeyMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL);
             ResultSet rs = conn.getMetaData().getPrimaryKeys(null, "PUBLIC", "PERSON")) {

            int cnt = 0;

            while (rs.next()) {
                assertEquals(rs.getString("COLUMN_NAME"), "ORGID");

                cnt++;
            }

            assertEquals(1, cnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllPrimaryKeys() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, null);

            Set<String> expectedPks = new HashSet<>(Arrays.asList(
                "PUBLIC.ORGANIZATION.PK_ORGANIZATION.ID",
                "PUBLIC.PERSON.PK_PERSON.ORGID"));

            Set<String> actualPks = new HashSet<>(expectedPks.size());

            while (rs.next()) {
                actualPks.add(rs.getString("TABLE_SCHEM") +
                    '.' + rs.getString("TABLE_NAME") +
                    '.' + rs.getString("PK_NAME") +
                    '.' + rs.getString("COLUMN_NAME"));
            }

            assertEquals(expectedPks, actualPks, "Metadata contains unexpected primary keys info.");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalidCatalog() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getSchemas("q", null);

            assertFalse(rs.next(), "Results must be empty");

            rs = meta.getTables("q", null, null, null);

            assertFalse(rs.next(), "Results must be empty");

            rs = meta.getColumns("q", null, null, null);

            assertFalse(rs.next(), "Results must be empty");

            rs = meta.getIndexInfo("q", null, null, false, false);

            assertFalse(rs.next(), "Results must be empty");

            rs = meta.getPrimaryKeys("q", null, null);

            assertFalse(rs.next(), "Results must be empty");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetTableTypes() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getTableTypes();

            assertTrue(rs.next());

            assertEquals("TABLE", rs.getString("TABLE_TYPE"));

            assertFalse(rs.next());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Disabled
    public void testParametersMetadata() throws Exception {
        // Perform checks few times due to query/plan caching.
        for (int i = 0; i < 3; i++) {
            // No parameters statement.
            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement noParams = conn.prepareStatement("select * from Person;");
                ParameterMetaData params = noParams.getParameterMetaData();

                assertEquals(0, params.getParameterCount(), "Parameters should be empty.");
            }

            // Selects.
            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement selectStmt = conn.prepareStatement("select orgId from Person p where p.name > ? and p.orgId > ?");

                ParameterMetaData meta = selectStmt.getParameterMetaData();

                assertNotNull(meta);

                assertEquals(2, meta.getParameterCount());

                assertEquals(VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(INTEGER, meta.getParameterType(2));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));
            }

            // Updates.
            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement updateStmt = conn.prepareStatement("update Person p set orgId = 42 where p.name > ? and p.orgId > ?");

                ParameterMetaData meta = updateStmt.getParameterMetaData();

                assertNotNull(meta);

                assertEquals(2, meta.getParameterCount());

                assertEquals(VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(INTEGER, meta.getParameterType(2));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));
            }

            // Multistatement
            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement updateStmt = conn.prepareStatement(
                    "update Person p set orgId = 42 where p.name > ? and p.orgId > ?;" +
                        "select orgId from Person p where p.name > ? and p.orgId > ?");

                ParameterMetaData meta = updateStmt.getParameterMetaData();

                assertNotNull(meta);

                assertEquals(4, meta.getParameterCount());

                assertEquals(VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(INTEGER, meta.getParameterType(2));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));

                assertEquals(VARCHAR, meta.getParameterType(3));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(3));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(3));

                assertEquals(INTEGER, meta.getParameterType(4));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(4));
            }
        }
    }

    /**
     * Check that parameters metadata throws correct exception on non-parsable statement.
     */
    @Test
    public void testParametersMetadataNegative() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setSchema("\"pers\"");

            PreparedStatement notCorrect = conn.prepareStatement("select * from NotExistingTable;");

            assertThrows(SQLException.class, notCorrect::getParameterMetaData, "Table \"NOTEXISTINGTABLE\" not found");
        }
    }

    /**
     * Negative scenarios for catalog name.
     * Perform metadata lookups, that use incorrect catalog names.
     */
    @Test
    public void testCatalogWithNotExistingName() throws SQLException {
        checkNoEntitiesFoundForCatalog("");
        checkNoEntitiesFoundForCatalog("NOT_EXISTING_CATALOG");
    }

    /**
     * Check that lookup in the metadata have been performed using specified catalog name (that is neither {@code null}
     * nor correct catalog name), empty result set is returned.
     *
     * @param invalidCat catalog name that is not either
     */
    private void checkNoEntitiesFoundForCatalog(String invalidCat) throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            // Intention: we set the other arguments that way, the values to have as many results as possible.
            assertIsEmpty(meta.getTables(invalidCat, null, "%", new String[] {"TABLE"}));
            assertIsEmpty(meta.getColumns(invalidCat, null, "%", "%"));
            assertIsEmpty(meta.getColumnPrivileges(invalidCat, "pers", "PERSON", "%"));
            assertIsEmpty(meta.getTablePrivileges(invalidCat, null, "%"));
            assertIsEmpty(meta.getPrimaryKeys(invalidCat, "pers", "PERSON"));
            assertIsEmpty(meta.getImportedKeys(invalidCat, "pers", "PERSON"));
            assertIsEmpty(meta.getExportedKeys(invalidCat, "pers", "PERSON"));
            // meta.getCrossReference(...) doesn't make sense because we don't have FK constraint.
            assertIsEmpty(meta.getIndexInfo(invalidCat, null, "%", false, true));
            assertIsEmpty(meta.getSuperTables(invalidCat, "%", "%"));
            assertIsEmpty(meta.getSchemas(invalidCat, null));
            assertIsEmpty(meta.getPseudoColumns(invalidCat, null, "%", ""));
        }
    }

    /**
     * Assert that specified ResultSet contains no rows.
     *
     * @param rs result set to check.
     * @throws SQLException on error.
     */
    private static void assertIsEmpty(ResultSet rs) throws SQLException {
        try (rs) {
            boolean empty = !rs.next();

            assertTrue(empty, "Result should be empty because invalid catalog is specified.");
        }
    }
}
