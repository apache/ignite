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

package org.apache.ignite.cache.store.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for {@link JdbcTypesDefaultTransformer}.
 */
public class JdbcTypesDefaultTransformerTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testTransformer() throws Exception {
        // Connection to H2.
        String jdbcUrl = "jdbc:h2:mem:JdbcTypesDefaultTransformerTest";
        String usr = "sa";
        String pwd = "";

        // Connection to Oracle.
        // -Duser.region=us -Duser.language=en
//        Class.forName("oracle.jdbc.OracleDriver");
//        String jdbcUrl = "jdbc:oracle:thin:@localhost:1521:XE";
//        String usr = "test";
//        String pwd = "test";

        // Connection to MS SQL.
//        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
//        String jdbcUrl = "jdbc:sqlserver://localhost;databaseName=master";
//        String usr = "test";
//        String pwd = "test";

        // Connection to DB2.
//        Class.forName("com.ibm.db2.jcc.DB2Driver");
//        String jdbcUrl = "jdbc:db2://localhost:50000/sample";
//        String usr = "test";
//        String pwd = "test";

        // Connection to Postgre SQL.
//        Class.forName("org.postgresql.Driver");
//        String jdbcUrl = "jdbc:postgresql://localhost:5433/postgres";
//        String usr = "test";
//        String pwd = "test";

        // Connection to My SQL.
//        Class.forName("com.mysql.jdbc.Driver");
//        String jdbcUrl = "jdbc:mysql://localhost:3306/test";
//        String usr = "test";
//        String pwd = "test";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, usr, pwd)) {
            Statement stmt = conn.createStatement();

            try {
                stmt.executeUpdate("DROP TABLE TEST_TRANSFORMER");
            }
            catch (SQLException ignored) {
                // No-op.
            }

            // Create table in H2.
            stmt.executeUpdate("CREATE TABLE TEST_TRANSFORMER(id INTEGER, " +
                "c1 BOOLEAN, c2 INTEGER, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 DECIMAL(20, 2), c7 DOUBLE PRECISION, c8 REAL, " +
                "c9 TIME, c10 DATE, c11 TIMESTAMP, c12 VARCHAR(100), c13 UUID)");

            // Create table in ORACLE.
//                stmt.executeUpdate("CREATE TABLE TEST_TRANSFORMER(id INTEGER, " +
//                    "c1 NUMBER(1), c2 INTEGER, c3 NUMBER(3), c4 NUMBER(4), c5 NUMBER(20), c6 NUMBER(20, 2), c7 NUMBER(20, 2), c8 NUMBER(10, 2), " +
//                    "c9 TIMESTAMP, c10 DATE, c11 TIMESTAMP, c12 VARCHAR(100), c13 VARCHAR(36))");

            // Create table in MS SQL.
//            stmt.executeUpdate("CREATE TABLE TEST_TRANSFORMER(id INTEGER, " +
//                "c1 BIT, c2 INTEGER, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 DECIMAL(20, 2), c7 DOUBLE PRECISION, c8 REAL, " +
//                "c9 TIME, c10 DATE, c11 DATETIME, c12 VARCHAR(100), c13 VARCHAR(36))");

            // Create table in DB2.
//            stmt.executeUpdate("CREATE TABLE TEST_TRANSFORMER(id INTEGER, " +
//                "c1 SMALLINT , c2 INTEGER, c3 SMALLINT , c4 SMALLINT, c5 BIGINT, c6 DECIMAL(20, 2), c7 DOUBLE PRECISION, c8 REAL, " +
//                "c9 TIME, c10 DATE, c11 TIMESTAMP, c12 VARCHAR(100), c13 VARCHAR(36))");

            // Create table in Postgre SQL.
//            stmt.executeUpdate("CREATE TABLE TEST_TRANSFORMER(id INTEGER, " +
//                "c1 BOOLEAN, c2 INTEGER, c3 SMALLINT, c4 SMALLINT, c5 BIGINT, c6 DECIMAL(20, 2), c7 DOUBLE PRECISION, c8 REAL, " +
//                "c9 TIME, c10 DATE, c11 TIMESTAMP, c12 VARCHAR(100), c13 UUID)");

            // Create table in MySQL.
//            stmt.executeUpdate("CREATE TABLE TEST_TRANSFORMER(id INTEGER, " +
//                "c1 BOOLEAN, c2 INTEGER, c3 TINYINT, c4 SMALLINT, c5 BIGINT, c6 DECIMAL(20, 2), c7 DOUBLE PRECISION, c8 REAL, " +
//                "c9 TIME, c10 DATE, c11 TIMESTAMP(3), c12 VARCHAR(100), c13 VARCHAR(36))");

            // Add data to H2, Postgre SQL and MySQL.
            stmt.executeUpdate("INSERT INTO TEST_TRANSFORMER(id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) " +
                "VALUES (1, true, 1, 2, 3, 4, 5.35, 6.15, 7.32, '00:01:08', '2016-01-01', '2016-01-01 00:01:08.296', " +
                "'100', '736bc956-090c-40d2-94da-916f2161f8a2')");
            stmt.executeUpdate("INSERT INTO TEST_TRANSFORMER(id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) " +
                "VALUES (2, false, 10, 20, 30, 40, 50, 60, 70, current_time, current_date, current_timestamp, " +
                "'100.55', '736bc956-090c-40d2-94da-916f2161cdea')");

            // Add data to Oracle.
//            stmt.executeUpdate("INSERT INTO TEST_TRANSFORMER(id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) " +
//                "VALUES (1, 1, 1, 2, 3, 4, 5.35, 6.15, 7.32, " +
//                "TO_TIMESTAMP('2016-01-01 00:01:08', 'YYYY-MM-DD HH24:MI:SS'), " +
//                "TO_DATE('2016-01-01', 'YYYY-MM-DD')," +
//                "TO_TIMESTAMP('2016-01-01 00:01:08.296', 'YYYY-MM-DD HH24:MI:SS.FF3'), " +
//                "'100', '736bc956-090c-40d2-94da-916f2161f8a2')");
//            stmt.executeUpdate("INSERT INTO TEST_TRANSFORMER(id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) " +
//                "VALUES (2, 0, 10, 20, 30, 40, 50, 60, 70," +
//                "TO_TIMESTAMP('2016-01-01 00:01:08', 'YYYY-MM-DD HH24:MI:SS'), " +
//                "TO_DATE('2016-01-01', 'YYYY-MM-DD')," +
//                "TO_TIMESTAMP('2016-01-01 00:01:08.296', 'YYYY-MM-DD HH24:MI:SS.FF3'), " +
//                "'100.55', '736bc956-090c-40d2-94da-916f2161cdea')");

            // Add data to MS SQL or IBM DB2.
//            stmt.executeUpdate("INSERT INTO TEST_TRANSFORMER(id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) " +
//                "VALUES (1, 1, 1, 2, 3, 4, 5.35, 6.15, 7.32, '00:01:08', '2016-01-01', '2016-01-01 00:01:08.296', " +
//                "'100', '736bc956-090c-40d2-94da-916f2161f8a2')");
//            stmt.executeUpdate("INSERT INTO TEST_TRANSFORMER(id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) " +
//                "VALUES (2, 0, 10, 20, 30, 40, 50, 60, 70, '00:01:08', '2016-01-01', '2016-01-01 00:01:08.296', " +
//                "'100.55', '736bc956-090c-40d2-94da-916f2161cdea')");

            stmt.executeUpdate("INSERT INTO TEST_TRANSFORMER(id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) " +
                "VALUES (3, null, null, null, null, null, null, null, null, null, null, null, null, null)");

            ResultSet rs = stmt.executeQuery("select c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13 from TEST_TRANSFORMER order by id");

            assertTrue(rs.next());

            JdbcTypesDefaultTransformer transformer = JdbcTypesDefaultTransformer.INSTANCE;

            // c1: Test BOOLEAN column.
            assertTrue((Boolean)transformer.getColumnValue(rs, 1, boolean.class));
            assertTrue((Boolean)transformer.getColumnValue(rs, 1, Boolean.class));
            assertEquals(1, transformer.getColumnValue(rs, 1, int.class));
            assertEquals(1.0, transformer.getColumnValue(rs, 1, double.class));

            String s = (String)transformer.getColumnValue(rs, 1, String.class);
            assertTrue("true".equalsIgnoreCase(s) || "1".equals(s) || "t".equalsIgnoreCase(s));

            // c2: Test INTEGER column.
            assertEquals(1, transformer.getColumnValue(rs, 2, int.class));
            assertEquals(1, transformer.getColumnValue(rs, 2, Integer.class));
            assertEquals(1L, transformer.getColumnValue(rs, 2, Long.class));
            assertEquals(1.0, transformer.getColumnValue(rs, 2, double.class));
            assertEquals("1", transformer.getColumnValue(rs, 2, String.class));

            // c3: Test TINYINT column.
            byte b = 2;
            assertEquals(b, transformer.getColumnValue(rs, 3, byte.class));
            assertEquals(b, transformer.getColumnValue(rs, 3, Byte.class));
            assertEquals(2.0, transformer.getColumnValue(rs, 3, double.class));
            assertEquals("2", transformer.getColumnValue(rs, 3, String.class));

            // c4: Test SMALLINT column.
            short shrt = 3;
            assertEquals(shrt, transformer.getColumnValue(rs, 4, short.class));
            assertEquals(shrt, transformer.getColumnValue(rs, 4, Short.class));
            assertEquals(3.0, transformer.getColumnValue(rs, 4, double.class));
            assertEquals("3", transformer.getColumnValue(rs, 4, String.class));

            // c5: Test BIGINT column.
            assertEquals(4L, transformer.getColumnValue(rs, 5, long.class));
            assertEquals(4L, transformer.getColumnValue(rs, 5, Long.class));
            assertEquals(4, transformer.getColumnValue(rs, 5, int.class));
            assertEquals(4, transformer.getColumnValue(rs, 5, Integer.class));
            assertEquals(4.0, transformer.getColumnValue(rs, 5, double.class));
            assertEquals("4", transformer.getColumnValue(rs, 5, String.class));
            assertEquals(new BigDecimal("4"), transformer.getColumnValue(rs, 5, BigDecimal.class));

            // c6: Test DECIMAL column.
            assertEquals(new BigDecimal("5.35"), transformer.getColumnValue(rs, 6, BigDecimal.class));
            assertEquals(5L, transformer.getColumnValue(rs, 6, long.class));
            assertEquals("5.35", transformer.getColumnValue(rs, 6, String.class));

            // c7: Test DOUBLE column.
            assertEquals(6.15, transformer.getColumnValue(rs, 7, double.class));
            assertEquals(6.15, transformer.getColumnValue(rs, 7, Double.class));
            assertEquals(6, transformer.getColumnValue(rs, 7, int.class));
            assertEquals(6, transformer.getColumnValue(rs, 7, Integer.class));
            assertTrue(transformer.getColumnValue(rs, 7, String.class).toString().startsWith("6.15"));

            // c8: Test REAL column.
            assertTrue((7.32f - (Float)transformer.getColumnValue(rs, 8, float.class)) < 0.01);
            assertTrue((7.32f - (Float)transformer.getColumnValue(rs, 8, Float.class)) < 0.01);
            assertTrue((7.32 - (Double)transformer.getColumnValue(rs, 8, double.class)) < 0.01);
            assertTrue((7.32 - (Double)transformer.getColumnValue(rs, 8, Double.class)) < 0.01);
            assertEquals(7, transformer.getColumnValue(rs, 8, int.class));
            assertEquals(7, transformer.getColumnValue(rs, 8, Integer.class));
            assertTrue(transformer.getColumnValue(rs, 8, String.class).toString().startsWith("7.32"));

            // c9: Test TIME column.
            assertTrue(transformer.getColumnValue(rs, 9, Time.class) instanceof Time);
            assertTrue(transformer.getColumnValue(rs, 9, String.class).toString().contains("00:01:08"));

            // c10: Test DATE column.
            assertTrue(transformer.getColumnValue(rs, 10, Date.class) instanceof Date);
            assertTrue(transformer.getColumnValue(rs, 10, String.class).toString().startsWith("2016-01-01"));

            // c11: Test TIMESTAMP column.
            transformer.getColumnValue(rs, 11, Timestamp.class);
            assertTrue(transformer.getColumnValue(rs, 11, String.class).toString().startsWith("2016-01-01 00:01:08.29"));

            // c12: Test VARCHAR column.
            assertEquals("100", transformer.getColumnValue(rs, 12, String.class));
            assertEquals(100, transformer.getColumnValue(rs, 12, int.class));

            // c13: Test UUID column.
            transformer.getColumnValue(rs, 13, UUID.class);
            assertEquals("736bc956-090c-40d2-94da-916f2161f8a2", transformer.getColumnValue(rs, 13, String.class));

            assertTrue(rs.next());

            // Test BOOLEAN column.
            assertFalse((Boolean)transformer.getColumnValue(rs, 1, boolean.class));
            assertFalse((Boolean)transformer.getColumnValue(rs, 1, Boolean.class));
            assertEquals(0, transformer.getColumnValue(rs, 1, int.class));
            assertEquals(0.0, transformer.getColumnValue(rs, 1, double.class));

            s = (String)transformer.getColumnValue(rs, 1, String.class);
            assertTrue("false".equalsIgnoreCase(s) || "0".equals(s) || "f".equalsIgnoreCase(s));

            assertTrue(rs.next());

            // Check how null values will be transformed.
            assertNotNull(transformer.getColumnValue(rs, 1, boolean.class));
            assertNull(transformer.getColumnValue(rs, 1, Boolean.class));

            assertEquals(0, transformer.getColumnValue(rs, 2, int.class));
            assertNull(transformer.getColumnValue(rs, 2, Integer.class));

            assertEquals((byte)0, transformer.getColumnValue(rs, 3, byte.class));
            assertNull(transformer.getColumnValue(rs, 3, Byte.class));

            assertEquals((short)0, transformer.getColumnValue(rs, 4, short.class));
            assertNull(transformer.getColumnValue(rs, 4, Short.class));

            assertEquals(0L, transformer.getColumnValue(rs, 5, long.class));
            assertNull(transformer.getColumnValue(rs, 5, Long.class));

            assertNull(transformer.getColumnValue(rs, 6, BigDecimal.class));

            assertEquals(0d, transformer.getColumnValue(rs, 7, double.class));
            assertNull(transformer.getColumnValue(rs, 7, Double.class));

            assertEquals(0f, transformer.getColumnValue(rs, 8, float.class));
            assertNull(transformer.getColumnValue(rs, 8, Float.class));

            assertNull(transformer.getColumnValue(rs, 9, Time.class));

            assertNull(transformer.getColumnValue(rs, 10, Date.class));

            assertNull(transformer.getColumnValue(rs, 11, Timestamp.class));

            assertNull(transformer.getColumnValue(rs, 12, String.class));

            assertNull(transformer.getColumnValue(rs, 13, UUID.class));
        }
    }
}
