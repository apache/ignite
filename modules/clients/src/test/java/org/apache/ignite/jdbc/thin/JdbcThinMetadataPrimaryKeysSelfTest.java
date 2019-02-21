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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Verifies that primary keys in the metadata are valid.
 */
public class JdbcThinMetadataPrimaryKeysSelfTest extends GridCommonAbstractTest {
    /** Url. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** COLUMN_NAME column index in the metadata table. */
    private static final int COL_NAME_IDX = 4;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(1);
    }

    /**
     * Execute update sql operation using new connection.
     *
     * @param sql update SQL query.
     * @return update count.
     * @throws SQLException on error.
     */
    private int executeUpdate(String sql) throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                return stmt.executeUpdate();
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS TEST;");
    }

    /**
     * Checks for PK that contains single unwrapped field.
     */
    @Test
    public void testSingleUnwrappedKey() throws Exception {
        executeUpdate("CREATE TABLE TEST (ID LONG PRIMARY KEY, NAME VARCHAR);");

        checkPKFields("TEST", "ID");
    }

    /**
     * Checks for PK that contains single field. Key is forcibly wrapped.
     */
    @Test
    public void testSingleWrappedKey() throws Exception {
        executeUpdate("CREATE TABLE TEST (" +
            "ID LONG PRIMARY KEY, " +
            "NAME VARCHAR) " +
            "WITH \"wrap_key=true\";");

        checkPKFields("TEST", "ID");
    }

    /**
     * Checks for composite (so implicitly wrapped) primary key.
     */
    @Test
    public void testCompositeKey() throws Exception {
        executeUpdate("CREATE TABLE TEST (" +
            "ID LONG, " +
            "SEC_ID LONG, " +
            "NAME VARCHAR, " +
            "PRIMARY KEY (ID, SEC_ID));");

        checkPKFields("TEST", "ID", "SEC_ID");
    }

    /**
     * Checks for composite (so implicitly wrapped) primary key. Additionally, affinity key is used.
     */
    @Test
    public void testCompositeKeyWithAK() throws Exception {
        final String tpl = "CREATE TABLE TEST (" +
            "ID LONG, " +
            "SEC_ID LONG, " +
            "NAME VARCHAR, " +
            "PRIMARY KEY (ID, SEC_ID)) " +
            "WITH \"affinity_key=%s\";";

        executeUpdate(String.format(tpl, "ID"));

        checkPKFields("TEST", "ID", "SEC_ID");

        executeUpdate("DROP TABLE TEST;");

        executeUpdate(String.format(tpl, "SEC_ID"));

        checkPKFields("TEST", "ID", "SEC_ID");
    }

    /**
     * Checks that field names in the metadata matches specified expected fields.
     *
     * @param tabName part of the sql query after CREATE TABLE TESTER.
     * @param expPKFields Expected primary key fields.
     */
    private void checkPKFields(String tabName, String... expPKFields) throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData md = conn.getMetaData();

            ResultSet rs = md.getPrimaryKeys(conn.getCatalog(), null, tabName);

            List<String> colNames = new ArrayList<>();

            while (rs.next())
                colNames.add(rs.getString(COL_NAME_IDX));

            assertEquals("Field names in the primary key are not correct",
                Arrays.asList(expPKFields), colNames);
        }
    }
}
