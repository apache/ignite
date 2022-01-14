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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Verifies that primary keys in the metadata are valid.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ItJdbcMetadataPrimaryKeysSelfTest extends AbstractJdbcSelfTest {
    /** COLUMN_NAME column index in the metadata table. */
    private static final int COL_NAME_IDX = 4;

    /** {@inheritDoc} */
    @AfterEach
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        executeUpdate("DROP TABLE IF EXISTS PUBLIC.TEST;");
    }

    /**
     * Checks for PK that contains single field.
     */
    @Test
    public void testSingleKey() throws Exception {
        executeUpdate("CREATE TABLE PUBLIC.TEST (ID INT PRIMARY KEY, NAME VARCHAR);");

        checkPkFields("TEST", "ID");
    }

    /**
     * Checks for composite (so implicitly wrapped) primary key.
     */
    @Test
    public void testCompositeKey() throws Exception {
        executeUpdate("CREATE TABLE PUBLIC.TEST ("
                + "ID INT, "
                + "SEC_ID INT, "
                + "NAME VARCHAR, "
                + "PRIMARY KEY (ID, SEC_ID));");

        checkPkFields("TEST", "ID", "SEC_ID");
    }

    /**
     * Execute update sql operation using new connection.
     *
     * @param sql update SQL query.
     * @return update count.
     * @throws SQLException on error.
     */
    private int executeUpdate(String sql) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            return stmt.executeUpdate();
        }
    }

    /**
     * Checks that field names in the metadata matches specified expected fields.
     *
     * @param tabName part of the sql query after CREATE TABLE TESTER.
     * @param expPkFields Expected primary key fields.
     */
    private void checkPkFields(String tabName, String... expPkFields) throws Exception {
        DatabaseMetaData md = conn.getMetaData();

        ResultSet rs = md.getPrimaryKeys(conn.getCatalog(), null, tabName);

        List<String> colNames = new ArrayList<>();

        while (rs.next()) {
            colNames.add(rs.getString(COL_NAME_IDX));
        }

        assertEquals(Arrays.asList(expPkFields), colNames, "Field names in the primary key are not correct");
    }
}
