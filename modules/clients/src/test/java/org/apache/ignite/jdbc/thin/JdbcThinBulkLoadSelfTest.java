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

import org.apache.ignite.testframework.GridTestUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

/**
 * Statement test.
 */
public class JdbcThinBulkLoadSelfTest extends JdbcThinAbstractDmlStatementSelfTest {
    /** SQL query. */
    private static final String SQL_PREPARED = "insert into Person(_key, id, firstName, lastName, age) values " +
        "(?, ?, ?, ?, ?)";

    /** Statement. */
    private Statement stmt;

    /** Prepared statement. */
    private PreparedStatement pstmt;

    private String BULKLOAD0_CSV_FILE = System.getProperty("IGNITE_HOME") +
        "/modules/clients/src/test/resources/bulkload0.csv";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stmt = conn.createStatement();

        pstmt = conn.prepareStatement(SQL_PREPARED);

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());

        assertNotNull(pstmt);
        assertFalse(pstmt.isClosed());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed())
            stmt.close();

        if (pstmt != null && !pstmt.isClosed())
            pstmt.close();

        assertTrue(pstmt.isClosed());
        assertTrue(stmt.isClosed());

        super.afterTest();
    }

    /**
     * @throws SQLException If failed.
     *
     * FIXME SHQ: Tests to add
     * Missing table
     * Inserting _key, _value of a wrong type
     * Missing file
     * Read error in the middle of reading a file
     *
     */
    public void testWrongFileName() {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                stmt.executeUpdate(
                    "copy from \"nonexistent\" into Person (_key, firstName, lastName) format csv");

                return null;
            }
        }, SQLException.class, "Failed to read file: 'nonexistent'");
    }

    /**
     * @throws SQLException If failed.
     *
     * FIXME SHQ: Tests to add
     * Missing table
     * Inserting _key, _value of a wrong type
     * Missing file
     * Read error in the middle of reading a file
     *
     */
    public void testBatch() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person (_key, firstName, lastName) format csv");

        assertEquals(2, updatesCnt);

        checkBulkload0CsvCacheContents();
    }

    /**
     * @throws SQLException If failed.
     *
     * FIXME SHQ: Tests to add
     * Missing table
     * Inserting _key, _value of a wrong type
     * Missing file
     * Read error in the middle of reading a file
     *
     */
    public void testBatchSize_1() throws SQLException {
        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + BULKLOAD0_CSV_FILE + "\" into Person (_key, firstName, lastName) format csv batch_size 1");

        assertEquals(2, updatesCnt);

        checkBulkload0CsvCacheContents();
    }

    private void checkBulkload0CsvCacheContents() throws SQLException {
        ResultSet rs = stmt.executeQuery("select _key, firstName, lastName from Person");

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("_key");

            if (id == 123) {
                assertEquals("FirstName123 MiddleName123", rs.getString("firstName"));
                assertEquals("LastName123", rs.getString("lastName"));
            }
            else if (id == 456) {
                assertEquals("FirstName456", rs.getString("firstName"));
                assertEquals("LastName456", rs.getString("lastName"));
            }
            else
                fail("Wrong ID: " + id);

            cnt++;
        }

        assertEquals(2, cnt);
    }
}
