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

import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.testframework.GridTestUtils;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertArrayEquals;

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
    public void testBatch() throws SQLException {
        final int BATCH_SIZE = 10;

        String csvFileName = System.getProperty("IGNITE_HOME") + "/modules/clients/src/test/resources/bulkload0.csv";

        int updatesCnt = stmt.executeUpdate(
            "copy from \"" + csvFileName + "\" into Person (_key, firstName, lastName) format csv");

        assertEquals(2, updatesCnt);
    }

    /**
     * @param beginIndex Begin row index.
     * @param cnt Count of rows.
     * @return String contains values for 'cnt' rows.
     */
    private String generateValues(int beginIndex, int cnt) {
        StringBuilder sb = new StringBuilder();

        int lastIdx = beginIndex + cnt - 1;

        for (int i = beginIndex; i < lastIdx; ++i)
            sb.append(valuesRow(i)).append(',');

        sb.append(valuesRow(lastIdx));

        return sb.toString();
    }

    /**
     * @param idx Index of the row.
     * @return String with row values.
     */
    private String valuesRow(int idx) {
        return String.format("('p%d', %d, 'Name%d', 'Lastname%d', %d)", idx, idx, idx, idx, 20 + idx);
    }
}
