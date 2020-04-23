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

import java.sql.BatchUpdateException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Statement batch test.
 */
public class JdbcStatementBatchingSelfTest extends JdbcAbstractDmlStatementSelfTest {

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        jcache(0).clear();
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testDatabaseMetadataBatchSupportFlag() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        assertNotNull(meta);

        assertTrue(meta.supportsBatchUpdates());
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testBatch() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.addBatch("INSERT INTO Person(_key, id, firstName, lastName, age, data) " +
                "VALUES ('p1', 0, 'J', 'W', 250, RAWTOHEX('W'))");

            stmt.addBatch("MERGE INTO Person(_key, id, firstName, lastName, age, data) VALUES " +
                "('p1', 1, 'John', 'White', 25, RAWTOHEX('White')), " +
                "('p2', 2, 'Joe', 'Black', 35, RAWTOHEX('Black')), " +
                "('p3', 0, 'M', 'G', 4, RAWTOHEX('G'))");

            stmt.addBatch("UPDATE Person SET id = 3, firstName = 'Mike', lastName = 'Green', " +
                "age = 40, data = RAWTOHEX('Green') WHERE _key = 'p3'");

            stmt.addBatch("DELETE FROM Person WHERE _key = 'p1'");

            int[] res = stmt.executeBatch();

            assertEquals(4, res.length);
            assertEquals(1, res[0]);
            assertEquals(3, res[1]);
            assertEquals(1, res[2]);
            assertEquals(1, res[3]);
        }
    }

    /**
     * @throws SQLException If failed.
     */
    @Test
    public void testErrorAmidstBatch() throws SQLException {
        BatchUpdateException reason = (BatchUpdateException)
            GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.addBatch("INSERT INTO Person(_key, id, firstName, lastName, age, data) " +
                            "VALUES ('p1', 0, 'J', 'W', 250, RAWTOHEX('W'))");

                        stmt.addBatch("UPDATE Person SET id = 3, firstName = 'Mike', lastName = 'Green', " +
                            "age = 40, data = RAWTOHEX('Green') WHERE _key = 'p3'");

                        stmt.addBatch("SELECT id FROM Person WHERE _key = 'p1'");

                        return stmt.executeBatch();
                    }
                }
            },
            BatchUpdateException.class,
            "Given statement type does not match that declared by JDBC driver");

        // Check update counts in the exception.
        int[] counts = reason.getUpdateCounts();

        assertEquals(2, counts.length);
        assertEquals(1, counts[0]);
        assertEquals(0, counts[1]);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClearBatch() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws SQLException {
                    return stmt.executeBatch();
                }
            }, SQLException.class, "Batch is empty");

            stmt.addBatch("INSERT INTO Person(_key, id, firstName, lastName, age, data) " +
                "VALUES ('p1', 0, 'J', 'W', 250, RAWTOHEX('W'))");

            stmt.clearBatch();

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws SQLException {
                    return stmt.executeBatch();
                }
            }, SQLException.class, "Batch is empty");
        }
    }
}
