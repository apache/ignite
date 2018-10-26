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
import java.sql.SQLException;

import static org.apache.ignite.internal.processors.odbc.SqlStateCode.TRANSACTION_STATE_EXCEPTION;

/** */
public class JdbcMvccDisabledTxCommandsTest extends JdbcThinAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception if failed.
     */
    public void testBeginFailsWhenMvccIsDisabled() throws Exception {
        try (Connection conn = connect(grid(0), "")) {
            conn.createStatement().execute("BEGIN");

            fail("Exeception is expected");
        }
        catch (SQLException e) {
            assertEquals(TRANSACTION_STATE_EXCEPTION, e.getSQLState());
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testAutoCommitChangeIgnoredWhenMvccIsDisabled() throws Exception {
        try (Connection conn = connect(grid(0), "")) {
            boolean ac0 = conn.getAutoCommit();

            conn.setAutoCommit(!ac0);

            conn.setAutoCommit(ac0);
        }
        // assert no exception
    }

    /**
     * @throws Exception if failed.
     */
    public void testCommitIgnoredWhenMvccIsDisabled() throws Exception {
        try (Connection conn = connect(grid(0), "")) {
            conn.createStatement().execute("COMMIT");
        }
        // assert no exception
    }

    /**
     * @throws Exception if failed.
     */
    public void testRollbackIgnoredWhenMvccIsDisabled() throws Exception {
        try (Connection conn = connect(grid(0), "")) {
            conn.createStatement().execute("ROLLBACK");
        }
        // assert no exception
    }
}
