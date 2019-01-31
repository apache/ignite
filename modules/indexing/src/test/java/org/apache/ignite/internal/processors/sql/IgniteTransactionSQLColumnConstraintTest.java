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

package org.apache.ignite.internal.processors.sql;

import java.util.List;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 */
public class IgniteTransactionSQLColumnConstraintTest extends IgniteSQLColumnConstraintsTest {
    /** {@inheritDoc} */
    @Override protected void checkSQLThrows(String sql, String sqlStateCode, Object... args) {
        runSQL("BEGIN TRANSACTION");

        IgniteSQLException err = (IgniteSQLException)GridTestUtils.assertThrowsWithCause(() -> {
            runSQL(sql, args);

            return 0;
        }, IgniteSQLException.class);

        runSQL("ROLLBACK TRANSACTION");

        assertEquals(err.sqlState(), sqlStateCode);
    }

    /** {@inheritDoc} */
    @Override protected List<?> execSQL(String sql, Object... args) {
        runSQL("BEGIN TRANSACTION");

        List<?> res = runSQL(sql, args);

        runSQL("COMMIT TRANSACTION");

        return res;
    }

    /**
     * That test is ignored due to drop column(s) operation is unsupported for the MVCC tables.
     */
    @Test
    @Override public void testCharDropColumnWithConstraint() {
        // No-op.
    }

    /**
     * That test is ignored due to drop column(s) operation is unsupported for the MVCC tables.
     */
    @Test
    @Override public void testDecimalDropColumnWithConstraint() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected boolean mvccEnabled() {
        return true;
    }
}
