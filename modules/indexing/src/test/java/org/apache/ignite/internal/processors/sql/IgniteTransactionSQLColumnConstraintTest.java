package org.apache.ignite.internal.processors.sql;

import java.math.BigDecimal;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;

public class IgniteTransactionSQLColumnConstraintTest extends IgniteSQLColumnConstraintsTest {
    /** */
    @Override protected void checkSQLThrows(String sql, String sqlStateCode, Object... args) {
        runSQL("BEGIN TRANSACTION");

        IgniteSQLException err = (IgniteSQLException)GridTestUtils.assertThrowsWithCause(() -> {
            runSQL(sql, args);

            return 0;
        }, IgniteSQLException.class);

        runSQL("ROLLBACK TRANSACTION");

        assertEquals(err.sqlState(), sqlStateCode);
    }

    /** */
    @Override protected List<?> execSQL(String sql, Object... args) {
        runSQL("BEGIN TRANSACTION");

        List<?> res = runSQL(sql, args);

        runSQL("COMMIT TRANSACTION");

        return res;
    }

    /**
     * That test is ignored due to drop column(s) operation is unsupported while MVCC is enabled.
     */
    public void testCharDropColumnWithConstraint() {
        // No-op.
    }

    /**
     * That test is ignored due to drop column(s) operation is unsupported while MVCC is enabled.
     */
    public void testDecimalDropColumnWithConstraint() {
        // No-op.
    }

    @Override protected boolean mvccEnabled() {
        return true;
    }
}
