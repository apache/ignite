package org.apache.ignite.internal.processors.cache.index;

import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Smoke checks for explain operations.
 */
public class ExplainSelfTest extends GridCommonAbstractTest {
    /** Ignite instance. */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid(1);

        execute("CREATE TABLE TEST (ID LONG PRIMARY KEY, VAL LONG);");
    }

    /**
     * Executes sql query using native sql api.
     *
     * @param sql sql query.
     * @return fully fetched result of query.
     * @throws Exception on error.
     */
    private List<List<?>> execute(String sql) throws Exception {
        return ignite.context().query().querySqlFields(new SqlFieldsQuery(sql), false).getAll();
    }

    /**
     * Negative check that verifies EXPLAINs of update operations are not supported and cause correct exceptions.
     */
    public void testExplainUpdateOperation() {
        assertNotSupported("EXPLAIN INSERT INTO TEST VALUES (1, 2);");
        assertNotSupported("EXPLAIN UPDATE TEST SET VAL = VAL + 1;");
        assertNotSupported("EXPLAIN MERGE INTO TEST (ID, VAL) VALUES (1, 2);");
        assertNotSupported("EXPLAIN DELETE FROM TEST;");
    }

    /**
     * Check that EXPLAIN SELECT queries doesn't cause errors.
     */
    public void testExplainSelect() throws Exception {
        execute("EXPLAIN SELECT * FROM TEST;");
    }

    /**
     * Assert that specified explain slq query is not supported due to it explains update (update, delete, insert,
     * etc.). operation.
     *
     * @param explainSql explain query of update operation.
     */
    private void assertNotSupported(String explainSql) {
        Throwable exc = GridTestUtils.assertThrows(ignite.log(), () -> execute(explainSql), IgniteSQLException.class,
            "Explains of update queries are not supported.");

        IgniteSQLException sqlExc = ((IgniteSQLException)exc);

        assertEquals("Operation error code is not correct.",
            IgniteQueryErrorCode.UNSUPPORTED_OPERATION, sqlExc.statusCode());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        try {
            stopAllGrids();
        }
        finally {
            super.afterTestsStopped();
        }
    }
}
