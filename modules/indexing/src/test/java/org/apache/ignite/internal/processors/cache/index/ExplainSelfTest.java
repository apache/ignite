package org.apache.ignite.internal.processors.cache.index;

import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
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
     * @param sql sql query.
     * @return fully fetched result of query.
     * @throws Exception on error.
     */
    private List<List<?>> execute(String sql) throws Exception {
        return ignite.context().query().querySqlFields(new SqlFieldsQuery(sql), false).getAll();
    }

    /**
     * Check that EXPLAIN DELETE doesn't cause errors.
     */
    public void testExplainDelete() throws Exception {
        execute("EXPLAIN DELETE FROM TEST;");
    }

    /**
     * Check that EXPLAIN DELETE doesn't cause errors.
     */
    public void testExplainUpdate() throws Exception {
        execute("EXPLAIN UPDATE TEST SET VAL = VAL + 1;");
    }

    /**
     * Check that EXPLAIN DELETE doesn't cause errors.
     */
    public void testExplainSelect() throws Exception {
        execute("EXPLAIN SELECT * FROM TEST;");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        try{
            stopAllGrids();
        } finally {
            super.afterTestsStopped();
        }
    }
}
