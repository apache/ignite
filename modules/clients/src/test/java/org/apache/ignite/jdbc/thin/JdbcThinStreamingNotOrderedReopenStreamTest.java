package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test JDBC streaming with ALLOW_OVERWRITE option
 */
public class JdbcThinStreamingNotOrderedReopenStreamTest extends GridCommonAbstractTest {
    /** JDBC URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** Batch size. */
    private static final long BATCH_SIZE = 4096;

    /** Rows count. */
    private static final long ROWS = BATCH_SIZE * 2 + 1;

    /** Iterations count. */
    private static final long ITERATIONS = 100;

    /** JDBC Connection. */
    private Connection conn;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(3);

        conn = DriverManager.getConnection(URL, new Properties());

        conn.prepareStatement("CREATE TABLE test(id LONG PRIMARY KEY, val0 VARCHAR, val1 VARCHAR) " +
            "WITH \"template=replicated\"").execute();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.close(conn, log);

        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception On fails.
     */
    @Test
    public void test() throws Exception {
        try {
            for (int iter = 0; iter < ITERATIONS; ++iter) {
                conn.prepareStatement("SET STREAMING ON BATCH_SIZE " + BATCH_SIZE).execute();

                String sql = "INSERT INTO test (id, val0, val1) VALUES (?, ?, ?)";

                PreparedStatement ps = conn.prepareStatement(sql);

                for (int i = 0; i < ROWS; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, String.valueOf(Math.random()));
                    ps.setString(3, String.valueOf(Math.random()));

                    ps.execute();
                }

                conn.prepareStatement("SET STREAMING OFF").execute();
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
