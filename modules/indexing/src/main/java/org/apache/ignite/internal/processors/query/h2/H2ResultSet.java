package org.apache.ignite.internal.processors.query.h2;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.h2.jdbc.JdbcResultSet;
import org.h2.result.ResultInterface;
import org.h2.value.Value;

/**
 * Result set for H2 query.
 */
public class H2ResultSet implements AutoCloseable {
    /** */
    private static final Field RESULT_FIELD;

    /**
     * Initialize.
     */
    static {
        try {
            RESULT_FIELD = JdbcResultSet.class.getDeclaredField("result");

            RESULT_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException e) {
            throw new IllegalStateException("Check H2 version in classpath.", e);
        }
    }

    /** */
    private final H2Connection conn;

    /** */
    private final ResultSet rs;

    /** */
    private final ResultInterface res;

    /** */
    private final int colCnt;

    /**
     * @param conn Pooled H2 connection.
     * @param rs Result set.
     */
    public H2ResultSet(H2Connection conn, ResultSet rs) {
        assert conn != null;

        this.conn = conn;
        this.rs = rs;

        try {
            res = (ResultInterface)RESULT_FIELD.get(rs);
        }
        catch (IllegalAccessException e) {
            throw new IllegalStateException(e); // Must not happen.
        }

        colCnt = res.getVisibleColumnCount();
    }

    /**
     * @return Columns count.
     */
    public int getColumnsCount() {
        return colCnt;
    }

    /**
     * @return {@code true} If next row was fetched.
     * @throws SQLException If failed.
     */
    public boolean next() throws SQLException {
        return rs.next();
    }

    /**
     * @return Current row.
     */
    public Value[] currentRow() {
        return res.currentRow();
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        rs.close();

//    TODO    conn.returnToPool();
    }
}
