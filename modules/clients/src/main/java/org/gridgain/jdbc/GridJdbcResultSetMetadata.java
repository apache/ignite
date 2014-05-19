/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.jdbc;

import org.gridgain.jdbc.typedef.*;

import java.sql.*;
import java.util.*;

/**
 * JDBC result set metadata implementation.
 */
class GridJdbcResultSetMetadata implements ResultSetMetaData {
    /** Column width. */
    private static final int COL_WIDTH = 30;

    /** Table names. */
    private final List<String> tbls;

    /** Column names. */
    private final List<String> cols;

    /** Class names. */
    private final List<String> types;

    /**
     * @param tbls Table names.
     * @param cols Column names.
     * @param types Types.
     */
    GridJdbcResultSetMetadata(List<String> tbls, List<String> cols, List<String> types) {
        assert cols != null;
        assert types != null;

        this.tbls = tbls;
        this.cols = cols;
        this.types = types;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() throws SQLException {
        return cols.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isAutoIncrement(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isCaseSensitive(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isSearchable(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isCurrency(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int isNullable(int col) throws SQLException {
        return columnNullable;
    }

    /** {@inheritDoc} */
    @Override public boolean isSigned(int col) throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public int getColumnDisplaySize(int col) throws SQLException {
        return COL_WIDTH;
    }

    /** {@inheritDoc} */
    @Override public String getColumnLabel(int col) throws SQLException {
        return cols.get(col - 1);
    }

    /** {@inheritDoc} */
    @Override public String getColumnName(int col) throws SQLException {
        return cols.get(col - 1);
    }

    /** {@inheritDoc} */
    @Override public String getSchemaName(int col) throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public int getPrecision(int col) throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getScale(int col) throws SQLException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String getTableName(int col) throws SQLException {
        return tbls != null ? tbls.get(col - 1) : "";
    }

    /** {@inheritDoc} */
    @Override public String getCatalogName(int col) throws SQLException {
        return "";
    }

    /** {@inheritDoc} */
    @Override public int getColumnType(int col) throws SQLException {
        return JU.type(types.get(col - 1));
    }

    /** {@inheritDoc} */
    @Override public String getColumnTypeName(int col) throws SQLException {
        return JU.typeName(types.get(col - 1));
    }

    /** {@inheritDoc} */
    @Override public boolean isReadOnly(int col) throws SQLException {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isWritable(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDefinitelyWritable(int col) throws SQLException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String getColumnClassName(int col) throws SQLException {
        return types.get(col - 1);
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("Result set meta data is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface == ResultSetMetaData.class;
    }
}
