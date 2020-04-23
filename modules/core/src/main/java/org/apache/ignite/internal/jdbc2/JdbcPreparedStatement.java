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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.jdbc.thin.JdbcThinParameterMetadata;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcParameterMeta;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC prepared statement implementation.
 */
public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {
    /** SQL query. */
    private final String sql;

    /** Batch arguments. */
    private List<List<Object>> batchArgs;

    /** Parameters metadata (initialized lazily). */
    private ParameterMetaData paramsMeta;

    /** Result set metadata (initialized lazily). */
    private ResultSetMetaData resMeta;

    /** Whether metadata of returning result set have been fetched. */
    private boolean resMetaFetched;

    /**
     * Creates new prepared statement.
     *
     * @param conn Connection.
     * @param sql SQL query.
     */
    JdbcPreparedStatement(JdbcConnection conn, String sql) {
        super(conn);

        this.sql = sql;
    }

    /** {@inheritDoc} */
    @Override public void addBatch(String sql) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Adding new SQL command to batch is not supported for prepared " +
            "statement (use addBatch() to add new set of arguments)");
    }

    /** {@inheritDoc} */
    @Override public ResultSet executeQuery() throws SQLException {
        ensureNotClosed();

        return executeQuery(sql);
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate() throws SQLException {
        ensureNotClosed();

        return executeUpdate(sql);
    }

    /** {@inheritDoc} */
    @Override public void setNull(int paramIdx, int sqlType) throws SQLException {
        setArgument(paramIdx, null);
    }

    /** {@inheritDoc} */
    @Override public void setBoolean(int paramIdx, boolean x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setByte(int paramIdx, byte x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setShort(int paramIdx, short x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setInt(int paramIdx, int x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setLong(int paramIdx, long x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setFloat(int paramIdx, float x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setDouble(int paramIdx, double x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setBigDecimal(int paramIdx, BigDecimal x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setString(int paramIdx, String x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setBytes(int paramIdx, byte[] x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setDate(int paramIdx, Date x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setTime(int paramIdx, Time x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setTimestamp(int paramIdx, Timestamp x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setAsciiStream(int paramIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setUnicodeStream(int paramIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setBinaryStream(int paramIdx, InputStream x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void clearParameters() throws SQLException {
        ensureNotClosed();

        args = null;
    }

    /** {@inheritDoc} */
    @Override public void clearBatch() throws SQLException {
        ensureNotClosed();

        batchArgs = null;
    }

    /** {@inheritDoc} */
    @Override public void setObject(int paramIdx, Object x, int targetSqlType) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setObject(int paramIdx, Object x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public boolean execute() throws SQLException {
        return execute(sql);
    }

    /** {@inheritDoc} */
    @Override public void addBatch() throws SQLException {
        ensureNotClosed();

        if (batchArgs == null)
            batchArgs = new ArrayList<>();

        batchArgs.add(args);

        args = null;
    }

    /** {@inheritDoc} */
    @Override public int[] executeBatch() throws SQLException {
        ensureNotClosed();

        List<List<Object>> batchArgs = this.batchArgs;

        this.batchArgs = null;

        return doBatchUpdate(sql, null, batchArgs);
    }


    /** {@inheritDoc} */
    @Override public void setCharacterStream(int paramIdx, Reader x, int len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setRef(int paramIdx, Ref x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setBlob(int paramIdx, Blob x) throws SQLException {
        setBytes(paramIdx, x.getBytes(1, (int)x.length()));
    }

    /** {@inheritDoc} */
    @Override public void setClob(int paramIdx, Clob x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setArray(int paramIdx, Array x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public ResultSetMetaData getMetaData() throws SQLException {
        ensureNotClosed();

        if (!resMetaFetched) {
            assert resMeta == null;

            resMeta = getMetadata0();

            resMetaFetched = true;
        }

        return resMeta;
    }

    /**
     * Fetches metadata of the result set is returned when specified query gets executed.
     *
     * @return metadata describing result set or {@code null} if query is not a SELECT operation.
     */
    @Nullable private ResultSetMetaData getMetadata0() throws SQLException {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        setupQuery(qry);

        try {
            List<GridQueryFieldMetadata> meta = conn.ignite().context().query().getIndexing().resultMetaData(conn.schemaName(), qry);

            if (meta == null)
                return null;

            return new JdbcResultSetMetadata(meta);
        }
        catch (IgniteSQLException ex) {
            throw ex.toJdbcException();
        }
    }

    /** {@inheritDoc} */
    @Override public void setDate(int paramIdx, Date x, Calendar cal) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setTime(int paramIdx, Time x, Calendar cal) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setTimestamp(int paramIdx, Timestamp x, Calendar cal) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setNull(int paramIdx, int sqlType, String typeName) throws SQLException {
        setNull(paramIdx, sqlType);
    }

    /** {@inheritDoc} */
    @Override public void setURL(int paramIdx, URL x) throws SQLException {
        setArgument(paramIdx, x);
    }

    /**
     * {@inheritDoc}
     *
     * Implementation note: If this prepared statement is a multi-statement, returned meta contains meta of parameters
     * from the all statements.
     */
    @Override public ParameterMetaData getParameterMetaData() throws SQLException {
        ensureNotClosed();

        if (paramsMeta == null)
            paramsMeta = parameterMetaData();

        return paramsMeta;
    }

    /**
     * Fetches parameters metadata of the specified query.
     */
    private ParameterMetaData parameterMetaData() throws SQLException {
        SqlFieldsQueryEx qry = new SqlFieldsQueryEx(sql, null);

        setupQuery(qry);

        try {
            List<JdbcParameterMeta> params = conn.ignite().context().query().getIndexing().parameterMetaData(conn.schemaName(), qry);

            return new JdbcThinParameterMetadata(params);
        }
        catch (IgniteSQLException ex) {
            throw ex.toJdbcException();
        }
    }

    /** {@inheritDoc} */
    @Override public void setRowId(int paramIdx, RowId x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setNString(int paramIdx, String val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setNCharacterStream(int paramIdx, Reader val, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setNClob(int paramIdx, NClob val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setClob(int paramIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setBlob(int paramIdx, InputStream inputStream, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setNClob(int paramIdx, Reader reader, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setSQLXML(int paramIdx, SQLXML xmlObj) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setObject(int paramIdx, Object x, int targetSqlType,
        int scaleOrLen) throws SQLException {
        setArgument(paramIdx, x);
    }

    /** {@inheritDoc} */
    @Override public void setAsciiStream(int paramIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setBinaryStream(int paramIdx, InputStream x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setCharacterStream(int paramIdx, Reader x, long len) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setAsciiStream(int paramIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setBinaryStream(int paramIdx, InputStream x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setCharacterStream(int paramIdx, Reader x) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("Streams are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setNCharacterStream(int paramIdx, Reader val) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setClob(int paramIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setBlob(int paramIdx, InputStream inputStream) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /** {@inheritDoc} */
    @Override public void setNClob(int paramIdx, Reader reader) throws SQLException {
        ensureNotClosed();

        throw new SQLFeatureNotSupportedException("SQL-specific types are not supported.");
    }

    /**
     * Sets query argument value.
     *
     * @param paramIdx Index.
     * @param val Value.
     * @throws SQLException If index is invalid.
     */
    private void setArgument(int paramIdx, Object val) throws SQLException {
        ensureNotClosed();

        if (paramIdx < 1)
            throw new SQLException("Parameter index is invalid: " + paramIdx);

        ensureArgsSize(paramIdx);

        args.set(paramIdx - 1, val);
    }

    /**
     * Initialize {@link #args} and increase its capacity and size up to given argument if needed.
     * @param size new expected size.
     */
    private void ensureArgsSize(int size) {
        if (args == null)
            args = new ArrayList<>(size);

        args.ensureCapacity(size);

        while (args.size() < size)
            args.add(null);
    }
}
