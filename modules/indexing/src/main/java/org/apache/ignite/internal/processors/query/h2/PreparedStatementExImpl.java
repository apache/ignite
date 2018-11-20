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

package org.apache.ignite.internal.processors.query.h2;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * PreparedStatement with extended capability to store additional meta information.
 */
@SuppressWarnings("unchecked")
final class PreparedStatementExImpl implements PreparedStatementEx {
    /** */
    private final PreparedStatement delegate;

    /** */
    private Object[] meta = null;

    /**
     * @param delegate Wrapped statement.
     */
    public PreparedStatementExImpl(PreparedStatement delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public ResultSet executeQuery() throws SQLException {
        return delegate.executeQuery();
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate() throws SQLException {
        return delegate.executeUpdate();
    }

    /** {@inheritDoc} */
    @Override public void setNull(int parameterIndex, int sqlType) throws SQLException {
        delegate.setNull(parameterIndex, sqlType);
    }

    /** {@inheritDoc} */
    @Override public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        delegate.setBoolean(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setByte(int parameterIndex, byte x) throws SQLException {
        delegate.setByte(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setShort(int parameterIndex, short x) throws SQLException {
        delegate.setShort(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setInt(int parameterIndex, int x) throws SQLException {
        delegate.setInt(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setLong(int parameterIndex, long x) throws SQLException {
        delegate.setLong(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setFloat(int parameterIndex, float x) throws SQLException {
        delegate.setFloat(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setDouble(int parameterIndex, double x) throws SQLException {
        delegate.setDouble(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        delegate.setBigDecimal(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setString(int parameterIndex, String x) throws SQLException {
        delegate.setString(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        delegate.setBytes(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setDate(int parameterIndex, Date x) throws SQLException {
        delegate.setDate(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setTime(int parameterIndex, Time x) throws SQLException {
        delegate.setTime(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        delegate.setTimestamp(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        delegate.setAsciiStream(parameterIndex, x, length);
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        delegate.setUnicodeStream(parameterIndex, x, length);
    }

    /** {@inheritDoc} */
    @Override public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        delegate.setBinaryStream(parameterIndex, x, length);
    }

    /** {@inheritDoc} */
    @Override public void clearParameters() throws SQLException {
        delegate.clearParameters();
    }

    /** {@inheritDoc} */
    @Override public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        delegate.setObject(parameterIndex, x, targetSqlType);
    }

    /** {@inheritDoc} */
    @Override public void setObject(int parameterIndex, Object x) throws SQLException {
        delegate.setObject(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public boolean execute() throws SQLException {
        return delegate.execute();
    }

    /** {@inheritDoc} */
    @Override public void addBatch() throws SQLException {
        delegate.addBatch();
    }

    /** {@inheritDoc} */
    @Override public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        delegate.setCharacterStream(parameterIndex, reader, length);
    }

    /** {@inheritDoc} */
    @Override public void setRef(int parameterIndex, Ref x) throws SQLException {
        delegate.setRef(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setBlob(int parameterIndex, Blob x) throws SQLException {
        delegate.setBlob(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setClob(int parameterIndex, Clob x) throws SQLException {
        delegate.setClob(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setArray(int parameterIndex, Array x) throws SQLException {
        delegate.setArray(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public ResultSetMetaData getMetaData() throws SQLException {
        return delegate.getMetaData();
    }

    /** {@inheritDoc} */
    @Override public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        delegate.setDate(parameterIndex, x, cal);
    }

    /** {@inheritDoc} */
    @Override public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        delegate.setTime(parameterIndex, x, cal);
    }

    /** {@inheritDoc} */
    @Override public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        delegate.setTimestamp(parameterIndex, x, cal);
    }

    /** {@inheritDoc} */
    @Override public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        delegate.setNull(parameterIndex, sqlType, typeName);
    }

    /** {@inheritDoc} */
    @Override public void setURL(int parameterIndex, URL x) throws SQLException {
        delegate.setURL(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public ParameterMetaData getParameterMetaData() throws SQLException {
        return delegate.getParameterMetaData();
    }

    /** {@inheritDoc} */
    @Override public void setRowId(int parameterIndex, RowId x) throws SQLException {
        delegate.setRowId(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setNString(int parameterIndex, String value) throws SQLException {
        delegate.setNString(parameterIndex, value);
    }

    /** {@inheritDoc} */
    @Override public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        delegate.setNCharacterStream(parameterIndex, value, length);
    }

    /** {@inheritDoc} */
    @Override public void setNClob(int parameterIndex, NClob value) throws SQLException {
        delegate.setNClob(parameterIndex, value);
    }

    /** {@inheritDoc} */
    @Override public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        delegate.setClob(parameterIndex, reader, length);
    }

    /** {@inheritDoc} */
    @Override public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        delegate.setBlob(parameterIndex, inputStream, length);
    }

    /** {@inheritDoc} */
    @Override public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        delegate.setNClob(parameterIndex, reader, length);
    }

    /** {@inheritDoc} */
    @Override public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        delegate.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    /** {@inheritDoc} */
    @Override public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        delegate.setAsciiStream(parameterIndex, x, length);
    }

    /** {@inheritDoc} */
    @Override public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        delegate.setBinaryStream(parameterIndex, x, length);
    }

    /** {@inheritDoc} */
    @Override public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        delegate.setCharacterStream(parameterIndex, reader, length);
    }

    /** {@inheritDoc} */
    @Override public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        delegate.setAsciiStream(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        delegate.setBinaryStream(parameterIndex, x);
    }

    /** {@inheritDoc} */
    @Override public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        delegate.setCharacterStream(parameterIndex, reader);
    }

    /** {@inheritDoc} */
    @Override public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        delegate.setNCharacterStream(parameterIndex, value);
    }

    /** {@inheritDoc} */
    @Override public void setClob(int parameterIndex, Reader reader) throws SQLException {
        delegate.setClob(parameterIndex, reader);
    }

    /** {@inheritDoc} */
    @Override public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        delegate.setBlob(parameterIndex, inputStream);
    }

    /** {@inheritDoc} */
    @Override public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        delegate.setNClob(parameterIndex, reader);
    }

    /** {@inheritDoc} */
    @Override public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        delegate.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
    }

    /** {@inheritDoc} */
    @Override public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        delegate.setObject(parameterIndex, x, targetSqlType);
    }

    /** {@inheritDoc} */
    @Override public long executeLargeUpdate() throws SQLException {
        return delegate.executeLargeUpdate();
    }

    /** {@inheritDoc} */
    @Override public ResultSet executeQuery(String sql) throws SQLException {
        return delegate.executeQuery(sql);
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql) throws SQLException {
        return delegate.executeUpdate(sql);
    }

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        delegate.close();
    }

    /** {@inheritDoc} */
    @Override public int getMaxFieldSize() throws SQLException {
        return delegate.getMaxFieldSize();
    }

    /** {@inheritDoc} */
    @Override public void setMaxFieldSize(int max) throws SQLException {
        delegate.setMaxFieldSize(max);
    }

    /** {@inheritDoc} */
    @Override public int getMaxRows() throws SQLException {
        return delegate.getMaxRows();
    }

    /** {@inheritDoc} */
    @Override public void setMaxRows(int max) throws SQLException {
        delegate.setMaxRows(max);
    }

    /** {@inheritDoc} */
    @Override public void setEscapeProcessing(boolean enable) throws SQLException {
        delegate.setEscapeProcessing(enable);
    }

    /** {@inheritDoc} */
    @Override public int getQueryTimeout() throws SQLException {
        return delegate.getQueryTimeout();
    }

    /** {@inheritDoc} */
    @Override public void setQueryTimeout(int seconds) throws SQLException {
        delegate.setQueryTimeout(seconds);
    }

    /** {@inheritDoc} */
    @Override public void cancel() throws SQLException {
        delegate.cancel();
    }

    /** {@inheritDoc} */
    @Override public SQLWarning getWarnings() throws SQLException {
        return delegate.getWarnings();
    }

    /** {@inheritDoc} */
    @Override public void clearWarnings() throws SQLException {
        delegate.clearWarnings();
    }

    /** {@inheritDoc} */
    @Override public void setCursorName(String name) throws SQLException {
        delegate.setCursorName(name);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql) throws SQLException {
        return delegate.execute(sql);
    }

    /** {@inheritDoc} */
    @Override public ResultSet getResultSet() throws SQLException {
        return delegate.getResultSet();
    }

    /** {@inheritDoc} */
    @Override public int getUpdateCount() throws SQLException {
        return delegate.getUpdateCount();
    }

    /** {@inheritDoc} */
    @Override public boolean getMoreResults() throws SQLException {
        return delegate.getMoreResults();
    }

    /** {@inheritDoc} */
    @Override public int getFetchDirection() throws SQLException {
        return delegate.getFetchDirection();
    }

    /** {@inheritDoc} */
    @Override public void setFetchDirection(int direction) throws SQLException {
        delegate.setFetchDirection(direction);
    }

    /** {@inheritDoc} */
    @Override public int getFetchSize() throws SQLException {
        return delegate.getFetchSize();
    }

    /** {@inheritDoc} */
    @Override public void setFetchSize(int rows) throws SQLException {
        delegate.setFetchSize(rows);
    }

    /** {@inheritDoc} */
    @Override public int getResultSetConcurrency() throws SQLException {
        return delegate.getResultSetConcurrency();
    }

    /** {@inheritDoc} */
    @Override public int getResultSetType() throws SQLException {
        return delegate.getResultSetType();
    }

    /** {@inheritDoc} */
    @Override public void addBatch(String sql) throws SQLException {
        delegate.addBatch(sql);
    }

    /** {@inheritDoc} */
    @Override public void clearBatch() throws SQLException {
        delegate.clearBatch();
    }

    /** {@inheritDoc} */
    @Override public int[] executeBatch() throws SQLException {
        return delegate.executeBatch();
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection() throws SQLException {
        return delegate.getConnection();
    }

    /** {@inheritDoc} */
    @Override public boolean getMoreResults(int current) throws SQLException {
        return delegate.getMoreResults(current);
    }

    /** {@inheritDoc} */
    @Override public ResultSet getGeneratedKeys() throws SQLException {
        return delegate.getGeneratedKeys();
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.executeUpdate(sql, autoGeneratedKeys);
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return delegate.executeUpdate(sql, columnIndexes);
    }

    /** {@inheritDoc} */
    @Override public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return delegate.executeUpdate(sql, columnNames);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.execute(sql, autoGeneratedKeys);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return delegate.execute(sql, columnIndexes);
    }

    /** {@inheritDoc} */
    @Override public boolean execute(String sql, String[] columnNames) throws SQLException {
        return delegate.execute(sql, columnNames);
    }

    /** {@inheritDoc} */
    @Override public int getResultSetHoldability() throws SQLException {
        return delegate.getResultSetHoldability();
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    /** {@inheritDoc} */
    @Override public boolean isPoolable() throws SQLException {
        return delegate.isPoolable();
    }

    /** {@inheritDoc} */
    @Override public void setPoolable(boolean poolable) throws SQLException {
        delegate.setPoolable(poolable);
    }

    /** {@inheritDoc} */
    @Override public void closeOnCompletion() throws SQLException {
        delegate.closeOnCompletion();
    }

    /** {@inheritDoc} */
    @Override public boolean isCloseOnCompletion() throws SQLException {
        return delegate.isCloseOnCompletion();
    }

    /** {@inheritDoc} */
    @Override public long getLargeUpdateCount() throws SQLException {
        return delegate.getLargeUpdateCount();
    }

    /** {@inheritDoc} */
    @Override public long getLargeMaxRows() throws SQLException {
        return delegate.getLargeMaxRows();
    }

    /** {@inheritDoc} */
    @Override public void setLargeMaxRows(long max) throws SQLException {
        delegate.setLargeMaxRows(max);
    }

    /** {@inheritDoc} */
    @Override public long[] executeLargeBatch() throws SQLException {
        return delegate.executeLargeBatch();
    }

    /** {@inheritDoc} */
    @Override public long executeLargeUpdate(String sql) throws SQLException {
        return delegate.executeLargeUpdate(sql);
    }

    /** {@inheritDoc} */
    @Override public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.executeLargeUpdate(sql, autoGeneratedKeys);
    }

    /** {@inheritDoc} */
    @Override public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return delegate.executeLargeUpdate(sql, columnIndexes);
    }

    /** {@inheritDoc} */
    @Override public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        return delegate.executeLargeUpdate(sql, columnNames);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface == PreparedStatementExImpl.class || iface == PreparedStatementEx.class)
            return (T)this;

        return delegate.unwrap(iface);
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface == PreparedStatementExImpl.class
            || iface == PreparedStatementEx.class
            || delegate.isWrapperFor(iface);
    }

    /** {@inheritDoc} */
    @Override public @Nullable <T> T meta(int id) {
        return meta != null && id < meta.length ? (T)meta[id] : null;
    }

    /** {@inheritDoc} */
    @Override public void putMeta(int id, Object metaObj) {
        if (meta == null)
            meta = new Object[id + 1];
        else if (meta.length <= id)
            meta = Arrays.copyOf(meta, id + 1);

        meta[id] = metaObj;
    }

    /**
     *
     * @param stmt Prepared statement to wrap.
     * @return Wrapped statement.
     */
    public static PreparedStatement wrap(@NotNull PreparedStatement stmt) {
        if (stmt.getClass() == PreparedStatementExImpl.class)
            return stmt;

        return new PreparedStatementExImpl(stmt);
    }
}
