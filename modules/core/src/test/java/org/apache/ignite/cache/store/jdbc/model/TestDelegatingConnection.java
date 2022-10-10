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

package org.apache.ignite.cache.store.jdbc.model;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Delegating connection.
 */
public abstract class TestDelegatingConnection implements Connection {
    /** */
    protected Connection delegate;

    /** {@inheritDoc} */
    @Override public void close() throws SQLException {
        delegate.close();
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() throws SQLException {
        return delegate.isClosed();
    }

    /** {@inheritDoc} */
    @Override public Statement createStatement() throws SQLException {
        return delegate.createStatement();
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql) throws SQLException {
        return delegate.prepareStatement(sql);
    }

    /** {@inheritDoc} */
    @Override public CallableStatement prepareCall(String sql) throws SQLException {
        return delegate.prepareCall(sql);
    }

    /** {@inheritDoc} */
    @Override public String nativeSQL(String sql) throws SQLException {
        return delegate.nativeSQL(sql);
    }

    /** {@inheritDoc} */
    @Override public void setAutoCommit(boolean autoCommit) throws SQLException {
        delegate.setAutoCommit(autoCommit);
    }

    /** {@inheritDoc} */
    @Override public boolean getAutoCommit() throws SQLException {
        return delegate.getAutoCommit();
    }

    /** {@inheritDoc} */
    @Override public void commit() throws SQLException {
        delegate.commit();
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws SQLException {
        delegate.rollback();
    }

    /** {@inheritDoc} */
    @Override public DatabaseMetaData getMetaData() throws SQLException {
        return delegate.getMetaData();
    }

    /** {@inheritDoc} */
    @Override public void setReadOnly(boolean readOnly) throws SQLException {
        delegate.setReadOnly(readOnly);
    }

    /** {@inheritDoc} */
    @Override public boolean isReadOnly() throws SQLException {
        return delegate.isReadOnly();
    }

    /** {@inheritDoc} */
    @Override public void setCatalog(String catalog) throws SQLException {
        delegate.setCatalog(catalog);
    }

    /** {@inheritDoc} */
    @Override public String getCatalog() throws SQLException {
        return delegate.getCatalog();
    }

    /** {@inheritDoc} */
    @Override public void setTransactionIsolation(int level) throws SQLException {
        delegate.setTransactionIsolation(level);
    }

    /** {@inheritDoc} */
    @Override public int getTransactionIsolation() throws SQLException {
        return delegate.getTransactionIsolation();
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
    @Override public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return delegate.createStatement(resultSetType, resultSetConcurrency);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    /** {@inheritDoc} */
    @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Class<?>> getTypeMap() throws SQLException {
        return delegate.getTypeMap();
    }

    /** {@inheritDoc} */
    @Override public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        delegate.setTypeMap(map);
    }

    /** {@inheritDoc} */
    @Override public void setHoldability(int holdability) throws SQLException {
        delegate.setHoldability(holdability);
    }

    /** {@inheritDoc} */
    @Override public int getHoldability() throws SQLException {
        return delegate.getHoldability();
    }

    /** {@inheritDoc} */
    @Override public Savepoint setSavepoint() throws SQLException {
        return delegate.setSavepoint();
    }

    /** {@inheritDoc} */
    @Override public Savepoint setSavepoint(String name) throws SQLException {
        return delegate.setSavepoint(name);
    }

    /** {@inheritDoc} */
    @Override public void rollback(Savepoint savepoint) throws SQLException {
        delegate.rollback(savepoint);
    }

    /** {@inheritDoc} */
    @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        delegate.releaseSavepoint(savepoint);
    }

    /** {@inheritDoc} */
    @Override public Statement createStatement(int resultSetType, int resultSetConcurrency,
    int resultSetHoldability) throws SQLException {
        return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
    int resultSetHoldability) throws SQLException {
        return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    /** {@inheritDoc} */
    @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
    int resultSetHoldability) throws SQLException {
        return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return delegate.prepareStatement(sql, autoGeneratedKeys);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return delegate.prepareStatement(sql, columnIndexes);
    }

    /** {@inheritDoc} */
    @Override public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return delegate.prepareStatement(sql, columnNames);
    }

    /** {@inheritDoc} */
    @Override public Clob createClob() throws SQLException {
        return delegate.createClob();
    }

    /** {@inheritDoc} */
    @Override public Blob createBlob() throws SQLException {
        return delegate.createBlob();
    }

    /** {@inheritDoc} */
    @Override public NClob createNClob() throws SQLException {
        return delegate.createNClob();
    }

    /** {@inheritDoc} */
    @Override public SQLXML createSQLXML() throws SQLException {
        return delegate.createSQLXML();
    }

    /** {@inheritDoc} */
    @Override public boolean isValid(int timeout) throws SQLException {
        return delegate.isValid(timeout);
    }

    /** {@inheritDoc} */
    @Override public void setClientInfo(String name, String value) throws SQLClientInfoException {
        delegate.setClientInfo(name, value);
    }

    /** {@inheritDoc} */
    @Override public void setClientInfo(Properties properties) throws SQLClientInfoException {
        delegate.setClientInfo(properties);
    }

    /** {@inheritDoc} */
    @Override public String getClientInfo(String name) throws SQLException {
        return delegate.getClientInfo(name);
    }

    /** {@inheritDoc} */
    @Override public Properties getClientInfo() throws SQLException {
        return delegate.getClientInfo();
    }

    /** {@inheritDoc} */
    @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return delegate.createArrayOf(typeName, elements);
    }

    /** {@inheritDoc} */
    @Override public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return delegate.createStruct(typeName, attributes);
    }

    /** {@inheritDoc} */
    @Override public void setSchema(String schema) throws SQLException {
        delegate.setSchema(schema);
    }

    /** {@inheritDoc} */
    @Override public String getSchema() throws SQLException {
        return delegate.getSchema();
    }

    /** {@inheritDoc} */
    @Override public void abort(Executor executor) throws SQLException {
        delegate.abort(executor);
    }

    /** {@inheritDoc} */
    @Override public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        delegate.setNetworkTimeout(executor, milliseconds);
    }

    /** {@inheritDoc} */
    @Override public int getNetworkTimeout() throws SQLException {
        return delegate.getNetworkTimeout();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        return delegate.unwrap(iface);
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return delegate.isWrapperFor(iface);
    }
}
