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

import java.io.PrintWriter;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Test JDBC POJO DataSource.
 */
public class TestJdbcPojoDataSource implements DataSource {
    /** */
    private final ThreadLocal<ConnectionHolder> holder = new ThreadLocal<ConnectionHolder>() {
        @Override protected ConnectionHolder initialValue() {
            return new ConnectionHolder();
        }
    };

    /** */
    private volatile boolean perThreadMode = true;

    /** */
    private String url;

    /** */
    private String username;

    /** */
    private String password;

    /** */
    private long idleCheckTimeout = 30_000;

    /** */
    public void setDriverClassName(String driverClassName) {
        try {
            Class.forName(driverClassName);
        }
        catch (ClassNotFoundException e) {
            throw new IllegalStateException("JDBC driver class not found: " + driverClassName, e);
        }
    }

    /** */
    public void setUrl(String url) {
        this.url = url;
    }

    /** */
    public void setUsername(String username) {
        this.username = username;
    }

    /** */
    public void setPassword(String password) {
        this.password = password;
    }

    /** */
    public void setIdleCheckTimeout(long idleCheckTimeout) {
        this.idleCheckTimeout = idleCheckTimeout;
    }

    /** */
    void switchPerThreadMode(boolean perThreadMode) {
        this.perThreadMode = perThreadMode;
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection() throws SQLException {
        if (perThreadMode) {
            ConnectionHolder holder = this.holder.get();

            holder.initIfNeeded();

            return holder;
        }
        else
            return createConnection();
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection(String username, String password) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override public void setLoginTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override public int getLoginTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override public PrintWriter getLogWriter() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override public void setLogWriter(PrintWriter out) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** {@inheritDoc} */
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /** */
    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    /** */
    private class ConnectionHolder implements Connection {
        /** */
        private Connection conn;

        /** */
        private long lastAccessTs = System.currentTimeMillis();

        /** */
        void initIfNeeded() throws SQLException {
            if (conn == null || conn.isClosed())
                conn = createConnection();
            else if (System.currentTimeMillis() - lastAccessTs > idleCheckTimeout && !conn.isValid(5_000)) {
                conn.close();

                conn = createConnection();
            }

            lastAccessTs = System.currentTimeMillis();
        }

        /** {@inheritDoc} */
        @Override public void close() throws SQLException {
            if (!perThreadMode)
                conn.close();
        }

        // --- Delegation. ---

        /** {@inheritDoc} */
        @Override public boolean isClosed() throws SQLException {
            return conn.isClosed();
        }

        /** {@inheritDoc} */
        @Override public Statement createStatement() throws SQLException {
            return conn.createStatement();
        }

        /** {@inheritDoc} */
        @Override public PreparedStatement prepareStatement(String sql) throws SQLException {
            return conn.prepareStatement(sql);
        }

        /** {@inheritDoc} */
        @Override public CallableStatement prepareCall(String sql) throws SQLException {
            return conn.prepareCall(sql);
        }

        /** {@inheritDoc} */
        @Override public String nativeSQL(String sql) throws SQLException {
            return conn.nativeSQL(sql);
        }

        /** {@inheritDoc} */
        @Override public void setAutoCommit(boolean autoCommit) throws SQLException {
            conn.setAutoCommit(autoCommit);
        }

        /** {@inheritDoc} */
        @Override public boolean getAutoCommit() throws SQLException {
            return conn.getAutoCommit();
        }

        /** {@inheritDoc} */
        @Override public void commit() throws SQLException {
            conn.commit();
        }

        /** {@inheritDoc} */
        @Override public void rollback() throws SQLException {
            conn.rollback();
        }

        /** {@inheritDoc} */
        @Override public DatabaseMetaData getMetaData() throws SQLException {
            return conn.getMetaData();
        }

        /** {@inheritDoc} */
        @Override public void setReadOnly(boolean readOnly) throws SQLException {
            conn.setReadOnly(readOnly);
        }

        /** {@inheritDoc} */
        @Override public boolean isReadOnly() throws SQLException {
            return conn.isReadOnly();
        }

        /** {@inheritDoc} */
        @Override public void setCatalog(String catalog) throws SQLException {
            conn.setCatalog(catalog);
        }

        /** {@inheritDoc} */
        @Override public String getCatalog() throws SQLException {
            return conn.getCatalog();
        }

        /** {@inheritDoc} */
        @Override public void setTransactionIsolation(int level) throws SQLException {
            conn.setTransactionIsolation(level);
        }

        /** {@inheritDoc} */
        @Override public int getTransactionIsolation() throws SQLException {
            return conn.getTransactionIsolation();
        }

        /** {@inheritDoc} */
        @Override public SQLWarning getWarnings() throws SQLException {
            return conn.getWarnings();
        }

        /** {@inheritDoc} */
        @Override public void clearWarnings() throws SQLException {
            conn.clearWarnings();
        }

        /** {@inheritDoc} */
        @Override public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return conn.createStatement(resultSetType, resultSetConcurrency);
        }

        /** {@inheritDoc} */
        @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
            return conn.prepareStatement(sql, resultSetType, resultSetConcurrency);
        }

        /** {@inheritDoc} */
        @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
            return conn.prepareCall(sql, resultSetType, resultSetConcurrency);
        }

        /** {@inheritDoc} */
        @Override public Map<String, Class<?>> getTypeMap() throws SQLException {
            return conn.getTypeMap();
        }

        /** {@inheritDoc} */
        @Override public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            conn.setTypeMap(map);
        }

        /** {@inheritDoc} */
        @Override public void setHoldability(int holdability) throws SQLException {
            conn.setHoldability(holdability);
        }

        /** {@inheritDoc} */
        @Override public int getHoldability() throws SQLException {
            return conn.getHoldability();
        }

        /** {@inheritDoc} */
        @Override public Savepoint setSavepoint() throws SQLException {
            return conn.setSavepoint();
        }

        /** {@inheritDoc} */
        @Override public Savepoint setSavepoint(String name) throws SQLException {
            return conn.setSavepoint(name);
        }

        /** {@inheritDoc} */
        @Override public void rollback(Savepoint savepoint) throws SQLException {
            conn.rollback(savepoint);
        }

        /** {@inheritDoc} */
        @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            conn.releaseSavepoint(savepoint);
        }

        /** {@inheritDoc} */
        @Override public Statement createStatement(int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
            return conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        /** {@inheritDoc} */
        @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
            return conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        /** {@inheritDoc} */
        @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
            return conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        /** {@inheritDoc} */
        @Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return conn.prepareStatement(sql, autoGeneratedKeys);
        }

        /** {@inheritDoc} */
        @Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return conn.prepareStatement(sql, columnIndexes);
        }

        /** {@inheritDoc} */
        @Override public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return conn.prepareStatement(sql, columnNames);
        }

        /** {@inheritDoc} */
        @Override public Clob createClob() throws SQLException {
            return conn.createClob();
        }

        /** {@inheritDoc} */
        @Override public Blob createBlob() throws SQLException {
            return conn.createBlob();
        }

        /** {@inheritDoc} */
        @Override public NClob createNClob() throws SQLException {
            return conn.createNClob();
        }

        /** {@inheritDoc} */
        @Override public SQLXML createSQLXML() throws SQLException {
            return conn.createSQLXML();
        }

        /** {@inheritDoc} */
        @Override public boolean isValid(int timeout) throws SQLException {
            return conn.isValid(timeout);
        }

        /** {@inheritDoc} */
        @Override public void setClientInfo(String name, String value) throws SQLClientInfoException {
            conn.setClientInfo(name, value);
        }

        /** {@inheritDoc} */
        @Override public void setClientInfo(Properties properties) throws SQLClientInfoException {
            conn.setClientInfo(properties);
        }

        /** {@inheritDoc} */
        @Override public String getClientInfo(String name) throws SQLException {
            return conn.getClientInfo(name);
        }

        /** {@inheritDoc} */
        @Override public Properties getClientInfo() throws SQLException {
            return conn.getClientInfo();
        }

        /** {@inheritDoc} */
        @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return conn.createArrayOf(typeName, elements);
        }

        /** {@inheritDoc} */
        @Override public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return conn.createStruct(typeName, attributes);
        }

        /** {@inheritDoc} */
        @Override public void setSchema(String schema) throws SQLException {
            conn.setSchema(schema);
        }

        /** {@inheritDoc} */
        @Override public String getSchema() throws SQLException {
            return conn.getSchema();
        }

        /** {@inheritDoc} */
        @Override public void abort(Executor executor) throws SQLException {
            conn.abort(executor);
        }

        /** {@inheritDoc} */
        @Override public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            conn.setNetworkTimeout(executor, milliseconds);
        }

        /** {@inheritDoc} */
        @Override public int getNetworkTimeout() throws SQLException {
            return conn.getNetworkTimeout();
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            return conn.unwrap(iface);
        }

        /** {@inheritDoc} */
        @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return conn.isWrapperFor(iface);
        }
    }
}


