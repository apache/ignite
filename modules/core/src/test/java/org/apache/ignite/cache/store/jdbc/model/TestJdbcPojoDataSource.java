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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
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
    private class ConnectionHolder extends TestDelegatingConnection {
        /** */
        private long lastAccessTs = System.currentTimeMillis();

        /** */
        void initIfNeeded() throws SQLException {
            if (delegate == null || delegate.isClosed())
                delegate = createConnection();
            else if (System.currentTimeMillis() - lastAccessTs > idleCheckTimeout && !delegate.isValid(5_000)) {
                delegate.close();

                delegate = createConnection();
            }

            lastAccessTs = System.currentTimeMillis();
        }

        /** {@inheritDoc} */
        @Override public void close() throws SQLException {
            if (!perThreadMode)
                delegate.close();
        }
    }
}


