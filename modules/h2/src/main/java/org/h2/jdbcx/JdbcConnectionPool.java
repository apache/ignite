/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Christian d'Heureuse, www.source-code.biz
 *
 * This class is multi-licensed under LGPL, MPL 2.0, and EPL 1.0.
 *
 * This module is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 * See http://www.gnu.org/licenses/lgpl.html
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public
 * License for more details.
 */
package org.h2.jdbcx;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import org.h2.message.DbException;
import org.h2.util.New;

/**
 * A simple standalone JDBC connection pool.
 * It is based on the
 * <a href="http://www.source-code.biz/snippets/java/8.htm">
 *  MiniConnectionPoolManager written by Christian d'Heureuse (Java 1.5)
 * </a>. It is used as follows:
 * <pre>
 * import java.sql.*;
 * import org.h2.jdbcx.JdbcConnectionPool;
 * public class Test {
 *     public static void main(String... args) throws Exception {
 *         JdbcConnectionPool cp = JdbcConnectionPool.create(
 *             "jdbc:h2:~/test", "sa", "sa");
 *         for (String sql : args) {
 *             Connection conn = cp.getConnection();
 *             conn.createStatement().execute(sql);
 *             conn.close();
 *         }
 *         cp.dispose();
 *     }
 * }
 * </pre>
 *
 * @author Christian d'Heureuse
 *      (<a href="http://www.source-code.biz">www.source-code.biz</a>)
 * @author Thomas Mueller
 */
public class JdbcConnectionPool implements DataSource, ConnectionEventListener,
        JdbcConnectionPoolBackwardsCompat {

    private static final int DEFAULT_TIMEOUT = 30;
    private static final int DEFAULT_MAX_CONNECTIONS = 10;

    private final ConnectionPoolDataSource dataSource;
    private final ArrayList<PooledConnection> recycledConnections = New.arrayList();
    private PrintWriter logWriter;
    private int maxConnections = DEFAULT_MAX_CONNECTIONS;
    private int timeout = DEFAULT_TIMEOUT;
    private int activeConnections;
    private boolean isDisposed;

    protected JdbcConnectionPool(ConnectionPoolDataSource dataSource) {
        this.dataSource = dataSource;
        if (dataSource != null) {
            try {
                logWriter = dataSource.getLogWriter();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Constructs a new connection pool.
     *
     * @param dataSource the data source to create connections
     * @return the connection pool
     */
    public static JdbcConnectionPool create(ConnectionPoolDataSource dataSource) {
        return new JdbcConnectionPool(dataSource);
    }

    /**
     * Constructs a new connection pool for H2 databases.
     *
     * @param url the database URL of the H2 connection
     * @param user the user name
     * @param password the password
     * @return the connection pool
     */
    public static JdbcConnectionPool create(String url, String user,
            String password) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(url);
        ds.setUser(user);
        ds.setPassword(password);
        return new JdbcConnectionPool(ds);
    }

    /**
     * Sets the maximum number of connections to use from now on.
     * The default value is 10 connections.
     *
     * @param max the maximum number of connections
     */
    public synchronized void setMaxConnections(int max) {
        if (max < 1) {
            throw new IllegalArgumentException("Invalid maxConnections value: " + max);
        }
        this.maxConnections = max;
        // notify waiting threads if the value was increased
        notifyAll();
    }

    /**
     * Gets the maximum number of connections to use.
     *
     * @return the max the maximum number of connections
     */
    public synchronized int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Gets the maximum time in seconds to wait for a free connection.
     *
     * @return the timeout in seconds
     */
    @Override
    public synchronized int getLoginTimeout() {
        return timeout;
    }

    /**
     * Sets the maximum time in seconds to wait for a free connection.
     * The default timeout is 30 seconds. Calling this method with the
     * value 0 will set the timeout to the default value.
     *
     * @param seconds the timeout, 0 meaning the default
     */
    @Override
    public synchronized void setLoginTimeout(int seconds) {
        if (seconds == 0) {
            seconds = DEFAULT_TIMEOUT;
        }
        this.timeout = seconds;
    }

    /**
     * Closes all unused pooled connections.
     * Exceptions while closing are written to the log stream (if set).
     */
    public synchronized void dispose() {
        if (isDisposed) {
            return;
        }
        isDisposed = true;
        for (PooledConnection aList : recycledConnections) {
            closeConnection(aList);
        }
    }

    /**
     * Retrieves a connection from the connection pool. If
     * <code>maxConnections</code> connections are already in use, the method
     * waits until a connection becomes available or <code>timeout</code>
     * seconds elapsed. When the application is finished using the connection,
     * it must close it in order to return it to the pool.
     * If no connection becomes available within the given timeout, an exception
     * with SQL state 08001 and vendor code 8001 is thrown.
     *
     * @return a new Connection object.
     * @throws SQLException when a new connection could not be established,
     *      or a timeout occurred
     */
    @Override
    public Connection getConnection() throws SQLException {
        long max = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeout);
        do {
            synchronized (this) {
                if (activeConnections < maxConnections) {
                    return getConnectionNow();
                }
                try {
                    wait(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        } while (System.nanoTime() <= max);
        throw new SQLException("Login timeout", "08001", 8001);
    }

    /**
     * INTERNAL
     */
    @Override
    public Connection getConnection(String user, String password) {
        throw new UnsupportedOperationException();
    }

    private Connection getConnectionNow() throws SQLException {
        if (isDisposed) {
            throw new IllegalStateException("Connection pool has been disposed.");
        }
        PooledConnection pc;
        if (!recycledConnections.isEmpty()) {
            pc = recycledConnections.remove(recycledConnections.size() - 1);
        } else {
            pc = dataSource.getPooledConnection();
        }
        Connection conn = pc.getConnection();
        activeConnections++;
        pc.addConnectionEventListener(this);
        return conn;
    }

    /**
     * This method usually puts the connection back into the pool. There are
     * some exceptions: if the pool is disposed, the connection is disposed as
     * well. If the pool is full, the connection is closed.
     *
     * @param pc the pooled connection
     */
    synchronized void recycleConnection(PooledConnection pc) {
        if (activeConnections <= 0) {
            throw new AssertionError();
        }
        activeConnections--;
        if (!isDisposed && activeConnections < maxConnections) {
            recycledConnections.add(pc);
        } else {
            closeConnection(pc);
        }
        if (activeConnections >= maxConnections - 1) {
            notifyAll();
        }
    }

    private void closeConnection(PooledConnection pc) {
        try {
            pc.close();
        } catch (SQLException e) {
            if (logWriter != null) {
                e.printStackTrace(logWriter);
            }
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public void connectionClosed(ConnectionEvent event) {
        PooledConnection pc = (PooledConnection) event.getSource();
        pc.removeConnectionEventListener(this);
        recycleConnection(pc);
    }

    /**
     * INTERNAL
     */
    @Override
    public void connectionErrorOccurred(ConnectionEvent event) {
        // not used
    }

    /**
     * Returns the number of active (open) connections of this pool. This is the
     * number of <code>Connection</code> objects that have been issued by
     * getConnection() for which <code>Connection.close()</code> has
     * not yet been called.
     *
     * @return the number of active connections.
     */
    public synchronized int getActiveConnections() {
        return activeConnections;
    }

    /**
     * INTERNAL
     */
    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    /**
     * INTERNAL
     */
    @Override
    public void setLogWriter(PrintWriter logWriter) {
        this.logWriter = logWriter;
    }

    /**
     * [Not supported] Return an object of this class if possible.
     *
     * @param iface the class
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw DbException.getUnsupportedException("unwrap");
    }

    /**
     * [Not supported] Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw DbException.getUnsupportedException("isWrapperFor");
    }

    /**
     * [Not supported]
     */
    @Override
    public Logger getParentLogger() {
        return null;
    }


}
