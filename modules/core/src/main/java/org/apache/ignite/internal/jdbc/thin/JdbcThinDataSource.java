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

package org.apache.ignite.internal.jdbc.thin;

import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.util.typedef.F;

/**
 * JDBC result set metadata implementation.
 */
public class JdbcThinDataSource implements DataSource {
    /** Logger. */
    private static final Logger LOG = Logger.getLogger(JdbcThinDataSource.class.getName());

    /** Connection URL. */
    private String url;

    /** Schema name. */
    private String schema;

    /** Host. */
    private String host;

    /** Port. */
    private int port;

    /** Distributed joins. */
    private boolean distributedJoins;

    /** Enforce join order. */
    private boolean enforceJoinOrder;

    /** Collocated flag. */
    private boolean collocated;

    /** Replicated only flag. */
    private boolean replicatedOnly;

    /** Lazy execution query flag. */
    private boolean lazy;

    /** Flag to automatically close server cursor. */
    private boolean autoCloseServerCursor;

    /** TCP_NODELAY flag. */
    private boolean tcpNoDelay;

    /** Socket send buffer size. */
    private int socketSendBuffer;

    /** Socket receive buffer size. */
    private int socketRecvBuffer;

    /** Login timeout. */
    private int loginTimeout;

    /** {@inheritDoc} */
    @Override public Connection getConnection() throws SQLException {
        Properties props = exposeAsProperties();

        return IgniteJdbcThinDriver.register().connect(getUrl(), props);
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!isWrapperFor(iface))
            throw new SQLException("DataSource is not a wrapper for " + iface.getName());

        return (T)this;
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(JdbcThinDataSource.class);
    }

    /** {@inheritDoc} */
    @Override public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void setLogWriter(PrintWriter out) throws SQLException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeout = seconds;
    }

    /** {@inheritDoc} */
    @Override public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    /** {@inheritDoc} */
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger("org.apache.ignite");
    }

    /**
     * Different application servers us different format (URL & url).
     * @return Connection URL.
     */
    public String getURL() {
        return getUrl();
    }

    /**
     * Different application servers us different format (URL & url).
     * @param url Connection URL.
     */
    public void setURL(String url) {
        setUrl(url);
    }

    /**
     * Different application servers us different format (URL & url).
     * @return Connection URL.
     */
    public String getUrl() {
        if (url != null)
            return url;
        else {
            if (F.isEmpty(host))
                return null;

            StringBuilder sbUrl = new StringBuilder(host);

            if (port > 0)
                sbUrl.append(':').append(port);

            if (!F.isEmpty(schema))
                sbUrl.append('/').append(schema);

            return sbUrl.toString();
        }
    }

    /**
     * Different application servers us different format (URL & url).
     * @param url Connection URL.
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * @return Database schema to access.
     */
    public String getSchema() {
        return schema;
    }

    /**
     * @param schema Database schema to access.
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * @return Ignite host to connect.
     */
    public String getHost() {
        return host;
    }

    /**
     * @param host Ignite host to connect.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * @return Connection port.
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port Connection port.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * @return Distribute join flag (SQL hint).
     * @see SqlFieldsQuery#isDistributedJoins()
     */
    public boolean isDistributedJoins() {
        return distributedJoins;
    }

    /**
     * @param distributedJoins Distributed join flag (SQL hint).
     * @see SqlFieldsQuery#setDistributedJoins(boolean)
     */
    public void setDistributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;
    }

    /**
     * @return Enforce join order flag (SQL hint).
     * @see SqlFieldsQuery#isEnforceJoinOrder()
     */
    public boolean isEnforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * @param enforceJoinOrder Enforce join order flag (SQL hint).
     * @see SqlFieldsQuery#setEnforceJoinOrder(boolean)
     */
    public void setEnforceJoinOrder(boolean enforceJoinOrder) {
        this.enforceJoinOrder = enforceJoinOrder;
    }

    /**
     * @return Collocated query flag (SQL hint).
     * @see SqlFieldsQuery#isCollocated()
     */
    public boolean isCollocated() {
        return collocated;
    }

    /**
     * @param collocated Collocated query flag (SQL hint).
     * @see SqlFieldsQuery#setCollocated(boolean)
     */
    public void setCollocated(boolean collocated) {
        this.collocated = collocated;
    }

    /**
     * @return Replicated only flag (SQL hint).
     * @see SqlFieldsQuery#isReplicatedOnly()
     */
    public boolean isReplicatedOnly() {
        return replicatedOnly;
    }

    /**
     * @param replicatedOnly Replicated only flag (SQL hint).
     * @see SqlFieldsQuery#setReplicatedOnly(boolean)
     */
    public void setReplicatedOnly(boolean replicatedOnly) {
        this.replicatedOnly = replicatedOnly;
    }

    /**
     * @return Lazy query execution flag (SQL hint).
     * @see SqlFieldsQuery#isLazy()
     */
    public boolean isLazy() {
        return lazy;
    }

    /**
     * @param lazy Lazy query execution flag (SQL hint).
     * @see SqlFieldsQuery#setLazy(boolean)
     */
    public void setLazy(boolean lazy) {
        this.lazy = lazy;
    }

    /**
     * @return Auto close cursor on the server flag.
     */
    public boolean isAutoCloseServerCursor() {
        return autoCloseServerCursor;
    }

    /**
     * @param autoCloseServerCursor Auto close cursor on the server flag.
     */
    public void setAutoCloseServerCursor(boolean autoCloseServerCursor) {
        this.autoCloseServerCursor = autoCloseServerCursor;
    }

    /**
     * @return TCP_NODELAY flag.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * @param tcpNoDelay TCP_NODELAY flag.
     */
    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    /**
     * @return Socket send buffer size.
     */
    public int getSocketSendBuffer() {
        return socketSendBuffer;
    }

    /**
     * @param socketSendBuffer Socket send buffer size.
     */
    public void setSocketSendBuffer(int socketSendBuffer) {
        this.socketSendBuffer = socketSendBuffer;
    }

    /**
     * @return Socket receive buffer size.
     */
    public int getSocketReceiveBuffer() {
        return socketRecvBuffer;
    }

    /**
     * @param socketRecvBuffer Socket receive buffer size.
     */
    public void setSocketReceiveBuffer(int socketRecvBuffer) {
        this.socketRecvBuffer = socketRecvBuffer;
    }

    /**
     * @return Properties
     */
    private Properties exposeAsProperties() {
        Properties props = new Properties();

        props.setProperty("distributedJoins", Boolean.toString(distributedJoins));
        props.setProperty("enforceJoinOrder", Boolean.toString(enforceJoinOrder));
        props.setProperty("collocated", Boolean.toString(collocated));
        props.setProperty("replicatedOnly", Boolean.toString(replicatedOnly));
        props.setProperty("lazy", Boolean.toString(autoCloseServerCursor));
        props.setProperty("tcpNoDelay", Boolean.toString(tcpNoDelay));

        props.setProperty("socketSendBuffer", Integer.toString(socketSendBuffer));
        props.setProperty("socketRecvBuffer", Integer.toString(socketRecvBuffer));

        return props;
    }
}