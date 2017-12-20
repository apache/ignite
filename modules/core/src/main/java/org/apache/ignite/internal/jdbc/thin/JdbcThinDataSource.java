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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.internal.util.typedef.F;

/**
 * JDBC result set metadata implementation.
 */
public class JdbcThinDataSource extends ConnectionPropertiesImpl implements DataSource {
    /** */
    private static final long serialVersionUID = 0L;

    /** Connection URL. */
    private String url;

    /** Schema name. */
    private String schema;

    /** Login timeout. */
    private int loginTimeout;

    /** {@inheritDoc} */
    @Override public Connection getConnection() throws SQLException {
        return IgniteJdbcThinDriver.register().connect(getUrl(), storeToProperties());
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection(String username, String pwd) throws SQLException {
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
            if (F.isEmpty(getHost()))
                return null;

            StringBuilder sbUrl = new StringBuilder(JdbcThinUtils.URL_PREFIX).append(getHost());

            if (getPort() > 0)
                sbUrl.append(':').append(getPort());

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
}