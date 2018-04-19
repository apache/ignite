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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import javax.sql.DataSource;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.internal.util.typedef.F;

/**
 * JDBC thin DataSource implementation.
 */
public class JdbcThinDataSource extends ConnectionPropertiesImpl implements DataSource {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger. */
    private static final Logger LOG = Logger.getLogger(JdbcThinConnection.class.getName());

    /** Login timeout. */
    private int loginTimeout;

    /** Log print writer. */
    private PrintWriter logOut;

    /** Custom logger handler. */
    private Handler logHnd;

    /** {@inheritDoc} */
    @Override public Connection getConnection() throws SQLException {
        return getConnection(null, null);
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection(String username, String pwd) throws SQLException {
        Properties props = storeToProperties();

        if (!F.isEmpty(username))
            props.put("user", username);

        if (!F.isEmpty(pwd))
            props.put("password", pwd);

        return IgniteJdbcThinDriver.register().connect(getUrl(), props);
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
        return logOut;
    }

    /** {@inheritDoc} */
    @Override public void setLogWriter(PrintWriter out) throws SQLException {
        if (logHnd != null)
            LOG.removeHandler(logHnd);

        logOut = out;

        if (out != null) {
            logHnd = new StreamHandler(new WriterOutputStream(out), new SimpleFormatter());

            LOG.addHandler(logHnd);
        }
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
     * @throws SQLException On error whrn URL is invalid.
     */
    public void setURL(String url) throws SQLException {
        setUrl(url);
    }

    /**
     *
     */
    private static final class WriterOutputStream extends OutputStream {
        /** Print Writer. */
        private PrintWriter pw;

        /**
         * @param pw Print writer.
         */
        WriterOutputStream(PrintWriter pw) {
            this.pw = pw;
        }

        /** {@inheritDoc} */
        @Override public void write(int b) throws IOException {
            write(new byte[] { (byte)b }, 0, 1);
        }
    }
}