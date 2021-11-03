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

package org.apache.ignite.jdbc;

import static org.apache.ignite.internal.jdbc.ConnectionPropertiesImpl.URL_PREFIX;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.jdbc.ConnectionPropertiesImpl;
import org.apache.ignite.internal.jdbc.JdbcConnection;

/**
 * JDBC driver thin implementation for Apache Ignite 3.x.
 *
 * <p>Driver allows to get distributed data from Ignite Data Storage using standard SQL queries and standard JDBC API.
 */
public class IgniteJdbcDriver implements Driver {
    /** Driver instance. */
    private static Driver instance;

    static {
        register();
    }

    /** Major version. */
    private static final int MAJOR_VER = ProtocolVersion.LATEST_VER.major();

    /** Minor version. */
    private static final int MINOR_VER = ProtocolVersion.LATEST_VER.minor();

    /** {@inheritDoc} */
    @Override
    public Connection connect(String url, Properties props) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();

        connProps.init(url, props);

        return new JdbcConnection(connProps);
    }

    /** {@inheritDoc} */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(URL_PREFIX);
    }

    /** {@inheritDoc} */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();

        connProps.init(url, info);

        return connProps.getDriverPropertyInfo();
    }

    /** {@inheritDoc} */
    @Override
    public int getMajorVersion() {
        return MAJOR_VER;
    }

    /** {@inheritDoc} */
    @Override
    public int getMinorVersion() {
        return MINOR_VER;
    }

    /** {@inheritDoc} */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("java.util.logging is not used.");
    }

    /**
     * Register the driver instance.
     *
     * @return Driver instance.
     * @throws RuntimeException when failed to register driver.
     */
    private static synchronized void register() {
        if (isRegistered()) {
            throw new RuntimeException("Driver is already registered. It can only be registered once.");
        }

        try {
            Driver registeredDriver = new IgniteJdbcDriver();
            DriverManager.registerDriver(registeredDriver);
            IgniteJdbcDriver.instance = registeredDriver;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register Ignite JDBC driver.", e);
        }
    }

    /**
     * Checks if Driver is instantiated.
     *
     * @return {@code true} if the driver is registered against {@link DriverManager}.
     */
    private static boolean isRegistered() {
        return instance != null;
    }
}
