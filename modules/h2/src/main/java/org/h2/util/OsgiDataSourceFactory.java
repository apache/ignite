/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.XADataSource;
import org.h2.engine.Constants;
import org.h2.jdbcx.JdbcDataSource;
import org.osgi.framework.BundleContext;
import org.osgi.service.jdbc.DataSourceFactory;

/**
 * This class implements the OSGi DataSourceFactory interface for the H2 JDBC
 * driver. The following standard configuration properties are supported:
 * {@link #JDBC_USER}, {@link #JDBC_PASSWORD}, {@link #JDBC_DESCRIPTION},
 * {@link #JDBC_DATASOURCE_NAME}, {@link #JDBC_NETWORK_PROTOCOL},
 * {@link #JDBC_URL}, {@link #JDBC_SERVER_NAME}, {@link #JDBC_PORT_NUMBER}. The
 * following standard configuration properties are not supported:
 * {@link #JDBC_ROLE_NAME}, {@link #JDBC_DATABASE_NAME},
 * {@link #JDBC_INITIAL_POOL_SIZE}, {@link #JDBC_MAX_POOL_SIZE},
 * {@link #JDBC_MIN_POOL_SIZE}, {@link #JDBC_MAX_IDLE_TIME},
 * {@link #JDBC_MAX_STATEMENTS}, {@link #JDBC_PROPERTY_CYCLE}. Any other
 * property will be treated as a H2 specific option. If the {@link #JDBC_URL}
 * property is passed to any of the DataSource factories, the following
 * properties will be ignored: {@link #JDBC_DATASOURCE_NAME},
 * {@link #JDBC_NETWORK_PROTOCOL}, {@link #JDBC_SERVER_NAME},
 * {@link #JDBC_PORT_NUMBER}.
 *
 * @author Per Otterstrom
 */
public class OsgiDataSourceFactory implements DataSourceFactory {
    private final org.h2.Driver driver;

    public OsgiDataSourceFactory(org.h2.Driver driver) {
        this.driver = driver;
    }

    /**
     * Creates a basic data source.
     *
     * @param properties the properties for the data source.
     * @throws SQLException if unsupported properties are supplied, or if data
     *             source can not be created.
     * @return a new data source.
     */
    @Override
    public DataSource createDataSource(Properties properties)
            throws SQLException {
        // Make copy of properties
        Properties propertiesCopy = new Properties();
        if (properties != null) {
            propertiesCopy.putAll(properties);
        }

        // Verify that no unsupported standard options are used
        rejectUnsupportedOptions(propertiesCopy);

        // Standard pool properties in OSGi not applicable here
        rejectPoolingOptions(propertiesCopy);

        JdbcDataSource dataSource = new JdbcDataSource();

        setupH2DataSource(dataSource, propertiesCopy);

        return dataSource;
    }

    /**
     * Creates a pooled data source.
     *
     * @param properties the properties for the data source.
     * @throws SQLException if unsupported properties are supplied, or if data
     *             source can not be created.
     * @return a new data source.
     */
    @Override
    public ConnectionPoolDataSource createConnectionPoolDataSource(
            Properties properties) throws SQLException {
        // Make copy of properties
        Properties propertiesCopy = new Properties();
        if (properties != null) {
            propertiesCopy.putAll(properties);
        }

        // Verify that no unsupported standard options are used
        rejectUnsupportedOptions(propertiesCopy);

        // The integrated connection pool is H2 is not configurable
        rejectPoolingOptions(propertiesCopy);

        JdbcDataSource dataSource = new JdbcDataSource();

        setupH2DataSource(dataSource, propertiesCopy);

        return dataSource;
    }

    /**
     * Creates a pooled XA data source.
     *
     * @param properties the properties for the data source.
     * @throws SQLException if unsupported properties are supplied, or if data
     *             source can not be created.
     * @return a new data source.
     */
    @Override
    public XADataSource createXADataSource(Properties properties)
            throws SQLException {
        // Make copy of properties
        Properties propertiesCopy = new Properties();
        if (properties != null) {
            propertiesCopy.putAll(properties);
        }

        // Verify that no unsupported standard options are used
        rejectUnsupportedOptions(propertiesCopy);

        // The integrated connection pool is H2 is not configurable
        rejectPoolingOptions(propertiesCopy);

        JdbcDataSource dataSource = new JdbcDataSource();

        setupH2DataSource(dataSource, propertiesCopy);

        return dataSource;
    }

    /**
     * Returns a driver. The H2 driver does not support any properties.
     *
     * @param properties must be null or empty list.
     * @throws SQLException if any property is supplied.
     * @return a driver.
     */
    @Override
    public java.sql.Driver createDriver(Properties properties)
            throws SQLException {
        if (properties != null && !properties.isEmpty()) {
            // No properties supported
            throw new SQLException();
        }
        return driver;
    }

    /**
     * Checker method that will throw if any unsupported standard OSGi options
     * is present.
     *
     * @param p the properties to check
     * @throws SQLFeatureNotSupportedException if unsupported properties are
     *             present
     */
    private static void rejectUnsupportedOptions(Properties p)
            throws SQLFeatureNotSupportedException {
        // Unsupported standard properties in OSGi
        if (p.containsKey(DataSourceFactory.JDBC_ROLE_NAME)) {
            throw new SQLFeatureNotSupportedException("The " +
                    DataSourceFactory.JDBC_ROLE_NAME +
                    " property is not supported by H2");
        }
        if (p.containsKey(DataSourceFactory.JDBC_DATASOURCE_NAME)) {
            throw new SQLFeatureNotSupportedException("The " +
                    DataSourceFactory.JDBC_DATASOURCE_NAME +
                    " property is not supported by H2");
        }
    }

    /**
     * Applies common OSGi properties to a H2 data source. Non standard
     * properties will be applied as H2 options.
     *
     * @param dataSource the data source to configure
     * @param p the properties to apply to the data source
     */
    private static void setupH2DataSource(JdbcDataSource dataSource,
            Properties p) {
        // Setting user and password
        if (p.containsKey(DataSourceFactory.JDBC_USER)) {
            dataSource.setUser((String) p.remove(DataSourceFactory.JDBC_USER));
        }
        if (p.containsKey(DataSourceFactory.JDBC_PASSWORD)) {
            dataSource.setPassword((String) p
                    .remove(DataSourceFactory.JDBC_PASSWORD));
        }

        // Setting description
        if (p.containsKey(DataSourceFactory.JDBC_DESCRIPTION)) {
            dataSource.setDescription((String) p
                    .remove(DataSourceFactory.JDBC_DESCRIPTION));
        }

        // Setting URL
        StringBuilder connectionUrl = new StringBuilder();
        if (p.containsKey(DataSourceFactory.JDBC_URL)) {
            // Use URL if specified
            connectionUrl.append(p.remove(DataSourceFactory.JDBC_URL));
            // Remove individual properties
            p.remove(DataSourceFactory.JDBC_NETWORK_PROTOCOL);
            p.remove(DataSourceFactory.JDBC_SERVER_NAME);
            p.remove(DataSourceFactory.JDBC_PORT_NUMBER);
            p.remove(DataSourceFactory.JDBC_DATABASE_NAME);
        } else {
            // Creating URL from individual properties
            connectionUrl.append(Constants.START_URL);

            // Set network protocol (tcp/ssl) or DB type (mem/file)
            String protocol = "";
            if (p.containsKey(DataSourceFactory.JDBC_NETWORK_PROTOCOL)) {
                protocol = (String) p.remove(DataSourceFactory.JDBC_NETWORK_PROTOCOL);
                connectionUrl.append(protocol).append(":");
            }

            // Host name and/or port
            if (p.containsKey(DataSourceFactory.JDBC_SERVER_NAME)) {
                connectionUrl.append("//").append(
                        p.remove(DataSourceFactory.JDBC_SERVER_NAME));

                if (p.containsKey(DataSourceFactory.JDBC_PORT_NUMBER)) {
                    connectionUrl.append(":").append(
                            p.remove(DataSourceFactory.JDBC_PORT_NUMBER));
                }

                connectionUrl.append("/");
            } else if (p.containsKey(
                    DataSourceFactory.JDBC_PORT_NUMBER)) {
                // Assume local host if only port was set
                connectionUrl
                        .append("//localhost:")
                        .append(p.remove(DataSourceFactory.JDBC_PORT_NUMBER))
                        .append("/");
            } else if (protocol.equals("tcp") || protocol.equals("ssl")) {
                // Assume local host if network protocol is set, but no host or
                // port is set
                connectionUrl.append("//localhost/");
            }

            // DB path and name
            if (p.containsKey(DataSourceFactory.JDBC_DATABASE_NAME)) {
                connectionUrl.append(
                        p.remove(DataSourceFactory.JDBC_DATABASE_NAME));
            }
        }

        // Add remaining properties as options
        for (Object option : p.keySet()) {
            connectionUrl.append(";").append(option).append("=")
                    .append(p.get(option));
        }

        if (connectionUrl.length() > Constants.START_URL.length()) {
            dataSource.setURL(connectionUrl.toString());
        }
    }

    /**
     * Checker method that will throw if any pooling related standard OSGi
     * options are present.
     *
     * @param p the properties to check
     * @throws SQLFeatureNotSupportedException if unsupported properties are
     *             present
     */
    private static void rejectPoolingOptions(Properties p)
            throws SQLFeatureNotSupportedException {
        if (p.containsKey(DataSourceFactory.JDBC_INITIAL_POOL_SIZE) ||
                p.containsKey(DataSourceFactory.JDBC_MAX_IDLE_TIME) ||
                p.containsKey(DataSourceFactory.JDBC_MAX_POOL_SIZE) ||
                p.containsKey(DataSourceFactory.JDBC_MAX_STATEMENTS) ||
                p.containsKey(DataSourceFactory.JDBC_MIN_POOL_SIZE) ||
                p.containsKey(DataSourceFactory.JDBC_PROPERTY_CYCLE)) {
            throw new SQLFeatureNotSupportedException(
                    "Pooling properties are not supported by H2");
        }
    }

    /**
     * Register the H2 JDBC driver service.
     *
     * @param bundleContext the bundle context
     * @param driver the driver
     */
    static void registerService(BundleContext bundleContext,
            org.h2.Driver driver) {
        Properties properties = new Properties();
        properties.put(
                DataSourceFactory.OSGI_JDBC_DRIVER_CLASS,
                org.h2.Driver.class.getName());
        properties.put(
                DataSourceFactory.OSGI_JDBC_DRIVER_NAME,
                "H2 JDBC Driver");
        properties.put(
                DataSourceFactory.OSGI_JDBC_DRIVER_VERSION,
                Constants.getFullVersion());
        bundleContext.registerService(
                DataSourceFactory.class.getName(),
                new OsgiDataSourceFactory(driver), properties);
    }
}
