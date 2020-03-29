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

package org.apache.ignite.console.agent.db;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.ignite.console.agent.db.dialect.DB2MetadataDialect;
import org.apache.ignite.console.agent.db.dialect.DatabaseMetadataDialect;
import org.apache.ignite.console.agent.db.dialect.JdbcMetadataDialect;
import org.apache.ignite.console.agent.db.dialect.MySQLMetadataDialect;
import org.apache.ignite.console.agent.db.dialect.OracleMetadataDialect;
import org.apache.log4j.Logger;

/**
 * Singleton to extract database metadata.
 */
public class DbMetadataReader {
    /** Logger. */
    private static final Logger log = Logger.getLogger(DbMetadataReader.class.getName());

    /** */
    private final Map<String, Driver> drivers = new HashMap<>();

    /**
     * Get specified dialect object for selected database.
     *
     * @param conn Connection to database.
     * @return Specific dialect object.
     */
    private DatabaseMetadataDialect dialect(Connection conn) {
        try {
            String dbProductName = conn.getMetaData().getDatabaseProductName();

            if ("Oracle".equals(dbProductName))
                return new OracleMetadataDialect();

            if (dbProductName.startsWith("DB2/"))
                return new DB2MetadataDialect();

            if ("MySQL".equals(dbProductName))
                return new MySQLMetadataDialect();

            return new JdbcMetadataDialect();
        }
        catch (SQLException e) {
            log.error("Failed to resolve dialect (JdbcMetaDataDialect will be used.", e);

            return new JdbcMetadataDialect();
        }
    }

    /**
     * Get list of schemas from database.
     *
     * @param conn Connection to database.
     * @return List of schema names.
     * @throws SQLException If schemas loading failed.
     */
    public Collection<String> schemas(Connection conn) throws SQLException  {
        return dialect(conn).schemas(conn);
    }

    /**
     * Extract DB metadata.
     *
     * @param conn Connection.
     * @param schemas List of database schemas to process. In case of empty list all schemas will be processed.
     * @param tblsOnly Tables only flag.
     */
    public Collection<DbTable> metadata(Connection conn, List<String> schemas, boolean tblsOnly) throws SQLException {
        return dialect(conn).tables(conn, schemas, tblsOnly);
    }

    /**
     * Connect to database.
     *
     * @param jdbcDrvJarPath Path to JDBC driver.
     * @param jdbcDrvCls JDBC class name.
     * @param jdbcUrl JDBC connection URL.
     * @param jdbcInfo Connection properties.
     * @return Connection to database.
     * @throws SQLException if connection failed.
     */
    public Connection connect(String jdbcDrvJarPath, String jdbcDrvCls, String jdbcUrl, Properties jdbcInfo)
        throws SQLException {
        Driver drv = drivers.get(jdbcDrvCls);

        if (drv == null) {
            if (jdbcDrvJarPath.isEmpty())
                throw new IllegalStateException("Driver jar file name is not specified.");

            File drvJar = new File(jdbcDrvJarPath);

            if (!drvJar.exists())
                throw new IllegalStateException("Driver jar file is not found.");

            try {
                URL u = new URL("jar:" + drvJar.toURI() + "!/");

                URLClassLoader ucl = URLClassLoader.newInstance(new URL[] {u});

                drv = (Driver)Class.forName(jdbcDrvCls, true, ucl).newInstance();

                drivers.put(jdbcDrvCls, drv);
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        Connection conn = drv.connect(jdbcUrl, jdbcInfo);

        if (conn == null)
            throw new IllegalStateException("Connection was not established (JDBC driver returned null value).");

        return conn;
    }
}
