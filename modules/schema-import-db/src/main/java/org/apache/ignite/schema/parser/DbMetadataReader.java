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

package org.apache.ignite.schema.parser;

import org.apache.ignite.schema.parser.dialect.*;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;

/**
 * Singleton to extract database metadata.
 */
public class DbMetadataReader {
    /** Logger. */
    private static final Logger log = Logger.getLogger(DbMetadataReader.class.getName());

    /** */
    private static final DbMetadataReader INSTANCE = new DbMetadataReader();

    /** */
    private final Map<String, Driver> drivers = new HashMap<>();

    /**
     * Default constructor.
     */
    private DbMetadataReader() {
        // No-op.
    }

    /**
     * @return Instance.
     */
    public static DbMetadataReader getInstance() {
        return INSTANCE;
    }

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
            else if (dbProductName.startsWith("DB2/"))
                return new DB2MetadataDialect();
            else if (dbProductName.equals("MySQL"))
                return new MySQLMetadataDialect();
            else
                return new JdbcMetadataDialect();
        }
        catch (SQLException e) {
            log.log(Level.SEVERE, "Failed to resolve dialect (JdbcMetaDataDialect will be used.", e);

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
