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

package org.apache.ignite.agent.handlers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.ignite.agent.AgentConfiguration;
import org.apache.ignite.agent.remote.Remote;
import org.apache.ignite.agent.testdrive.AgentMetadataTestDrive;
import org.apache.ignite.schema.parser.DbMetadataReader;
import org.apache.ignite.schema.parser.DbTable;
import org.apache.log4j.Logger;

import static org.apache.ignite.agent.AgentUtils.resolvePath;

/**
 * Remote API to extract database metadata.
 */
public class DatabaseMetadataExtractor {
    /** */
    private static final Logger log = Logger.getLogger(DatabaseMetadataExtractor.class.getName());

    /** */
    private final File driversFolder;

    /**
     * @param cfg Config.
     */
    public DatabaseMetadataExtractor(AgentConfiguration cfg) {
        driversFolder = resolvePath(cfg.driversFolder() == null ? "jdbc-drivers" : cfg.driversFolder());
    }

    /**
     * @param jdbcDriverJarPath JDBC driver JAR path.
     * @param jdbcDriverCls JDBC driver class.
     * @param jdbcUrl JDBC URL.
     * @param jdbcInfo Properties to connect to database.
     * @return Connection to database.
     * @throws SQLException
     */
    private Connection connect(String jdbcDriverJarPath, String jdbcDriverCls, String jdbcUrl, Properties jdbcInfo) throws SQLException {
        if (!new File(jdbcDriverJarPath).isAbsolute() && driversFolder != null)
            jdbcDriverJarPath = new File(driversFolder, jdbcDriverJarPath).getPath();

        if (AgentMetadataTestDrive.isTestDriveUrl(jdbcUrl))
            AgentMetadataTestDrive.testDrive();

        return DbMetadataReader.getInstance().connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo);
    }

    /**
     * @param jdbcDriverJarPath JDBC driver JAR path.
     * @param jdbcDriverCls JDBC driver class.
     * @param jdbcUrl JDBC URL.
     * @param jdbcInfo Properties to connect to database.
     * @return Collection of schema names.
     * @throws SQLException
     */
    @Remote
    public Collection<String> schemas(String jdbcDriverJarPath, String jdbcDriverCls, String jdbcUrl,
        Properties jdbcInfo) throws SQLException {
        log.debug("Start collecting database schemas [driver jar=" + jdbcDriverJarPath +
            ", driver class=" + jdbcDriverCls + ", url=" + jdbcUrl + "]");

        try (Connection conn = connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo)) {
            Collection<String> schemas = DbMetadataReader.getInstance().schemas(conn);

            log.debug("Finished collection of schemas [url=" + jdbcUrl + ", count="+ schemas.size() +"]");

            return schemas;
        }
        catch (SQLException e) {
            log.error("Failed to collect schemas", e);

            throw e;
        }
    }

    /**
     * @param jdbcDriverJarPath JDBC driver JAR path.
     * @param jdbcDriverCls JDBC driver class.
     * @param jdbcUrl JDBC URL.
     * @param jdbcInfo Properties to connect to database.
     * @param schemas List of schema names to process.
     * @param tblsOnly If {@code true} then only tables will be processed otherwise views also will be processed.
     * @return Collection of tables.
     */
    @Remote
    public Collection<DbTable> metadata(String jdbcDriverJarPath, String jdbcDriverCls, String jdbcUrl,
        Properties jdbcInfo, List<String> schemas, boolean tblsOnly) throws SQLException {
        log.debug("Start collecting database metadata [driver jar=" + jdbcDriverJarPath +
            ", driver class=" + jdbcDriverCls + ", url=" + jdbcUrl + "]");

        try (Connection conn = connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo)) {
            Collection<DbTable> metadata = DbMetadataReader.getInstance().metadata(conn, schemas, tblsOnly);

            log.debug("Finished collection of metadata [url=" + jdbcUrl + ", count="+ metadata.size() +"]");

            return metadata;
        }
        catch (SQLException e) {
            log.error("Failed to collect metadata", e);

            throw e;
        }
    }

    /**
     * @return Drivers in drivers folder
     * @see AgentConfiguration#driversFolder
     */
    @Remote
    public List<JdbcDriver> availableDrivers() {
        if (driversFolder == null) {
            log.info("JDBC drivers folder not specified, returning empty list");

            return Collections.emptyList();
        }

        log.debug("Collecting JDBC drivers in folder: " + driversFolder.getPath());

        File[] list = driversFolder.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        });

        if (list == null) {
            log.info("JDBC drivers folder has no files, returning empty list");

            return Collections.emptyList();
        }

        List<JdbcDriver> res = new ArrayList<>();

        for (File file : list) {
            try {
                boolean win = System.getProperty("os.name").contains("win");

                URL url = new URL("jar", null, "file:" + (win ? "/" : "") + file.getPath() + "!/META-INF/services/java.sql.Driver");

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                    String jdbcDriverCls = reader.readLine();

                    res.add(new JdbcDriver(file.getName(), jdbcDriverCls));

                    log.debug("Found: [driver=" + file + ", class=" + jdbcDriverCls + "]");
                }
            }
            catch (IOException e) {
                res.add(new JdbcDriver(file.getName(), null));

                log.info("Found: [driver=" + file + "]");
                log.info("Failed to detect driver class: " + e.getMessage());
            }
        }

        return res;
    }

    /**
     * Wrapper class for later to be transformed to JSON and send to Web Console.
     */
    private static class JdbcDriver {
        /** */
        private final String jdbcDriverJar;
        /** */
        private final String jdbcDriverClass;

        /**
         * @param jdbcDriverJar File name of driver jar file.
         * @param jdbcDriverClass Optional JDBC driver class.
         */
        public JdbcDriver(String jdbcDriverJar, String jdbcDriverClass) {
            this.jdbcDriverJar = jdbcDriverJar;
            this.jdbcDriverClass = jdbcDriverClass;
        }
    }
}
