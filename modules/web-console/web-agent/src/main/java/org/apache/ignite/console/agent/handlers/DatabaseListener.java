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

package org.apache.ignite.console.agent.handlers;

import io.socket.emitter.Emitter;
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
import java.util.Map;
import java.util.Properties;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.demo.AgentMetadataDemo;
import org.apache.ignite.schema.parser.DbMetadataReader;
import org.apache.ignite.schema.parser.DbTable;
import org.apache.log4j.Logger;

import static org.apache.ignite.console.agent.AgentUtils.resolvePath;

/**
 * API to extract database metadata.
 */
public class DatabaseListener {
    /** */
    private static final Logger log = Logger.getLogger(DatabaseListener.class.getName());

    /** */
    private final File driversFolder;

    /** */
    private final AbstractListener schemasLsnr = new AbstractListener() {
        @Override public Object execute(Map<String, Object> args) throws Exception {
            String driverPath = null;

            if (args.containsKey("driverPath"))
                driverPath = args.get("driverPath").toString();

            if (!args.containsKey("driverClass"))
                throw new IllegalArgumentException("Missing driverClass in arguments: " + args);

            String driverCls = args.get("driverClass").toString();

            if (!args.containsKey("url"))
                throw new IllegalArgumentException("Missing url in arguments: " + args);

            String url = args.get("url").toString();

            if (!args.containsKey("info"))
                throw new IllegalArgumentException("Missing info in arguments: " + args);

            Properties info = new Properties();

            info.putAll((Map)args.get("info"));

            return schemas(driverPath, driverCls, url, info);
        }
    };

    private final AbstractListener metadataLsnr = new AbstractListener() {
        @SuppressWarnings("unchecked")
        @Override public Object execute(Map<String, Object> args) throws Exception {
            String driverPath = null;

            if (args.containsKey("driverPath"))
                driverPath = args.get("driverPath").toString();

            if (!args.containsKey("driverClass"))
                throw new IllegalArgumentException("Missing driverClass in arguments: " + args);

            String driverCls = args.get("driverClass").toString();

            if (!args.containsKey("url"))
                throw new IllegalArgumentException("Missing url in arguments: " + args);

            String url = args.get("url").toString();

            if (!args.containsKey("info"))
                throw new IllegalArgumentException("Missing info in arguments: " + args);

            Properties info = new Properties();

            info.putAll((Map)args.get("info"));

            if (!args.containsKey("schemas"))
                throw new IllegalArgumentException("Missing schemas in arguments: " + args);

            List<String> schemas = (List<String>)args.get("schemas");

            if (!args.containsKey("tablesOnly"))
                throw new IllegalArgumentException("Missing tablesOnly in arguments: " + args);

            boolean tblsOnly = (boolean)args.get("tablesOnly");

            return metadata(driverPath, driverCls, url, info, schemas, tblsOnly);
        }
    };

    private final AbstractListener availableDriversLsnr = new AbstractListener() {
        @Override public Object execute(Map<String, Object> args) throws Exception {
            if (driversFolder == null) {
                log.info("JDBC drivers folder not specified, returning empty list");

                return Collections.emptyList();
            }

            if (log.isDebugEnabled())
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

                    URL url = new URL("jar", null,
                        "file:" + (win ? "/" : "") + file.getPath() + "!/META-INF/services/java.sql.Driver");

                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
                        String jdbcDriverCls = reader.readLine();

                        res.add(new JdbcDriver(file.getName(), jdbcDriverCls));

                        if (log.isDebugEnabled())
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
    };

    /**
     * @param cfg Config.
     */
    public DatabaseListener(AgentConfiguration cfg) {
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
    private Connection connect(String jdbcDriverJarPath, String jdbcDriverCls, String jdbcUrl,
        Properties jdbcInfo) throws SQLException {
        if (AgentMetadataDemo.isTestDriveUrl(jdbcUrl))
            return AgentMetadataDemo.testDrive();

        if (!new File(jdbcDriverJarPath).isAbsolute() && driversFolder != null)
            jdbcDriverJarPath = new File(driversFolder, jdbcDriverJarPath).getPath();

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
    protected Collection<String> schemas(String jdbcDriverJarPath, String jdbcDriverCls, String jdbcUrl,
        Properties jdbcInfo) throws SQLException {
        if (log.isDebugEnabled())
            log.debug("Start collecting database schemas [drvJar=" + jdbcDriverJarPath +
                ", drvCls=" + jdbcDriverCls + ", jdbcUrl=" + jdbcUrl + "]");

        try (Connection conn = connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo)) {
            Collection<String> schemas = DbMetadataReader.getInstance().schemas(conn);

            if (log.isDebugEnabled())
                log.debug("Finished collection of schemas [jdbcUrl=" + jdbcUrl + ", count=" + schemas.size() + "]");

            return schemas;
        }
        catch (Throwable e) {
            log.error("Failed to collect schemas", e);

            throw new SQLException("Failed to collect schemas", e);
        }
    }

    /**
     * Listener for drivers.
     *
     * @return Drivers in drivers folder
     * @see AgentConfiguration#driversFolder
     */
    public Emitter.Listener availableDriversListener() {
        return availableDriversLsnr;
    }

    /**
     * Listener for schema names.
     *
     * @return Collection of schema names.
     */
    public Emitter.Listener schemasListener() {
        return schemasLsnr;
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
    protected Collection<DbTable> metadata(String jdbcDriverJarPath, String jdbcDriverCls, String jdbcUrl,
        Properties jdbcInfo, List<String> schemas, boolean tblsOnly) throws SQLException {
        if (log.isDebugEnabled())
            log.debug("Start collecting database metadata [drvJar=" + jdbcDriverJarPath +
                ", drvCls=" + jdbcDriverCls + ", jdbcUrl=" + jdbcUrl + "]");

        try (Connection conn = connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo)) {
            Collection<DbTable> metadata = DbMetadataReader.getInstance().metadata(conn, schemas, tblsOnly);

            if (log.isDebugEnabled())
                log.debug("Finished collection of metadata [jdbcUrl=" + jdbcUrl + ", count=" + metadata.size() + "]");

            return metadata;
        }
        catch (Throwable e) {
            log.error("Failed to collect metadata", e);

            throw new SQLException("Failed to collect metadata", e);
        }
    }

    /**
     * Listener for tables.
     *
     * @return Collection of tables.
     */
    public Emitter.Listener metadataListener() {
        return metadataLsnr;
    }

    /**
     * Stop handler.
     */
    public void stop() {
        availableDriversLsnr.stop();

        schemasLsnr.stop();

        metadataLsnr.stop();
    }

    /**
     * Wrapper class for later to be transformed to JSON and send to Web Console.
     */
    private static class JdbcDriver {
        /** */
        public final String jdbcDriverJar;
        /** */
        public final String jdbcDriverCls;

        /**
         * @param jdbcDriverJar File name of driver jar file.
         * @param jdbcDriverCls Optional JDBC driver class.
         */
        public JdbcDriver(String jdbcDriverJar, String jdbcDriverCls) {
            this.jdbcDriverJar = jdbcDriverJar;
            this.jdbcDriverCls = jdbcDriverCls;
        }
    }
}
