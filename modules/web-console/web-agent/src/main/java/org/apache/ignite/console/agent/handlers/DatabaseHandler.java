/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.agent.handlers;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.db.DbMetadataReader;
import org.apache.ignite.console.agent.db.DbSchema;
import org.apache.ignite.console.agent.db.DbTable;
import org.apache.ignite.console.demo.AgentMetadataDemo;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.agent.AgentUtils.resolvePath;
import static org.apache.ignite.console.utils.Utils.fromJson;

/**
 * Handler extract database metadata for "Metadata import" dialog on Web Console.
 */
public class DatabaseHandler {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(DatabaseHandler.class));

    /** */
    private static final String IMPLEMENTATION_VERSION = "Implementation-Version";

    /** */
    private static final String BUNDLE_VERSION = "Bundle-Version";

    /** */
    private final File driversFolder;

    /** */
    private final DbMetadataReader dbMetaReader;

    /**
     * @param cfg Config.
     */
    public DatabaseHandler(AgentConfiguration cfg) {
        driversFolder = resolvePath(F.isEmpty(cfg.driversFolder()) ? "jdbc-drivers" : cfg.driversFolder());

        dbMetaReader = new DbMetadataReader();
    }

    /**
     * @param jdbcDriverJar File name of driver jar file.
     * @param jdbcDriverCls Optional JDBC driver class name.
     * @param jdbcDriverImplVer Optional JDBC driver version.
     * @return JSON for driver info.
     */
    private Map<String, String> driver(
        String jdbcDriverJar,
        String jdbcDriverCls,
        String jdbcDriverImplVer
    ) {
        Map<String, String> map = new LinkedHashMap<>();

        map.put("jdbcDriverJar", jdbcDriverJar);
        map.put("jdbcDriverClass", jdbcDriverCls);
        map.put("jdbcDriverImplVersion", jdbcDriverImplVer);

        return map;
    }

    /**
     * Collect list of JDBC drivers.
     *
     * @return List of JDBC drivers.
     */
    public List<Map<String, String>> collectJdbcDrivers() {
        List<Map<String, String>> drivers = new ArrayList<>();

        if (driversFolder != null) {
            log.info("Collecting JDBC drivers in folder: " + driversFolder.getPath());

            File[] list = driversFolder.listFiles((dir, name) -> name.endsWith(".jar"));

            if (list != null) {
                for (File file : list) {
                    try {
                        boolean win = System.getProperty("os.name").contains("win");

                        URL url = new URL("jar", null,
                            "file:" + (win ? "/" : "") + file.getPath() + "!/META-INF/services/java.sql.Driver");

                        try (
                            BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), UTF_8));
                            JarFile jar = new JarFile(file.getPath())
                        ) {
                            Manifest m = jar.getManifest();
                            Object ver = m.getMainAttributes().getValue(IMPLEMENTATION_VERSION);

                            if (ver == null)
                                ver = m.getMainAttributes().getValue(BUNDLE_VERSION);

                            String jdbcDriverCls = reader.readLine();

                            drivers.add(driver(file.getName(), jdbcDriverCls, ver != null ? ver.toString() : null));

                            log.info("Found: [driver=" + file + ", class=" + jdbcDriverCls + "]");
                        }
                    }
                    catch (IOException e) {
                        drivers.add(driver(file.getName(), null, null));

                        log.info("Found: [driver=" + file + "]");
                        log.error("Failed to detect driver class: " + e.getMessage());
                    }
                }
            }
            else
                throw new IllegalStateException("JDBC drivers folder has no files");
        }
        else
            throw new IllegalStateException("JDBC drivers folder not specified");

        return drivers;
    }

    /**
     * Collect DB schemas.
     *
     * @param evt Websocket event.
     * @return DB schemas.
     */
    public DbSchema collectDbSchemas(WebSocketRequest evt) throws SQLException {
        log.info("Collecting database schemas...");

        JsonObject args = fromJson(evt.getPayload());

        try (Connection conn = connect(args)) {
            String catalog = conn.getCatalog();

            if (catalog == null) {
                String jdbcUrl = args.getString("jdbcUrl", "");

                String[] parts = jdbcUrl.split("[/:=]");

                catalog = parts.length > 0 ? parts[parts.length - 1] : "NONE";
            }

            Collection<String> schemas = dbMetaReader.schemas(conn, args.getBoolean("importSamples", false));

            log.info("Collected database schemas:" + schemas.size());

            return new DbSchema(catalog, schemas);
        }
    }

    /**
     * Collect DB metadata.
     *
     * @param evt Websocket event.
     * @return DB metadata
     */
    @SuppressWarnings("unchecked")
    public Collection<DbTable> collectDbMetadata(WebSocketRequest evt) throws SQLException {
        log.info("Collecting database metadata...");

        JsonObject args = fromJson(evt.getPayload());

        if (!args.containsKey("schemas"))
            throw new IllegalArgumentException("Missing schemas in arguments: " + args);

        List<String> schemas = (List)args.get("schemas");

        if (!args.containsKey("tablesOnly"))
            throw new IllegalArgumentException("Missing tablesOnly in arguments: " + args);

        boolean tblsOnly = args.getBoolean("tablesOnly", false);

        try (Connection conn = connect(args)) {
            Collection<DbTable> metadata = dbMetaReader.metadata(conn, schemas, tblsOnly);

            log.info("Collected database metadata: " + metadata.size());

            return metadata;
        }
    }

    /**
     * @param args Connection arguments.
     * @return Connection to database.
     * @throws SQLException If failed to connect.
     */
    private Connection connect(JsonObject args) throws SQLException {
        String jdbcDriverJarPath = args.getString("jdbcDriverJar", "");

        if (F.isEmpty(jdbcDriverJarPath))
            throw new IllegalArgumentException("Path to JDBC driver not found in arguments");

        String jdbcDriverCls = args.getString("jdbcDriverClass", "");

        if (F.isEmpty(jdbcDriverCls))
            throw new IllegalArgumentException("JDBC driver class not found in arguments");

        String jdbcUrl = args.getString("jdbcUrl", "");

        if (F.isEmpty(jdbcUrl))
            throw new IllegalArgumentException("JDBC URL not found in arguments");

        if (!args.containsKey("info"))
            throw new IllegalArgumentException("Connection parameters not found in arguments");

        Properties jdbcInfo = new Properties();

        jdbcInfo.putAll((Map)args.get("info"));

        if (AgentMetadataDemo.isTestDriveUrl(jdbcUrl))
            return AgentMetadataDemo.testDrive();

        if (!new File(jdbcDriverJarPath).isAbsolute() && driversFolder != null)
            jdbcDriverJarPath = new File(driversFolder, jdbcDriverJarPath).getPath();

        log.info("Connecting to database[drvJar=" + jdbcDriverJarPath +
            ", drvCls=" + jdbcDriverCls + ", jdbcUrl=" + jdbcUrl + "]");

        return dbMetaReader.connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo);
    }
}
