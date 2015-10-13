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

package org.apache.ignite.agent;

import com.beust.jcommander.Parameter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Properties;

/**
 * Agent configuration.
 */
public class AgentConfiguration {
    /** Default server port. */
    public static final int DFLT_SERVER_PORT = 3001;
    /** Default Ignite node HTTP port. */
    public static final int DFLT_NODE_PORT = 8080;
    /** Default server URI. */
    private static final String DFLT_SERVER_URI = "wss://localhost:3001";
    /** Default Ignite node HTTP URI. */
    private static final String DFLT_NODE_URI = "http://localhost:8080";
    /** */
    @Parameter(names = {"-t", "--token"}, description = "User's security token used to establish connection to Ignite Console.")
    private String tok;

    /** */
    @Parameter(names = {"-s", "--server-uri"}, description = "URI for connect to Ignite Console via web-socket protocol" +
        "           " +
        "      Default value: wss://localhost:3001")
    private String srvUri;

    /** */
    @Parameter(names = {"-n", "--node-uri"}, description = "URI for connect to Ignite node REST server" +
        "                        " +
        "      Default value: http://localhost:8080")
    private String nodeUri;

    /** */
    @Parameter(names = {"-c", "--config"}, description = "Path to agent property file" +
        "                                  " +
        "      Default value: ./default.properties")
    private String cfgPath;

    /** */
    @Parameter(names = {"-d", "--driver-folder"}, description = "Path to folder with JDBC drivers" +
        "                             " +
        "      Default value: ./jdbc-drivers")
    private String driversFolder;

    /** */
    @Parameter(names = { "-tm", "--test-drive-metadata" },
        description = "Start H2 database with sample tables in same process. " +
            "JDBC URL for connecting to sample database: jdbc:h2:mem:test-drive-db")
    private Boolean meta;

    /** */
    @Parameter(names = { "-ts", "--test-drive-sql" },
        description = "Create cache and populate it with sample data for use in query")
    private Boolean sql;

    /** */
    @Parameter(names = { "-h", "--help" }, help = true, description = "Print this help message")
    private Boolean help;

    /**
     * @return Token.
     */
    public String token() {
        return tok;
    }

    /**
     * @param tok Token.
     */
    public void token(String tok) {
        this.tok = tok;
    }

    /**
     * @return Server URI.
     */
    public String serverUri() {
        return srvUri;
    }

    /**
     * @param srvUri URI.
     */
    public void serverUri(String srvUri) {
        this.srvUri = srvUri;
    }

    /**
     * @return Node URI.
     */
    public String nodeUri() {
        return nodeUri;
    }

    /**
     * @param nodeUri Node URI.
     */
    public void nodeUri(String nodeUri) {
        this.nodeUri = nodeUri;
    }

    /**
     * @return Configuration path.
     */
    public String configPath() {
        return cfgPath == null ? "./default.properties" : cfgPath;
    }

    /**
     * @return Configured drivers folder.
     */
    public String driversFolder() {
        return driversFolder;
    }

    /**
     * @param driversFolder Driver folder.
     */
    public void driversFolder(String driversFolder) {
        this.driversFolder = driversFolder;
    }

    /**
     * @return {@code true} If metadata test drive should be started.
     */
    public Boolean testDriveMetadata() {
        return meta != null ? meta : false;
    }

    /**
     * @param meta Set to {@code true} if metadata test drive should be started.
     */
    public void testDriveMetadata(Boolean meta) {
        this.meta = meta;
    }

    /**
     * @return {@code true} If SQL test drive should be started.
     */
    public Boolean testDriveSql() {
        return sql != null ? sql : false;
    }

    /**
     * @param sql Set to {@code true} if SQL test drive should be started.
     */
    public void testDriveSql(Boolean sql) {
        this.sql = sql;
    }

    /**
     * @return {@code true} If agent options usage should be printed.
     */
    public Boolean help() {
        return help != null ? help : false;
    }

    /**
     * @param cfgUrl URL.
     */
    public void load(URL cfgUrl) throws IOException {
        Properties props = new Properties();

        try (Reader reader = new InputStreamReader(cfgUrl.openStream())) {
            props.load(reader);
        }

        String val = (String)props.remove("token");

        if (val != null)
            token(val);

        val = (String)props.remove("serverURI");

        if (val != null)
            serverUri(val);

        val = (String)props.remove("nodeURI");

        if (val != null)
            nodeUri(val);

        val = (String)props.remove("driverFolder");

        if (val != null)
            driversFolder(val);

        val = (String)props.remove("test-drive-metadata");

        if (val != null)
            testDriveMetadata(Boolean.valueOf(val));

        val = (String)props.remove("test-drive-sql");

        if (val != null)
            testDriveSql(Boolean.valueOf(val));
    }

    /**
     * @param cmd Command.
     */
    public void merge(AgentConfiguration cmd) {
        if (tok == null)
            token(cmd.token());

        if (srvUri == null)
            serverUri(cmd.serverUri());

        if (srvUri == null)
            serverUri(DFLT_SERVER_URI);

        if (nodeUri == null)
            nodeUri(cmd.nodeUri());

        if (nodeUri == null)
            nodeUri(DFLT_NODE_URI);

        if (driversFolder == null)
            driversFolder(cmd.driversFolder());

        if (testDriveMetadata())
            testDriveMetadata(true);

        if (testDriveSql())
            testDriveSql(true);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        if (tok != null)
            sb.append("User's security token         : ").append(token()).append('\n');

        sb.append("URI to Ignite node REST server: ").append(nodeUri == null ? DFLT_NODE_URI : nodeUri).append('\n');
        sb.append("URI to Ignite Console server  : ").append(srvUri == null ? DFLT_SERVER_URI : srvUri).append('\n');
        sb.append("Path to agent property file   : ").append(configPath()).append('\n');

        String drvFld = driversFolder();

        if (drvFld == null) {
            File agentHome = AgentUtils.getAgentHome();

            if (agentHome != null)
                drvFld = new File(agentHome, "jdbc-drivers").getPath();
        }

        sb.append("Path to JDBC drivers folder   : ").append(drvFld).append('\n');

        sb.append("Test-drive for load metadata  : ").append(testDriveMetadata()).append('\n');
        sb.append("Test-drive for execute query  : ").append(testDriveSql());

        return sb.toString();
    }
}
