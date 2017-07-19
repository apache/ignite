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

package org.apache.ignite.console.agent;

import com.beust.jcommander.Parameter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Agent configuration.
 */
public class AgentConfiguration {
    /** Default Ignite node HTTP port. */
    public static final int DFLT_NODE_PORT = 8080;

    /** Default path to agent property file. */
    public static final String DFLT_CFG_PATH = "default.properties";

    /** Default server URI. */
    private static final String DFLT_SERVER_URI = "http://localhost:3000";

    /** Default Ignite node HTTP URI. */
    private static final String DFLT_NODE_URI = "http://localhost:8080";

    /** */
    @Parameter(names = {"-t", "--tokens"},
        description = "User's tokens separated by comma used to connect to Ignite Console.")
    private List<String> tokens;

    /** */
    @Parameter(names = {"-s", "--server-uri"},
        description = "URI for connect to Ignite Console via web-socket protocol" +
        "           " +
        "      Default value: " + DFLT_SERVER_URI)
    private String srvUri;

    /** */
    @Parameter(names = {"-n", "--node-uri"}, description = "URI for connect to Ignite node REST server" +
        "                        " +
        "      Default value: " + DFLT_NODE_URI)
    private String nodeUri;

    /** URI for connect to Ignite demo node REST server */
    private String demoNodeUri;

    /** */
    @Parameter(names = {"-c", "--config"}, description = "Path to agent property file" +
        "                                  " +
        "      Default value: " + DFLT_CFG_PATH)
    private String cfgPath;

    /** */
    @Parameter(names = {"-d", "--driver-folder"}, description = "Path to folder with JDBC drivers" +
        "                             " +
        "      Default value: ./jdbc-drivers")
    private String driversFolder;

    /** */
    @Parameter(names = { "-h", "--help" }, help = true, description = "Print this help message")
    private Boolean help;

    /**
     * @return Tokens.
     */
    public List<String> tokens() {
        return tokens;
    }

    /**
     * @param tokens Tokens.
     */
    public void tokens(List<String> tokens) {
        this.tokens = tokens;
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
     * @return Demo node URI.
     */
    public String demoNodeUri() {
        return demoNodeUri;
    }

    /**
     * @param demoNodeUri Demo node URI.
     */
    public void demoNodeUri(String demoNodeUri) {
        this.demoNodeUri = demoNodeUri;
    }

    /**
     * @return Configuration path.
     */
    public String configPath() {
        return cfgPath == null ? DFLT_CFG_PATH : cfgPath;
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
     * @return {@code true} If agent options usage should be printed.
     */
    public Boolean help() {
        return help != null ? help : Boolean.FALSE;
    }

    /**
     * @param cfgUrl URL.
     */
    public void load(URL cfgUrl) throws IOException {
        Properties props = new Properties();

        try (Reader reader = new InputStreamReader(cfgUrl.openStream())) {
            props.load(reader);
        }

        String val = (String)props.remove("tokens");

        if (val != null)
            tokens(Arrays.asList(val.split(",")));

        val = (String)props.remove("server-uri");

        if (val != null)
            serverUri(val);

        val = (String)props.remove("node-uri");

        if (val != null)
            nodeUri(val);

        val = (String)props.remove("driver-folder");

        if (val != null)
            driversFolder(val);
    }

    /**
     * @param cmd Command.
     */
    public void merge(AgentConfiguration cmd) {
        if (tokens == null)
            tokens(cmd.tokens());

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
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        if (tokens != null && tokens.size() > 0) {
            sb.append("User's security tokens        : ");

            boolean first = true;

            for (String tok : tokens) {
                if (first)
                    first = false;
                else
                    sb.append(",");

                if (tok.length() > 4) {
                    sb.append(new String(new char[tok.length() - 4]).replace('\0', '*'));

                    sb.append(tok.substring(tok.length() - 4));
                }
                else
                    sb.append(new String(new char[tok.length()]).replace('\0', '*'));
            }

            sb.append('\n');
        }

        sb.append("URI to Ignite node REST server: ").append(nodeUri == null ? DFLT_NODE_URI : nodeUri).append('\n');
        sb.append("URI to Ignite Console server  : ").append(srvUri == null ? DFLT_SERVER_URI : srvUri).append('\n');
        sb.append("Path to agent property file   : ").append(configPath()).append('\n');

        String drvFld = driversFolder();

        if (drvFld == null) {
            File agentHome = AgentUtils.getAgentHome();

            if (agentHome != null)
                drvFld = new File(agentHome, "jdbc-drivers").getPath();
        }

        sb.append("Path to JDBC drivers folder   : ").append(drvFld);

        return sb.toString();
    }
}
