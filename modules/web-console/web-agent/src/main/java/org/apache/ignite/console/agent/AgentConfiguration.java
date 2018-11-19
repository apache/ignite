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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.ignite.internal.util.typedef.F;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Agent configuration.
 */
public class AgentConfiguration {
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
    @Parameter(names = {"-n", "--node-uri"},
        description = "Comma-separated URIs for connect to Ignite node via REST" +
            "                        " +
            "      Default value: " + DFLT_NODE_URI)
    private List<String> nodeURIs;

    /** */
    @Parameter(names = {"-nl", "--node-login"},
        description = "User name that will be used to connect to secured cluster")
    private String nodeLogin;

    /** */
    @Parameter(names = {"-np", "--node-password"},
        description = "Password that will be used to connect to secured cluster")
    private String nodePwd;

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
    @Parameter(names = {"-dd", "--disable-demo"}, description = "Disable demo mode on this agent " +
        "                             " +
        "      Default value: false")
    private Boolean disableDemo;

    /** */
    @Parameter(names = {"-nks", "--node-key-store"},
        description = "Path to key store that will be used to connect to cluster")
    private String nodeKeyStore;

    /** */
    @Parameter(names = {"-nkp", "--node-key-store-password"},
        description = "Optional password for node key store")
    private String nodeKeyStorePass;

    /** */
    @Parameter(names = {"-nts", "--node-trust-store"},
        description = "Path to trust store that will be used to connect to cluster")
    private String nodeTrustStore;

    /** */
    @Parameter(names = {"-ntp", "--node-trust-store-password"},
        description = "Password for node trust store")
    private String nodeTrustStorePass;

    /** */
    @Parameter(names = {"-sks", "--server-key-store"},
        description = "Path to key store that will be used to connect to Web server")
    private String srvKeyStore;

    /** */
    @Parameter(names = {"-skp", "--node-key-store-password"},
        description = "Optional password for server key store")
    private String srvKeyStorePass;

    /** */
    @Parameter(names = {"-sts", "--server-trust-store"},
        description = "Path to trust store that will be used to connect to Web server")
    private String srvTrustStore;

    /** */
    @Parameter(names = {"-stp", "--server-trust-store-password"},
        description = "Password for server trust store")
    private String srvTrustStorePass;

    /** */
    @Parameter(names = {"-h", "--help"}, help = true, description = "Print this help message")
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
     * @return Node URIs.
     */
    public List<String> nodeURIs() {
        return nodeURIs;
    }

    /**
     * @param nodeURIs Node URIs.
     */
    public void nodeURIs(List<String> nodeURIs) {
        this.nodeURIs = nodeURIs;
    }

    /**
     * @return User name for agent to authenticate on node.
     */
    public String nodeLogin() {
        return nodeLogin;
    }

    /**
     * @param nodeLogin User name for agent to authenticate on node.
     */
    public void nodeLogin(String nodeLogin) {
        this.nodeLogin = nodeLogin;
    }

    /**
     * @return Agent password to authenticate on node.
     */
    public String nodePassword() {
        return nodePwd;
    }

    /**
     * @param nodePwd Agent password to authenticate on node.
     */
    public void nodePassword(String nodePwd) {
        this.nodePwd = nodePwd;
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
     * @return Disable demo mode.
     */
    public Boolean disableDemo() {
        return disableDemo != null ? disableDemo : Boolean.FALSE;
    }

    /**
     * @param disableDemo Disable demo mode.
     */
    public void disableDemo(Boolean disableDemo) {
        this.disableDemo = disableDemo;
    }

    /**
     * @return Path to node key store.
     */
    public String nodeKeyStore() {
        return nodeKeyStore;
    }

    /**
     * @param nodeKeyStore Path to node key store.
     */
    public void nodeKeyStore(String nodeKeyStore) {
        this.nodeKeyStore = nodeKeyStore;
    }

    /**
     * @return Node key store password.
     */
    public String nodeKeyStorePassword() {
        return nodeKeyStorePass;
    }

    /**
     * @param nodeKeyStorePass Node key store password.
     */
    public void nodeKeyStorePassword(String nodeKeyStorePass) {
        this.nodeKeyStorePass = nodeKeyStorePass;
    }

    /**
     * @return Path to node trust store.
     */
    public String nodeTrustStore() {
        return nodeTrustStore;
    }

    /**
     * @param nodeTrustStore Path to node trust store.
     */
    public void nodeTrustStore(String nodeTrustStore) {
        this.nodeTrustStore = nodeTrustStore;
    }

    /**
     * @return Node trust store password.
     */
    public String nodeTrustStorePassword() {
        return nodeTrustStorePass;
    }

    /**
     * @param nodeTrustStorePass Node trust store password.
     */
    public void nodeTrustStorePassword(String nodeTrustStorePass) {
        this.nodeTrustStorePass = nodeTrustStorePass;
    }

    /**
     * @return Path to server key store.
     */
    public String serverKeyStore() {
        return srvKeyStore;
    }

    /**
     * @param srvKeyStore Path to server key store.
     */
    public void serverKeyStore(String srvKeyStore) {
        this.srvKeyStore = srvKeyStore;
    }

    /**
     * @return Server key store password.
     */
    public String serverKeyStorePassword() {
        return srvKeyStorePass;
    }

    /**
     * @param srvKeyStorePass Server key store password.
     */
    public void serverKeyStorePassword(String srvKeyStorePass) {
        this.srvKeyStorePass = srvKeyStorePass;
    }

    /**
     * @return Path to server trust store.
     */
    public String serverTrustStore() {
        return srvTrustStore;
    }

    /**
     * @param srvTrustStore Path to server trust store.
     */
    public void serverTrustStore(String srvTrustStore) {
        this.srvTrustStore = srvTrustStore;
    }

    /**
     * @return Server trust store password.
     */
    public String serverTrustStorePassword() {
        return srvTrustStorePass;
    }

    /**
     * @param srvTrustStorePass Server trust store password.
     */
    public void serverTrustStorePassword(String srvTrustStorePass) {
        this.srvTrustStorePass = srvTrustStorePass;
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

        try (Reader reader = new InputStreamReader(cfgUrl.openStream(), UTF_8)) {
            props.load(reader);
        }

        String val = (String)props.remove("tokens");

        if (val != null)
            tokens(new ArrayList<>(Arrays.asList(val.split(","))));

        val = (String)props.remove("server-uri");

        if (val != null)
            serverUri(val);

        val = (String)props.remove("node-uri");

        if (val != null)
            nodeURIs(new ArrayList<>(Arrays.asList(val.split(","))));

        val = (String)props.remove("node-login");

        if (val != null)
            nodeLogin(val);

        val = (String)props.remove("node-password");

        if (val != null)
            nodePassword(val);

        val = (String)props.remove("driver-folder");

        if (val != null)
            driversFolder(val);
    }

    /**
     * @param cfg Config to merge with.
     */
    public void merge(AgentConfiguration cfg) {
        if (tokens == null)
            tokens(cfg.tokens());

        if (srvUri == null)
            serverUri(cfg.serverUri());

        if (srvUri == null)
            serverUri(DFLT_SERVER_URI);

        if (nodeURIs == null)
            nodeURIs(cfg.nodeURIs());

        if (nodeURIs == null)
            nodeURIs(Collections.singletonList(DFLT_NODE_URI));

        if (nodeLogin == null)
            nodeLogin(cfg.nodeLogin());

        if (nodePwd == null)
            nodePassword(cfg.nodePassword());

        if (driversFolder == null)
            driversFolder(cfg.driversFolder());

        if (disableDemo == null)
            disableDemo(cfg.disableDemo());

        if (nodeKeyStore == null)
            nodeKeyStore(cfg.nodeKeyStore());

        if (nodeKeyStorePass == null)
            nodeKeyStorePassword(cfg.nodeKeyStorePassword());

        if (nodeTrustStore == null)
            nodeTrustStore(cfg.nodeTrustStore());

        if (nodeTrustStorePass == null)
            nodeTrustStorePassword(cfg.nodeTrustStorePassword());

        if (srvKeyStore == null)
            serverKeyStore(cfg.serverKeyStore());

        if (srvKeyStorePass == null)
            serverKeyStorePassword(cfg.serverKeyStorePassword());

        if (srvTrustStore == null)
            serverTrustStore(cfg.serverTrustStore());

        if (srvTrustStorePass == null)
            serverTrustStorePassword(cfg.serverTrustStorePassword());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        if (!F.isEmpty(tokens)) {
            sb.append("User's security tokens          : ");

            boolean first = true;

            for (String tok : tokens) {
                if (first)
                    first = false;
                else
                    sb.append(',');

                if (tok.length() > 4) {
                    sb.append(new String(new char[tok.length() - 4]).replace('\0', '*'));

                    sb.append(tok.substring(tok.length() - 4));
                }
                else
                    sb.append(new String(new char[tok.length()]).replace('\0', '*'));
            }

            sb.append('\n');
        }

        sb.append("URI to Ignite node REST server  : ")
            .append(nodeURIs == null ? DFLT_NODE_URI : String.join(", ", nodeURIs)).append('\n');

        if (nodeLogin != null)
            sb.append("Login to Ignite node REST server: ").append(nodeLogin).append('\n');

        sb.append("URI to Ignite Console server    : ").append(srvUri == null ? DFLT_SERVER_URI : srvUri).append('\n');
        sb.append("Path to agent property file     : ").append(configPath()).append('\n');

        String drvFld = driversFolder();

        if (drvFld == null) {
            File agentHome = AgentUtils.getAgentHome();

            if (agentHome != null)
                drvFld = new File(agentHome, "jdbc-drivers").getPath();
        }

        sb.append("Path to JDBC drivers folder     : ").append(drvFld).append('\n');
        sb.append("Demo mode                       : ").append(disableDemo() ? "disabled" : "enabled");

        return sb.toString();
    }
}
