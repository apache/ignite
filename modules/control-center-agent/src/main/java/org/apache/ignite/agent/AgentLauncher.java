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

import com.beust.jcommander.JCommander;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.agent.handlers.RestExecutor;
import org.apache.ignite.agent.testdrive.AgentMetadataTestDrive;
import org.apache.ignite.agent.testdrive.AgentSqlTestDrive;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import static org.apache.ignite.agent.AgentConfiguration.DFLT_SERVER_PORT;
import static org.apache.ignite.agent.AgentUtils.resolvePath;

/**
 * Control Center Agent launcher.
 */
public class AgentLauncher {
    /** */
    private static final Logger log = Logger.getLogger(AgentLauncher.class.getName());

    /** */
    private static final int RECONNECT_INTERVAL = 3000;

    /** Static initializer. */
    static {
        AgentLoggingConfigurator.configure();
    }

    /**
     * @param args Args.
     */
    @SuppressWarnings("BusyWait")
    public static void main(String[] args) throws Exception {
        log.log(Level.INFO, "Starting Apache Ignite Web Console Agent...");

        AgentConfiguration cfg = new AgentConfiguration();

        JCommander jCommander = new JCommander(cfg, args);

        String osName = System.getProperty("os.name").toLowerCase();

        jCommander.setProgramName("ignite-web-agent." + (osName.contains("win") ? "bat" : "sh"));

        String prop = cfg.configPath();

        AgentConfiguration propCfg = new AgentConfiguration();

        try {
            File f = resolvePath(prop);

            if (f == null)
                log.log(Level.WARNING, "Failed to find agent property file: '" + prop + "'");
            else
                propCfg.load(f.toURI().toURL());
        }
        catch (IOException ignore) {
            if (!AgentConfiguration.DFLT_CFG_PATH.equals(prop))
                log.log(Level.WARNING, "Failed to load agent property file: '" + prop + "'", ignore);
        }

        cfg.merge(propCfg);

        if (cfg.help()) {
            jCommander.usage();

            return;
        }

        System.out.println();
        System.out.println("Agent configuration:");
        System.out.println(cfg);
        System.out.println();

        if (cfg.testDriveSql() && cfg.nodeUri() != null)
            log.log(Level.WARNING,
                "URI for connect to Ignite REST server will be ignored because --test-drive-sql option was specified.");

        if (!cfg.testDriveSql() && !cfg.testDriveMetadata()) {
            System.out.println("To start web-agent in test-drive mode, pass \"-tm\" and \"-ts\" parameters");
            System.out.println();
        }

        if (cfg.token() == null) {
            String webHost;

            try {
                webHost = new URI(cfg.serverUri()).getHost();
            }
            catch (URISyntaxException e) {
                log.log(Level.SEVERE, "Failed to parse Ignite Web Console uri", e);

                return;
            }

            System.out.println("Security token is required to establish connection to the web console.");
            System.out.println(String.format("It is available on the Profile page: https://%s/profile", webHost));

            System.out.print("Enter security token: ");

            cfg.token(System.console().readLine().trim());
        }

        if (cfg.testDriveMetadata())
            AgentMetadataTestDrive.testDrive();

        if (cfg.testDriveSql())
            AgentSqlTestDrive.testDrive(cfg);

        RestExecutor restExecutor = new RestExecutor(cfg);

        restExecutor.start();

        try {
            SslContextFactory sslCtxFactory = new SslContextFactory();

            // Workaround for use self-signed certificate:
            if (Boolean.getBoolean("trust.all"))
                sslCtxFactory.setTrustAll(true);

            WebSocketClient client = new WebSocketClient(sslCtxFactory);

            client.setMaxIdleTimeout(Long.MAX_VALUE);

            client.start();

            try {
                while (!Thread.interrupted()) {
                    AgentSocket agentSock = new AgentSocket(cfg, restExecutor);

                    log.log(Level.INFO, "Connecting to: " + cfg.serverUri());

                    URI uri = URI.create(cfg.serverUri());

                    if (uri.getPort() == -1)
                        uri = URI.create(cfg.serverUri() + ":" + DFLT_SERVER_PORT);

                    client.connect(agentSock, uri);

                    agentSock.waitForClose();

                    Thread.sleep(RECONNECT_INTERVAL);
                }
            }
            finally {
                client.stop();
            }
        }
        finally {
            restExecutor.stop();
        }
    }
}
