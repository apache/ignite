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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import io.socket.client.Ack;
import io.socket.client.IO;
import io.socket.client.Socket;
import io.socket.emitter.Emitter;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.console.agent.handlers.DatabaseHandler;
import org.apache.ignite.console.agent.handlers.RestHandler;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import static io.socket.client.Socket.EVENT_CONNECT;
import static io.socket.client.Socket.EVENT_CONNECTING;
import static io.socket.client.Socket.EVENT_CONNECT_ERROR;
import static io.socket.client.Socket.EVENT_DISCONNECT;
import static io.socket.client.Socket.EVENT_ERROR;
import static io.socket.client.Socket.EVENT_RECONNECTING;
import static org.apache.ignite.console.agent.AgentConfiguration.DFLT_SERVER_PORT;

/**
 * Control Center Agent launcher.
 */
public class AgentLauncher {
    /** */
    private static final Logger log = Logger.getLogger(AgentLauncher.class.getName());

    /** */
    private static final String EVENT_NODE_REST = "node:rest";

    /** */
    private static final String EVENT_SCHEMA_IMPORT_DRIVERS = "schemaImport:drivers";

    /** */
    private static final String EVENT_SCHEMA_IMPORT_SCHEMAS = "schemaImport:schemas";

    /** */
    private static final String EVENT_SCHEMA_IMPORT_METADATA = "schemaImport:metadata";

    /** */
    private static final String EVENT_AGENT_WARNING = "agent:warning";

    /** */
    private static final String EVENT_AGENT_CLOSE = "agent:close";

    /** */
    private static final int RECONNECT_INTERVAL = 3000;

    /**
     * Create a trust manager that trusts all certificates It is not using a particular keyStore
     */
    private static TrustManager[] getTrustManagers() {
        return new TrustManager[] {
            new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(
                    java.security.cert.X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(
                    java.security.cert.X509Certificate[] certs, String authType) {
                }
            }};
    }

    /**
     * On error listener.
     */
    private static final Emitter.Listener onError = new Emitter.Listener() {
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        @Override public void call(Object... args) {
            Throwable e = (Throwable)args[0];

            ConnectException ce = X.cause(e, ConnectException.class);

            if (ce != null)
                log.error("Failed to receive response from server (connection refused).");
            else {
                Exception ignore = X.cause(e, SSLHandshakeException.class);

                if (ignore != null) {
                    log.error("Failed to establish SSL connection to server, due to errors with SSL handshake.");
                    log.error("Add to environment variable JVM_OPTS parameter \"-Dtrust.all=true\" to skip certificate validation in case of using self-signed certificate.");

                    System.exit(1);
                }

                ignore = X.cause(e, IOException.class);

                if (ignore != null && "404".equals(ignore.getMessage())) {
                    log.error("Failed to receive response from server (connection refused).");

                    return;
                }

                log.error("Connection error.", e);
            }
        }
    };

    /**
     * On disconnect listener.
     */
    private static final Emitter.Listener onDisconnect = new Emitter.Listener() {
        @Override public void call(Object... args) {
            log.error(String.format("Connection closed: %s.", args));
        }
    };

    /**
     * @param args Args.
     */
    @SuppressWarnings("BusyWait")
    public static void main(String[] args) throws Exception {
        log.info("Starting Apache Ignite Web Console Agent...");

        final AgentConfiguration cfg = new AgentConfiguration();

        JCommander jCommander = new JCommander(cfg);

        String osName = System.getProperty("os.name").toLowerCase();

        jCommander.setProgramName("ignite-web-agent." + (osName.contains("win") ? "bat" : "sh"));

        try {
            jCommander.parse(args);
        }
        catch (ParameterException pe) {
            log.error("Failed to parse command line parameters: " + Arrays.toString(args), pe);

            jCommander.usage();

            return;
        }

        String prop = cfg.configPath();

        AgentConfiguration propCfg = new AgentConfiguration();

        try {
            File f = AgentUtils.resolvePath(prop);

            if (f == null)
                log.warn("Failed to find agent property file: " + prop);
            else
                propCfg.load(f.toURI().toURL());
        }
        catch (IOException ignore) {
            if (!AgentConfiguration.DFLT_CFG_PATH.equals(prop))
                log.warn("Failed to load agent property file: " + prop, ignore);
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

        if (cfg.tokens() == null) {
            String webHost;

            try {
                webHost = new URI(cfg.serverUri()).getHost();
            }
            catch (URISyntaxException e) {
                log.error("Failed to parse Ignite Web Console uri", e);

                return;
            }

            System.out.println("Security token is required to establish connection to the web console.");
            System.out.println(String.format("It is available on the Profile page: https://%s/profile", webHost));

            System.out.print("Enter security tokens separated by comma: ");

            cfg.tokens(Arrays.asList(System.console().readLine().trim().split(",")));
        }

        final RestHandler restHnd = new RestHandler(cfg);

        try {
            restHnd.start();

            URI uri = URI.create(cfg.serverUri());

            if (uri.getPort() == -1)
                uri = URI.create(cfg.serverUri() + ':' + DFLT_SERVER_PORT);

            IO.Options opts = new IO.Options();

            opts.reconnectionDelay = RECONNECT_INTERVAL;

            // Workaround for use self-signed certificate
            if (Boolean.getBoolean("trust.all")) {
                SSLContext ctx = SSLContext.getInstance("TLS");

                // Create an SSLContext that uses our TrustManager
                ctx.init(null, getTrustManagers(), null);

                opts.sslContext = ctx;
            }

            final Socket client = IO.socket(uri, opts);

            try {
                Emitter.Listener onConnecting = new Emitter.Listener() {
                    @Override public void call(Object... args) {
                        log.info("Connecting to: " + cfg.serverUri());
                    }
                };

                Emitter.Listener onConnect = new Emitter.Listener() {
                    @Override public void call(Object... args) {
                        log.info("Connection established.");

                        JSONObject authMsg = new JSONObject();

                        try {
                            authMsg.put("tokens", cfg.tokens());

                            String clsName = AgentLauncher.class.getSimpleName() + ".class";

                            String clsPath = AgentLauncher.class.getResource(clsName).toString();

                            if (clsPath.startsWith("jar")) {
                                String manifestPath = clsPath.substring(0, clsPath.lastIndexOf('!') + 1) +
                                    "/META-INF/MANIFEST.MF";

                                Manifest manifest = new Manifest(new URL(manifestPath).openStream());

                                Attributes attr = manifest.getMainAttributes();

                                authMsg.put("ver", attr.getValue("Implementation-Version"));
                                authMsg.put("bt", attr.getValue("Build-Time"));
                            }

                            client.emit("agent:auth", authMsg, new Ack() {
                                @Override public void call(Object... args) {
                                    // Authentication failed if response contains args.
                                    if (args != null && args.length > 0) {
                                        onDisconnect.call(args);

                                        System.exit(1);
                                    }

                                    log.info("Authentication success.");
                                }
                            });
                        }
                        catch (JSONException | IOException e) {
                            log.error("Failed to construct authentication message", e);

                            client.close();
                        }
                    }
                };

                DatabaseHandler dbHnd = new DatabaseHandler(cfg);

                final CountDownLatch latch = new CountDownLatch(1);

                client
                    .on(EVENT_CONNECTING, onConnecting)
                    .on(EVENT_CONNECT, onConnect)
                    .on(EVENT_CONNECT_ERROR, onError)
                    .on(EVENT_RECONNECTING, onConnecting)
                    .on(EVENT_NODE_REST, restHnd)
                    .on(EVENT_SCHEMA_IMPORT_DRIVERS, dbHnd.availableDriversListener())
                    .on(EVENT_SCHEMA_IMPORT_SCHEMAS, dbHnd.schemasListener())
                    .on(EVENT_SCHEMA_IMPORT_METADATA, dbHnd.metadataListener())
                    .on(EVENT_ERROR, onError)
                    .on(EVENT_DISCONNECT, onDisconnect)
                    .on(EVENT_AGENT_WARNING, new Emitter.Listener() {
                        @Override public void call(Object... args) {
                            log.warn(args[0]);
                        }
                    })
                    .on(EVENT_AGENT_CLOSE, new Emitter.Listener() {
                        @Override public void call(Object... args) {
                            onDisconnect.call(args);

                            client.off();

                            latch.countDown();
                        }
                    });

                client.connect();

                latch.await();
            }
            finally {
                client.close();
            }
        }
        finally {
            restHnd.stop();
        }
    }
}
