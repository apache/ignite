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
import java.net.Authenticator;
import java.net.ConnectException;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManager;
import org.apache.ignite.console.agent.handlers.ClusterListener;
import org.apache.ignite.console.agent.handlers.DatabaseListener;
import org.apache.ignite.console.agent.handlers.RestListener;
import org.apache.ignite.console.agent.rest.RestExecutor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import static io.socket.client.Socket.EVENT_CONNECT;
import static io.socket.client.Socket.EVENT_CONNECT_ERROR;
import static io.socket.client.Socket.EVENT_DISCONNECT;
import static io.socket.client.Socket.EVENT_ERROR;
import static org.apache.ignite.console.agent.AgentUtils.fromJSON;
import static org.apache.ignite.console.agent.AgentUtils.toJSON;
import static org.apache.ignite.console.agent.AgentUtils.trustManager;

/**
 * Ignite Web Agent launcher.
 */
public class AgentLauncher {
    /** */
    private static final Logger log = LoggerFactory.getLogger(AgentLauncher.class);

    /** */
    private static final String EVENT_SCHEMA_IMPORT_DRIVERS = "schemaImport:drivers";

    /** */
    private static final String EVENT_SCHEMA_IMPORT_SCHEMAS = "schemaImport:schemas";

    /** */
    private static final String EVENT_SCHEMA_IMPORT_METADATA = "schemaImport:metadata";

    /** */
    private static final String EVENT_NODE_VISOR_TASK = "node:visorTask";

    /** */
    private static final String EVENT_NODE_REST = "node:rest";

    /** */
    private static final String EVENT_RESET_TOKEN = "agent:reset:token";

    /** */
    private static final String EVENT_LOG_WARNING = "log:warn";

    static {
        // Optionally remove existing handlers attached to j.u.l root logger.
        SLF4JBridgeHandler.removeHandlersForRootLogger();

        // Add SLF4JBridgeHandler to j.u.l's root logger.
        SLF4JBridgeHandler.install();
    }

    /**
     * On error listener.
     */
    private static final Emitter.Listener onError = args -> {
        Throwable e = (Throwable)args[0];

        ConnectException ce = X.cause(e, ConnectException.class);

        if (ce != null)
            log.error("Failed to establish connection to server (connection refused).");
        else {
            Exception ignore = X.cause(e, SSLHandshakeException.class);

            if (ignore != null) {
                log.error("Failed to establish SSL connection to server, due to errors with SSL handshake.");
                log.error("Add to environment variable JVM_OPTS parameter \"-Dtrust.all=true\" to skip certificate validation in case of using self-signed certificate.");

                System.exit(1);
            }

            ignore = X.cause(e, UnknownHostException.class);

            if (ignore != null) {
                log.error("Failed to establish connection to server, due to errors with DNS or missing proxy settings.");
                log.error("Documentation for proxy configuration can be found here: http://apacheignite.readme.io/docs/web-agent#section-proxy-configuration");

                System.exit(1);
            }

            ignore = X.cause(e, IOException.class);

            if (ignore != null && "404".equals(ignore.getMessage())) {
                log.error("Failed to receive response from server (connection refused).");

                return;
            }

            if (ignore != null && "407".equals(ignore.getMessage())) {
                log.error("Failed to establish connection to server, due to proxy requires authentication.");

                String userName = System.getProperty("https.proxyUsername", System.getProperty("http.proxyUsername"));

                if (userName == null || userName.trim().isEmpty())
                    userName = readLine("Enter proxy user name: ");
                else
                    System.out.println("Read username from system properties: " + userName);

                char[] pwd = readPassword("Enter proxy password: ");

                final PasswordAuthentication pwdAuth = new PasswordAuthentication(userName, pwd);

                Authenticator.setDefault(new Authenticator() {
                    @Override protected PasswordAuthentication getPasswordAuthentication() {
                        return pwdAuth;
                    }
                });

                return;
            }

            log.error("Connection error.", e);
        }
    };

    /**
     * On disconnect listener.
     */
    private static final Emitter.Listener onDisconnect = args -> log.error("Connection closed: {}", args);

    /**
     * On token reset listener.
     */
    private static final Emitter.Listener onLogWarning = args -> log.warn(String.valueOf(args[0]));

    /**
     * @param fmt Format string.
     * @param args Arguments.
     */
    private static String readLine(String fmt, Object... args) {
        if (System.console() != null)
            return System.console().readLine(fmt, args);

        System.out.print(String.format(fmt, args));

        return new Scanner(System.in).nextLine();
    }

    /**
     * @param fmt Format string.
     * @param args Arguments.
     */
    private static char[] readPassword(String fmt, Object... args) {
        if (System.console() != null)
            return System.console().readPassword(fmt, args);

        System.out.print(String.format(fmt, args));

        return new Scanner(System.in).nextLine().toCharArray();
    }

    /**
     * @param args Args.
     */
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
                log.warn("Failed to find agent property file: {}", prop);
            else
                propCfg.load(f.toURI().toURL());
        }
        catch (IOException e) {
            if (!AgentConfiguration.DFLT_CFG_PATH.equals(prop))
                log.warn("Failed to load agent property file: " + prop, e);
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

        URI uri;

        try {
            uri = new URI(cfg.serverUri());
        }
        catch (URISyntaxException e) {
            log.error("Failed to parse Ignite Web Console uri", e);

            return;
        }

        if (cfg.tokens() == null) {
            System.out.println("Security token is required to establish connection to the web console.");
            System.out.println(String.format("It is available on the Profile page: https://%s/profile", uri.getHost()));

            String tokens = String.valueOf(readPassword("Enter security tokens separated by comma: "));

            cfg.tokens(new ArrayList<>(Arrays.asList(tokens.trim().split(","))));
        }

        // Create proxy authenticator using passed properties.
        switch (uri.getScheme()) {
            case "http":
            case "https":
                final String username = System.getProperty(uri.getScheme() + ".proxyUsername");
                final char[] pwd = System.getProperty(uri.getScheme() + ".proxyPassword", "").toCharArray();

                Authenticator.setDefault(new Authenticator() {
                    @Override protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, pwd);
                    }
                });

                break;

            default:
                // No-op.
        }

        List<String> nodeURIs = cfg.nodeURIs();

        for (int i = nodeURIs.size() - 1; i >= 0; i--) {
            String nodeURI = nodeURIs.get(i);

            try {
                new URI(nodeURI);
            }
            catch (URISyntaxException ignored) {
                log.warn("Failed to parse Ignite node URI: {}.", nodeURI);

                nodeURIs.remove(i);
            }
        }

        if (nodeURIs.isEmpty()) {
            log.error("Failed to find valid URIs for connect to Ignite node via REST. Please check agent settings");

            return;
        }

        cfg.nodeURIs(nodeURIs);

        IO.Options opts = new IO.Options();

        opts.path = "/agents";

        // Workaround for use self-signed certificate
        if (Boolean.getBoolean("trust.all")) {
            SSLContext ctx = SSLContext.getInstance("TLS");

            // Create an SSLContext that uses our TrustManager
            ctx.init(null, new TrustManager[] {trustManager()}, null);

            opts.sslContext = ctx;
        }

        final Socket client = IO.socket(uri, opts);

        try (RestExecutor restExecutor = new RestExecutor();
             ClusterListener clusterLsnr = new ClusterListener(cfg, client, restExecutor)) {
            Emitter.Listener onConnect = connectRes -> {
                log.info("Connection established.");

                JSONObject authMsg = new JSONObject();

                try {
                    authMsg.put("tokens", toJSON(cfg.tokens()));
                    authMsg.put("disableDemo", cfg.disableDemo());

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

                    client.emit("agent:auth", authMsg, (Ack) authRes -> {
                        if (authRes != null) {
                            if (authRes[0] instanceof String) {
                                log.error((String)authRes[0]);

                                System.exit(1);
                            }

                            if (authRes[0] == null && authRes[1] instanceof JSONArray) {
                                try {
                                    List<String> activeTokens = fromJSON(authRes[1], List.class);

                                    if (!F.isEmpty(activeTokens)) {
                                        Collection<String> missedTokens = cfg.tokens();

                                        cfg.tokens(activeTokens);

                                        missedTokens.removeAll(activeTokens);

                                        if (!F.isEmpty(missedTokens)) {
                                            String tokens = F.concat(missedTokens, ", ");

                                            log.warn("Failed to authenticate with token(s): {}. " +
                                                "Please reload agent archive or check settings", tokens);
                                        }

                                        log.info("Authentication success.");

                                        clusterLsnr.watch();

                                        return;
                                    }
                                }
                                catch (Exception e) {
                                    log.error("Failed to authenticate agent. Please check agent\'s tokens", e);

                                    System.exit(1);
                                }
                            }
                        }

                        log.error("Failed to authenticate agent. Please check agent\'s tokens");

                        System.exit(1);
                    });
                }
                catch (JSONException | IOException e) {
                    log.error("Failed to construct authentication message", e);

                    client.close();
                }
            };

            DatabaseListener dbHnd = new DatabaseListener(cfg);
            RestListener restHnd = new RestListener(cfg, restExecutor);

            final CountDownLatch latch = new CountDownLatch(1);

            log.info("Connecting to: {}", cfg.serverUri());

            client
                .on(EVENT_CONNECT, onConnect)
                .on(EVENT_CONNECT_ERROR, onError)
                .on(EVENT_ERROR, onError)
                .on(EVENT_DISCONNECT, onDisconnect)
                .on(EVENT_LOG_WARNING, onLogWarning)
                .on(EVENT_RESET_TOKEN, res -> {
                    String tok = String.valueOf(res[0]);

                    log.warn("Security token has been reset: {}", tok);

                    cfg.tokens().remove(tok);

                    if (cfg.tokens().isEmpty()) {
                        client.off();

                        latch.countDown();
                    }
                })
                .on(EVENT_SCHEMA_IMPORT_DRIVERS, dbHnd.availableDriversListener())
                .on(EVENT_SCHEMA_IMPORT_SCHEMAS, dbHnd.schemasListener())
                .on(EVENT_SCHEMA_IMPORT_METADATA, dbHnd.metadataListener())
                .on(EVENT_NODE_VISOR_TASK, restHnd)
                .on(EVENT_NODE_REST, restHnd);

            client.connect();

            latch.await();
        }
        finally {
            client.close();
        }
    }
}
