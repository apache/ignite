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

import java.net.ConnectException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.AgentUtils;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.demo.AgentClusterDemo;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.websocket.AgentHandshakeRequest;
import org.apache.ignite.console.websocket.AgentHandshakeResponse;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.console.websocket.WebSocketResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.UpgradeException;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketFrame;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.api.extensions.Frame;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.LoggerFactory;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.agent.AgentUtils.configureProxy;
import static org.apache.ignite.console.agent.AgentUtils.entriesToMap;
import static org.apache.ignite.console.agent.AgentUtils.entry;
import static org.apache.ignite.console.agent.AgentUtils.secured;
import static org.apache.ignite.console.agent.AgentUtils.sslContextFactory;
import static org.apache.ignite.console.agent.handlers.DemoClusterHandler.DEMO_CLUSTER_ID;
import static org.apache.ignite.console.utils.Utils.extractErrorMessage;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.websocket.AgentHandshakeRequest.CURRENT_VER;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENTS_PATH;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_HANDSHAKE;
import static org.apache.ignite.console.websocket.WebSocketEvents.AGENT_REVOKE_TOKEN;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_REST;
import static org.apache.ignite.console.websocket.WebSocketEvents.NODE_VISOR;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_DRIVERS;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_METADATA;
import static org.apache.ignite.console.websocket.WebSocketEvents.SCHEMA_IMPORT_SCHEMAS;
import static org.eclipse.jetty.websocket.api.StatusCode.SERVER_ERROR;

/**
 * Router that listen for web socket and redirect messages to event bus.
 */
@WebSocket(maxTextMessageSize = 10 * 1024 * 1024, maxBinaryMessageSize = 10 * 1024 * 1024)
public class WebSocketRouter implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(WebSocketRouter.class));

    /** Pong message. */
    private static final ByteBuffer PONG_MSG = UTF_8.encode("PONG");

    /** Default error messages. */
    private static final Map<String, String> ERROR_MSGS = Collections.unmodifiableMap(Stream.of(
        entry(SCHEMA_IMPORT_DRIVERS, "Failed to collect list of JDBC drivers"),
        entry(SCHEMA_IMPORT_SCHEMAS, "Failed to collect database schemas"),
        entry(SCHEMA_IMPORT_METADATA, "Failed to collect database metadata"),
        entry(NODE_REST, "Failed to handle REST request"),
        entry(NODE_VISOR, "Failed to handle Visor task request")).
        collect(entriesToMap()));

    /** Close agent latch. */
    private final CountDownLatch closeLatch = new CountDownLatch(1);

    /** Agent configuration. */
    private final AgentConfiguration cfg;

    /** Websocket Client. */
    private WebSocketClient client;

    /** Schema import handler. */
    private final DatabaseHandler dbHnd;

    /** Cluster handler. */
    private final ClusterHandler clusterHnd;

    /** Demo cluster handler. */
    private final DemoClusterHandler demoClusterHnd;

    /** Cluster watcher. */
    private final ClustersWatcher watcher;

    /** Reconnect count. */
    private int reconnectCnt = -1;

    /** Active tokens after handshake. */
    private List<String> validTokens;

    /** Connector pool. */
    private ExecutorService connectorPool = Executors.newSingleThreadExecutor(r -> new Thread(r, "Connect thread"));

    /**
     * @param cfg Configuration.
     */
    public WebSocketRouter(AgentConfiguration cfg) {
        this.cfg = cfg;

        dbHnd = new DatabaseHandler(cfg);
        clusterHnd = new ClusterHandler(cfg);
        demoClusterHnd = new DemoClusterHandler(cfg);

        watcher = new ClustersWatcher(cfg, clusterHnd, demoClusterHnd);
    }

    /**
     * @param cfg Config.
     */
    private static SslContextFactory createServerSslFactory(AgentConfiguration cfg) {
        boolean trustAll = Boolean.getBoolean("trust.all");

        if (trustAll && !F.isEmpty(cfg.serverTrustStore())) {
            log.warning("Options contains both '--server-trust-store' and '-Dtrust.all=true'. " +
                "Option '-Dtrust.all=true' will be ignored on connect to Web server.");

            trustAll = false;
        }

        boolean ssl = trustAll || !F.isEmpty(cfg.serverTrustStore()) || !F.isEmpty(cfg.serverKeyStore());

        if (!ssl)
            return null;

        return sslContextFactory(
            cfg.serverKeyStore(),
            cfg.serverKeyStorePassword(),
            trustAll,
            cfg.serverTrustStore(),
            cfg.serverTrustStorePassword(),
            cfg.cipherSuites()
        );
    }

    /**
     * Start websocket client.
     */
    public void start() {
        log.info("Starting Web Console Agent...");

        Runtime.getRuntime().addShutdownHook(new Thread(closeLatch::countDown));
        
        connect();
    }

    /**
     * Stop websocket client.
     */
    private void stopClient() {
        LT.clear();

        watcher.stop();

        if (client != null) {
            try {
                client.stop();
                client.destroy();
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        log.info("Stopping Web Console Agent...");

        stopClient();

        watcher.close();
    }

    /**
     * Connect to backend.
     */
    private void connect0() {
        try {
            stopClient();

            if (!isRunning())
                return;

            if (reconnectCnt == -1)
                log.info("Connecting to server: " + cfg.serverUri());

            if (reconnectCnt < 10)
                reconnectCnt++;

            Thread.sleep(reconnectCnt * 1000);

            if (!isRunning())
                return;

            HttpClient httpClient = new HttpClient(createServerSslFactory(cfg));

            // TODO GG-18379 Investigate how to establish native websocket connection with proxy.
            configureProxy(httpClient, cfg.serverUri());

            client = new WebSocketClient(httpClient);

            httpClient.start();
            client.start();
            client.connect(this, URI.create(cfg.serverUri()).resolve(AGENTS_PATH)).get(10L, TimeUnit.SECONDS);

            reconnectCnt = -1;
        }
        catch (InterruptedException e) {
            closeLatch.countDown();
        }
        catch (TimeoutException | ExecutionException | CancellationException ignored) {
            // No-op.
        }
        catch (Exception e) {
            if (reconnectCnt == 0)
                log.error("Failed to establish websocket connection with server: " + cfg.serverUri(), e);
        }
    }

    /**
     * Connect to backend.
     */
    private void connect() {
        connectorPool.submit(this::connect0);
    }

    /**
     * @throws InterruptedException If await failed.
     */
    public void awaitClose() throws InterruptedException {
        closeLatch.await();

        AgentClusterDemo.stop();
    }

    /**
     * @return {@code true} If web agent is running.
     */
    private boolean isRunning() {
        return closeLatch.getCount() > 0;
    }

    /**
     * @param ses Session.
     */
    @OnWebSocketConnect
    public void onConnect(Session ses) {
        AgentHandshakeRequest req = new AgentHandshakeRequest(CURRENT_VER, cfg.tokens());

        try {
            AgentUtils.send(ses, new WebSocketResponse(AGENT_HANDSHAKE, req), 10L, TimeUnit.SECONDS);
        }
        catch (Throwable e) {
            log.error("Failed to send handshake to server", e);

            connect();
        }
    }

    /**
     * @param res Response from server.
     */
    private void processHandshakeResponse(AgentHandshakeResponse res) {
        if (F.isEmpty(res.getError())) {
            validTokens = res.getTokens();

            List<String> missedTokens = new ArrayList<>(cfg.tokens());

            missedTokens.removeAll(validTokens);

            if (!F.isEmpty(missedTokens)) {
                log.warning("Failed to validate token(s): " + secured(missedTokens) + "." +
                    " Please reload agent archive or check settings");
            }

            if (F.isEmpty(validTokens)) {
                log.warning("Valid tokens not found. Stopping agent...");

                closeLatch.countDown();
            }

            log.info("Successfully completes handshake with server");
        }
        else {
            log.error(res.getError() + " Please reload agent or check settings");

            closeLatch.countDown();
        }
    }

    /**
     * @param tok Token to revoke.
     */
    private void processRevokeToken(String tok) {
        log.warning("Security token has been revoked: " + tok);

        validTokens.remove(tok);

        if (F.isEmpty(validTokens)) {
            log.warning("Web Console Agent will be stopped because no more valid tokens available");

            closeLatch.countDown();
        }
    }

    /**
     * @param msg Message.
     */
    @OnWebSocketMessage
    public void onMessage(Session ses, String msg) {
        WebSocketRequest evt = null;

        try {
            evt = fromJson(msg, WebSocketRequest.class);

            switch (evt.getEventType()) {
                case AGENT_HANDSHAKE:
                    AgentHandshakeResponse req0 = fromJson(evt.getPayload(), AgentHandshakeResponse.class);

                    processHandshakeResponse(req0);

                    if (closeLatch.getCount() > 0)
                        watcher.startWatchTask(ses);

                    break;

                case AGENT_REVOKE_TOKEN:
                    processRevokeToken(fromJson(evt.getPayload(), String.class));

                    return;

                case SCHEMA_IMPORT_DRIVERS:
                    send(ses, evt.withPayload(dbHnd.collectJdbcDrivers()));

                    break;

                case SCHEMA_IMPORT_SCHEMAS:
                    send(ses, evt.withPayload(dbHnd.collectDbSchemas(evt)));

                    break;

                case SCHEMA_IMPORT_METADATA:
                    send(ses, evt.withPayload(dbHnd.collectDbMetadata(evt)));

                    break;

                case NODE_REST:
                case NODE_VISOR:
                    if (log.isDebugEnabled())
                        log.debug("Processing REST request: " + evt);

                    RestRequest reqRest = fromJson(evt.getPayload(), RestRequest.class);

                    JsonObject params = reqRest.getParams();

                    RestResult res;

                    try {
                        res = DEMO_CLUSTER_ID.equals(reqRest.getClusterId()) ?
                            demoClusterHnd.restCommand(params) : clusterHnd.restCommand(params);
                    }
                    catch (Throwable e) {
                        res = RestResult.fail(HTTP_INTERNAL_ERROR, e.getMessage());
                    }

                    send(ses, evt.withPayload(res));

                    break;

                default:
                    log.warning("Unknown event: " + evt);
            }
        }
        catch (Throwable e) {
            if (evt == null) {
                log.error("Failed to process message: " + msg, e);

                return;
            }

            String errMsg = ERROR_MSGS.get(evt.getEventType());

            log.error(errMsg, e);

            try {
                send(ses, evt.withError(extractErrorMessage(errMsg, e)));
            }
            catch (Exception ex) {
                log.error("Failed to send response with error", e);

                connect();
            }
        }
    }

    /**
     * @param ses Session.
     * @param frame Frame.
     */
    @OnWebSocketFrame
    public void onFrame(Session ses, Frame frame) {
        if (isRunning() && frame.getType() == Frame.Type.PING) {
            if (log.isTraceEnabled())
                log.trace("Received ping message [socket=" + ses + ", msg=" + frame + "]");

            try {
                ses.getRemote().sendPong(PONG_MSG);
            }
            catch (Throwable e) {
                log.error("Failed to send pong to: " + ses, e);
            }
        }
    }

    /**
     * @param e Error.
     */
    @OnWebSocketError
    public void onError(Throwable e) {
        if (e instanceof ConnectException || e instanceof UpgradeException) {
            if (reconnectCnt <= 0)
                log.error("Failed to establish websocket connection with server: " + cfg.serverUri());

            connect();
        }
    }

    /**
     * @param statusCode Close status code.
     * @param reason Close reason.
     */
    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        if (statusCode != SERVER_ERROR)
            log.info("Websocket connection closed with code: " + statusCode);

        connect();
    }

    /**
     * Send event to websocket.
     *
     * @param ses Websocket session.
     * @param evt Event.
     * @throws Exception If failed to send event.
     */
    private static void send(Session ses, WebSocketResponse evt) throws Exception {
        AgentUtils.send(ses, evt, 60L, TimeUnit.SECONDS);
    }
}
