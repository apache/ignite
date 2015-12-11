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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLHandshakeException;
import org.apache.ignite.agent.handlers.DatabaseMetadataExtractor;
import org.apache.ignite.agent.handlers.RestExecutor;
import org.apache.ignite.agent.remote.Remote;
import org.apache.ignite.agent.remote.RemoteHandler;
import org.apache.ignite.agent.remote.WebSocketSender;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

/**
 * Handler for web-socket connection.
 */
@WebSocket
public class AgentSocket implements WebSocketSender {
    /** */
    public static final Gson GSON = new Gson();

    /** */
    public static final JsonParser PARSER = new JsonParser();

    /** */
    private static final Logger log = Logger.getLogger(AgentSocket.class.getName());

    /** */
    private final CountDownLatch closeLatch = new CountDownLatch(1);

    /** */
    private final AgentConfiguration cfg;

    /** */
    private final RestExecutor restExecutor;

    /** */
    private RemoteHandler remote;

    /** */
    private Session ses;

    /**
     * @param cfg Config.
     */
    public AgentSocket(AgentConfiguration cfg, RestExecutor restExecutor) {
        this.cfg = cfg;
        this.restExecutor = restExecutor;
    }

    /**
     * @param statusCode Status code.
     * @param reason Reason.
     */
    @OnWebSocketClose
    public void onClose(int statusCode, String reason) {
        log.log(Level.WARNING, String.format("Connection closed: %d - %s.", statusCode, reason));

        if (remote != null)
            remote.close();

        closeLatch.countDown();
    }

    /**
     * @param ses Session.
     */
    @OnWebSocketConnect
    public void onConnect(Session ses) {
        log.log(Level.INFO, "Connection established.");

        this.ses = ses;

        remote = RemoteHandler.wrap(this, this, restExecutor, new DatabaseMetadataExtractor(cfg));

        JsonObject authMsg = new JsonObject();

        authMsg.addProperty("type", "AuthMessage");
        authMsg.addProperty("token", cfg.token());

        send(authMsg);
    }

    /**
     * @param msg Message.
     * @return Whether or not message was sent.
     */
    @Override public boolean send(JsonObject msg) {
        return send(GSON.toJson(msg));
    }

    /**
     * @param msg Message.
     * @return Whether or not message was sent.
     */
    @Override public boolean send(String msg) {
        try {
            ses.getRemote().sendString(msg);

            return true;
        }
        catch (IOException ignored) {
            log.log(Level.SEVERE, "Failed to send message to Control Center.");

            return false;
        }
    }

    /**
     * @param ses Session.
     * @param error Error.
     */
    @OnWebSocketError
    public void onError(Session ses, Throwable error) {
        if (error instanceof ConnectException)
            log.log(Level.WARNING, error.getMessage());
        else if (error instanceof SSLHandshakeException) {
            log.log(Level.SEVERE, "Failed to establish SSL connection to Ignite Console. Start agent with " +
                "\"-Dtrust.all=true\" to skip certificate validation in case of using self-signed certificate.", error);

            System.exit(1);
        }
        else
            log.log(Level.SEVERE, "Connection error.", error);

        if (remote != null)
            remote.close();

        closeLatch.countDown();
    }

    /**
     * @param msg Message.
     */
    @OnWebSocketMessage
    public void onMessage(String msg) {
        JsonElement jsonElement = PARSER.parse(msg);

        remote.onMessage((JsonObject)jsonElement);
    }

    /**
     * @param errorMsg Authentication failed message or {@code null} if authentication success.
     */
    @Remote
    public void authResult(String errorMsg) {
        if (errorMsg != null) {
            onClose(401, "Authentication failed: " + errorMsg);

            System.exit(1);
        }

        log.info("Authentication success.");
    }

    /**
     * Await socket close.
     */
    public void waitForClose() throws InterruptedException {
        closeLatch.await();
    }
}
