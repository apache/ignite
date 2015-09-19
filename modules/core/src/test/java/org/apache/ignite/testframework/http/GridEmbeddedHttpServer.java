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

package org.apache.ignite.testframework.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Embedded HTTP/HTTPS server implementation aimed to simplify tests development
 * that need to make HTTP(s) interactions.
 * <p>
 * NOTE: this implementation is NOT thread-safe.
 */
public class GridEmbeddedHttpServer {
    /** Default hostname to bind the server to. */
    private static final String HOSTNAME_TO_BIND_SRV = "localhost";

    /** Simple Oracle HTTP server used as main workhorse. */
    private HttpServer httpSrv;

    /** Store exact protocol (HTTP or HTTPS) which we are running at. */
    private String proto;

    /**
     * Private constructor to promote server creation and initialization in <i>Builder pattern</i> style.
     */
    private GridEmbeddedHttpServer() {
        // No-op
    }

    /**
     * The class represents a server handler triggered on incoming request.
     * <p>The handler checks that a request is a HTTP GET and that url path is the expected one.
     * If all checks are passed it writes pre-configured file content to the HTTP response body.
     * </p>
     */
    private static class FileDownloadingHandler implements HttpHandler {
        /** URL path. */
        private final String urlPath;

        /** File to be downloaded. */
        private final File downloadFile;

        /**
         * Creates and configures FileDownloadingHandler.
         *
         * @param urlPath Url path on which a future GET request is going to be executed.
         * @param downloadFile File to be written into the HTTP response.
         */
        FileDownloadingHandler(String urlPath, File downloadFile) {
            this.urlPath = urlPath;
            this.downloadFile = downloadFile;
        }

        /**
         * Handles HTTP requests: checks that a request is a HTTP GET and that url path is the expected one.
         * If all checks are passed it writes pre-configured file content to the HTTP response body.
         *
         * @param exchange Wrapper above the HTTP request and response.
         */
        @Override public void handle(HttpExchange exchange) throws IOException {
            assert "GET".equalsIgnoreCase(exchange.getRequestMethod());
            assert urlPath == null || urlPath.equals(exchange.getRequestURI().toString());

            exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, 0);

            try (OutputStream resBody = exchange.getResponseBody()) {
                resBody.write(GridTestUtils.readFile(downloadFile));
            }
        }
    }

    /**
     * Creates and starts embedded HTTP server.
     *
     * @return Started HTTP server instance.
     */
    public static GridEmbeddedHttpServer startHttpServer() throws Exception {
        return createAndStart(false);
    }

    /**
     * Creates and starts embedded HTTPS server.
     *
     * @return Started HTTPS server instance.
     */
    public static GridEmbeddedHttpServer startHttpsServer() throws Exception {
        return createAndStart(true);
    }

    /**
     * Configures server with suitable for testing parameters.
     *
     * @param urlPath Url path on which a future GET request is going to be executed.
     *                If urlPath is null then no assertions against the requesting url will be done.
     * @param fileToBeDownloaded File to be written into the HTTP response.
     * @return Configured HTTP(s) server.
     */
    public GridEmbeddedHttpServer withFileDownloadingHandler(@Nullable String urlPath, File fileToBeDownloaded) {
        assert fileToBeDownloaded.exists();

        httpSrv.createContext("/", new FileDownloadingHandler(urlPath, fileToBeDownloaded));

        return this;
    }

    /**
     * Stops server by closing the listening socket and disallowing any new exchanges
     * from being processed.
     *
     * @param delay - the maximum time in seconds to wait until exchanges have finished.
     */
    public void stop(int delay) {
        httpSrv.stop(delay);
    }

    /**
     * Returns base server url in the form <i>protocol://serverHostName:serverPort</i>.
     *
     * @return Base server url.
     */
    public String getBaseUrl() {
        return proto + "://" + httpSrv.getAddress().getHostName() + ":" + httpSrv.getAddress().getPort();
    }

    /**
     * Internal method which creates and starts the server.
     *
     * @param httpsMode True if the server to be started is HTTPS, false otherwise.
     * @return Started server.
     */
    private static GridEmbeddedHttpServer createAndStart(boolean httpsMode) throws Exception {
        HttpServer httpSrv;
        InetSocketAddress addrToBind = new InetSocketAddress(HOSTNAME_TO_BIND_SRV, getAvailablePort());

        if (httpsMode) {
            HttpsServer httpsSrv = HttpsServer.create(addrToBind, 0);

            httpsSrv.setHttpsConfigurator(new HttpsConfigurator(GridTestUtils.sslContext()));

            httpSrv = httpsSrv;
        }
        else
            httpSrv = HttpServer.create(addrToBind, 0);

        GridEmbeddedHttpServer embeddedHttpSrv = new GridEmbeddedHttpServer();

        embeddedHttpSrv.proto = httpsMode ? "https" : "http";
        embeddedHttpSrv.httpSrv = httpSrv;
        embeddedHttpSrv.httpSrv.start();

        return embeddedHttpSrv;
    }

    /**
     * Returns a port number which was available for the moment of the method call.
     *
     * @return Available port number.
     */
    private static int getAvailablePort() throws IOException {
        int httpSrvPort;

        try (ServerSocket s = new ServerSocket(0)) {
            httpSrvPort = s.getLocalPort();
        }

        return httpSrvPort;
    }
}