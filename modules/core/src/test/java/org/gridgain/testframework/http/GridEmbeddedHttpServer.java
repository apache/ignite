/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.http;

import com.sun.net.httpserver.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;

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
