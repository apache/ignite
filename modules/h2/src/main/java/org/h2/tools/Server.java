/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.server.Service;
import org.h2.server.ShutdownHandler;
import org.h2.server.TcpServer;
import org.h2.server.pg.PgServer;
import org.h2.server.web.WebServer;
import org.h2.util.StringUtils;
import org.h2.util.Tool;
import org.h2.util.Utils;

/**
 * Starts the H2 Console (web-) server, TCP, and PG server.
 * @h2.resource
 */
public class Server extends Tool implements Runnable, ShutdownHandler {

    private final Service service;
    private Server web, tcp, pg;
    private ShutdownHandler shutdownHandler;
    private boolean started;

    public Server() {
        // nothing to do
        this.service = null;
    }

    /**
     * Create a new server for the given service.
     *
     * @param service the service
     * @param args the command line arguments
     */
    public Server(Service service, String... args) throws SQLException {
        verifyArgs(args);
        this.service = service;
        try {
            service.init(args);
        } catch (Exception e) {
            throw DbException.toSQLException(e);
        }
    }

    /**
     * When running without options, -tcp, -web, -browser and -pg are started.
     * <br />
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-web]</td>
     * <td>Start the web server with the H2 Console</td></tr>
     * <tr><td>[-webAllowOthers]</td>
     * <td>Allow other computers to connect - see below</td></tr>
     * <tr><td>[-webDaemon]</td>
     * <td>Use a daemon thread</td></tr>
     * <tr><td>[-webPort &lt;port&gt;]</td>
     * <td>The port (default: 8082)</td></tr>
     * <tr><td>[-webSSL]</td>
     * <td>Use encrypted (HTTPS) connections</td></tr>
     * <tr><td>[-browser]</td>
     * <td>Start a browser connecting to the web server</td></tr>
     * <tr><td>[-tcp]</td>
     * <td>Start the TCP server</td></tr>
     * <tr><td>[-tcpAllowOthers]</td>
     * <td>Allow other computers to connect - see below</td></tr>
     * <tr><td>[-tcpDaemon]</td>
     * <td>Use a daemon thread</td></tr>
     * <tr><td>[-tcpPort &lt;port&gt;]</td>
     * <td>The port (default: 9092)</td></tr>
     * <tr><td>[-tcpSSL]</td>
     * <td>Use encrypted (SSL) connections</td></tr>
     * <tr><td>[-tcpPassword &lt;pwd&gt;]</td>
     * <td>The password for shutting down a TCP server</td></tr>
     * <tr><td>[-tcpShutdown "&lt;url&gt;"]</td>
     * <td>Stop the TCP server; example: tcp://localhost</td></tr>
     * <tr><td>[-tcpShutdownForce]</td>
     * <td>Do not wait until all connections are closed</td></tr>
     * <tr><td>[-pg]</td>
     * <td>Start the PG server</td></tr>
     * <tr><td>[-pgAllowOthers]</td>
     * <td>Allow other computers to connect - see below</td></tr>
     * <tr><td>[-pgDaemon]</td>
     * <td>Use a daemon thread</td></tr>
     * <tr><td>[-pgPort &lt;port&gt;]</td>
     * <td>The port (default: 5435)</td></tr>
     * <tr><td>[-properties "&lt;dir&gt;"]</td>
     * <td>Server properties (default: ~, disable: null)</td></tr>
     * <tr><td>[-baseDir &lt;dir&gt;]</td>
     * <td>The base directory for H2 databases (all servers)</td></tr>
     * <tr><td>[-ifExists]</td>
     * <td>Only existing databases may be opened (all servers)</td></tr>
     * <tr><td>[-trace]</td>
     * <td>Print additional trace information (all servers)</td></tr>
     * <tr><td>[-key &lt;from&gt; &lt;to&gt;]</td>
     * <td>Allows to map a database name to another (all servers)</td></tr>
     * </table>
     * The options -xAllowOthers are potentially risky.
     * <br />
     * For details, see Advanced Topics / Protection against Remote Access.
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new Server().runTool(args);
    }

    private void verifyArgs(String... args) throws SQLException {
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg == null) {
            } else if ("-?".equals(arg) || "-help".equals(arg)) {
                // ok
            } else if (arg.startsWith("-web")) {
                if ("-web".equals(arg)) {
                    // ok
                } else if ("-webAllowOthers".equals(arg)) {
                    // no parameters
                } else if ("-webDaemon".equals(arg)) {
                    // no parameters
                } else if ("-webSSL".equals(arg)) {
                    // no parameters
                } else if ("-webPort".equals(arg)) {
                    i++;
                } else {
                    throwUnsupportedOption(arg);
                }
            } else if ("-browser".equals(arg)) {
                // ok
            } else if (arg.startsWith("-tcp")) {
                if ("-tcp".equals(arg)) {
                    // ok
                } else if ("-tcpAllowOthers".equals(arg)) {
                    // no parameters
                } else if ("-tcpDaemon".equals(arg)) {
                    // no parameters
                } else if ("-tcpSSL".equals(arg)) {
                    // no parameters
                } else if ("-tcpPort".equals(arg)) {
                    i++;
                } else if ("-tcpPassword".equals(arg)) {
                    i++;
                } else if ("-tcpShutdown".equals(arg)) {
                    i++;
                } else if ("-tcpShutdownForce".equals(arg)) {
                    // ok
                } else {
                    throwUnsupportedOption(arg);
                }
            } else if (arg.startsWith("-pg")) {
                if ("-pg".equals(arg)) {
                    // ok
                } else if ("-pgAllowOthers".equals(arg)) {
                    // no parameters
                } else if ("-pgDaemon".equals(arg)) {
                    // no parameters
                } else if ("-pgPort".equals(arg)) {
                    i++;
                } else {
                    throwUnsupportedOption(arg);
                }
            } else if (arg.startsWith("-ftp")) {
                if ("-ftpPort".equals(arg)) {
                    i++;
                } else if ("-ftpDir".equals(arg)) {
                    i++;
                } else if ("-ftpRead".equals(arg)) {
                    i++;
                } else if ("-ftpWrite".equals(arg)) {
                    i++;
                } else if ("-ftpWritePassword".equals(arg)) {
                    i++;
                } else if ("-ftpTask".equals(arg)) {
                    // no parameters
                } else {
                    throwUnsupportedOption(arg);
                }
            } else if ("-properties".equals(arg)) {
                i++;
            } else if ("-trace".equals(arg)) {
                // no parameters
            } else if ("-ifExists".equals(arg)) {
                // no parameters
            } else if ("-baseDir".equals(arg)) {
                i++;
            } else if ("-key".equals(arg)) {
                i += 2;
            } else if ("-tool".equals(arg)) {
                // no parameters
            } else {
                throwUnsupportedOption(arg);
            }
        }
    }

    @Override
    public void runTool(String... args) throws SQLException {
        boolean tcpStart = false, pgStart = false, webStart = false;
        boolean browserStart = false;
        boolean tcpShutdown = false, tcpShutdownForce = false;
        String tcpPassword = "";
        String tcpShutdownServer = "";
        boolean startDefaultServers = true;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg == null) {
            } else if ("-?".equals(arg) || "-help".equals(arg)) {
                showUsage();
                return;
            } else if (arg.startsWith("-web")) {
                if ("-web".equals(arg)) {
                    startDefaultServers = false;
                    webStart = true;
                } else if ("-webAllowOthers".equals(arg)) {
                    // no parameters
                } else if ("-webDaemon".equals(arg)) {
                    // no parameters
                } else if ("-webSSL".equals(arg)) {
                    // no parameters
                } else if ("-webPort".equals(arg)) {
                    i++;
                } else {
                    showUsageAndThrowUnsupportedOption(arg);
                }
            } else if ("-browser".equals(arg)) {
                startDefaultServers = false;
                browserStart = true;
            } else if (arg.startsWith("-tcp")) {
                if ("-tcp".equals(arg)) {
                    startDefaultServers = false;
                    tcpStart = true;
                } else if ("-tcpAllowOthers".equals(arg)) {
                    // no parameters
                } else if ("-tcpDaemon".equals(arg)) {
                    // no parameters
                } else if ("-tcpSSL".equals(arg)) {
                    // no parameters
                } else if ("-tcpPort".equals(arg)) {
                    i++;
                } else if ("-tcpPassword".equals(arg)) {
                    tcpPassword = args[++i];
                } else if ("-tcpShutdown".equals(arg)) {
                    startDefaultServers = false;
                    tcpShutdown = true;
                    tcpShutdownServer = args[++i];
                } else if ("-tcpShutdownForce".equals(arg)) {
                    tcpShutdownForce = true;
                } else {
                    showUsageAndThrowUnsupportedOption(arg);
                }
            } else if (arg.startsWith("-pg")) {
                if ("-pg".equals(arg)) {
                    startDefaultServers = false;
                    pgStart = true;
                } else if ("-pgAllowOthers".equals(arg)) {
                    // no parameters
                } else if ("-pgDaemon".equals(arg)) {
                    // no parameters
                } else if ("-pgPort".equals(arg)) {
                    i++;
                } else {
                    showUsageAndThrowUnsupportedOption(arg);
                }
            } else if ("-properties".equals(arg)) {
                i++;
            } else if ("-trace".equals(arg)) {
                // no parameters
            } else if ("-ifExists".equals(arg)) {
                // no parameters
            } else if ("-baseDir".equals(arg)) {
                i++;
            } else if ("-key".equals(arg)) {
                i += 2;
            } else {
                showUsageAndThrowUnsupportedOption(arg);
            }
        }
        verifyArgs(args);
        if (startDefaultServers) {
            tcpStart = true;
            pgStart = true;
            webStart = true;
            browserStart = true;
        }
        // TODO server: maybe use one single properties file?
        if (tcpShutdown) {
            out.println("Shutting down TCP Server at " + tcpShutdownServer);
            shutdownTcpServer(tcpShutdownServer, tcpPassword,
                    tcpShutdownForce, false);
        }
        try {
            if (tcpStart) {
                tcp = createTcpServer(args);
                tcp.start();
                out.println(tcp.getStatus());
                tcp.setShutdownHandler(this);
            }
            if (pgStart) {
                pg = createPgServer(args);
                pg.start();
                out.println(pg.getStatus());
            }
            if (webStart) {
                web = createWebServer(args);
                web.setShutdownHandler(this);
                SQLException result = null;
                try {
                    web.start();
                } catch (Exception e) {
                    result = DbException.toSQLException(e);
                }
                out.println(web.getStatus());
                // start browser in any case (even if the server is already
                // running) because some people don't look at the output, but
                // are wondering why nothing happens
                if (browserStart) {
                    try {
                        openBrowser(web.getURL());
                    } catch (Exception e) {
                        out.println(e.getMessage());
                    }
                }
                if (result != null) {
                    throw result;
                }
            } else if (browserStart) {
                out.println("The browser can only start if a web server is started (-web)");
            }
        } catch (SQLException e) {
            stopAll();
            throw e;
        }
    }

    /**
     * Shutdown one or all TCP server. If force is set to false, the server will
     * not allow new connections, but not kill existing connections, instead it
     * will stop if the last connection is closed. If force is set to true,
     * existing connections are killed. After calling the method with
     * force=false, it is not possible to call it again with force=true because
     * new connections are not allowed. Example:
     *
     * <pre>
     * Server.shutdownTcpServer(&quot;tcp://localhost:9094&quot;,
     *         password, true, false);
     * </pre>
     *
     * @param url example: tcp://localhost:9094
     * @param password the password to use ("" for no password)
     * @param force the shutdown (don't wait)
     * @param all whether all TCP servers that are running in the JVM should be
     *            stopped
     */
    public static void shutdownTcpServer(String url, String password,
            boolean force, boolean all) throws SQLException {
        TcpServer.shutdown(url, password, force, all);
    }

    /**
     * Get the status of this server.
     *
     * @return the status
     */
    public String getStatus() {
        StringBuilder buff = new StringBuilder();
        if (!started) {
            buff.append("Not started");
        } else if (isRunning(false)) {
            buff.append(service.getType()).
                append(" server running at ").
                append(service.getURL()).
                append(" (");
            if (service.getAllowOthers()) {
                buff.append("others can connect");
            } else {
                buff.append("only local connections");
            }
            buff.append(')');
        } else {
            buff.append("The ").
                append(service.getType()).
                append(" server could not be started. " +
                        "Possible cause: another server is already running at ").
                append(service.getURL());
        }
        return buff.toString();
    }

    /**
     * Create a new web server, but does not start it yet. Example:
     *
     * <pre>
     * Server server = Server.createWebServer("-trace").start();
     * </pre>
     * Supported options are:
     * -webPort, -webSSL, -webAllowOthers, -webDaemon,
     * -trace, -ifExists, -baseDir, -properties.
     * See the main method for details.
     *
     * @param args the argument list
     * @return the server
     */
    public static Server createWebServer(String... args) throws SQLException {
        WebServer service = new WebServer();
        Server server = new Server(service, args);
        service.setShutdownHandler(server);
        return server;
    }

    /**
     * Create a new TCP server, but does not start it yet. Example:
     *
     * <pre>
     * Server server = Server.createTcpServer(
     *     "-tcpPort", "9123", "-tcpAllowOthers").start();
     * </pre>
     * Supported options are:
     * -tcpPort, -tcpSSL, -tcpPassword, -tcpAllowOthers, -tcpDaemon,
     * -trace, -ifExists, -baseDir, -key.
     * See the main method for details.
     * <p>
     * If no port is specified, the default port is used if possible,
     * and if this port is already used, a random port is used.
     * Use getPort() or getURL() after starting to retrieve the port.
     * </p>
     *
     * @param args the argument list
     * @return the server
     */
    public static Server createTcpServer(String... args) throws SQLException {
        TcpServer service = new TcpServer();
        Server server = new Server(service, args);
        service.setShutdownHandler(server);
        return server;
    }

    /**
     * Create a new PG server, but does not start it yet.
     * Example:
     * <pre>
     * Server server =
     *     Server.createPgServer("-pgAllowOthers").start();
     * </pre>
     * Supported options are:
     * -pgPort, -pgAllowOthers, -pgDaemon,
     * -trace, -ifExists, -baseDir, -key.
     * See the main method for details.
     * <p>
     * If no port is specified, the default port is used if possible,
     * and if this port is already used, a random port is used.
     * Use getPort() or getURL() after starting to retrieve the port.
     * </p>
     *
     * @param args the argument list
     * @return the server
     */
    public static Server createPgServer(String... args) throws SQLException {
        return new Server(new PgServer(), args);
    }

    /**
     * Tries to start the server.
     * @return the server if successful
     * @throws SQLException if the server could not be started
     */
    public Server start() throws SQLException {
        try {
            started = true;
            service.start();
            String name = service.getName() + " (" + service.getURL() + ")";
            Thread t = new Thread(this, name);
            t.setDaemon(service.isDaemon());
            t.start();
            for (int i = 1; i < 64; i += i) {
                wait(i);
                if (isRunning(false)) {
                    return this;
                }
            }
            if (isRunning(true)) {
                return this;
            }
            throw DbException.get(ErrorCode.EXCEPTION_OPENING_PORT_2,
                    name, "timeout; " +
                    "please check your network configuration, specially the file /etc/hosts");
        } catch (DbException e) {
            throw DbException.toSQLException(e);
        }
    }

    private static void wait(int i) {
        try {
            // sleep at most 4096 ms
            long sleep = (long) i * (long) i;
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private void stopAll() {
        Server s = web;
        if (s != null && s.isRunning(false)) {
            s.stop();
            web = null;
        }
        s = tcp;
        if (s != null && s.isRunning(false)) {
            s.stop();
            tcp = null;
        }
        s = pg;
        if (s != null && s.isRunning(false)) {
            s.stop();
            pg = null;
        }
    }

    /**
     * Checks if the server is running.
     *
     * @param traceError if errors should be written
     * @return if the server is running
     */
    public boolean isRunning(boolean traceError) {
        return service.isRunning(traceError);
    }

    /**
     * Stops the server.
     */
    public void stop() {
        started = false;
        if (service != null) {
            service.stop();
        }
    }

    /**
     * Gets the URL of this server.
     *
     * @return the url
     */
    public String getURL() {
        return service.getURL();
    }

    /**
     * Gets the port this server is listening on.
     *
     * @return the port
     */
    public int getPort() {
        return service.getPort();
    }

    /**
     * INTERNAL
     */
    @Override
    public void run() {
        try {
            service.listen();
        } catch (Exception e) {
            DbException.traceThrowable(e);
        }
    }

    /**
     * INTERNAL
     */
    public void setShutdownHandler(ShutdownHandler shutdownHandler) {
        this.shutdownHandler = shutdownHandler;
    }

    /**
     * INTERNAL
     */
    @Override
    public void shutdown() {
        if (shutdownHandler != null) {
            shutdownHandler.shutdown();
        } else {
            stopAll();
        }
    }

    /**
     * Get the service attached to this server.
     *
     * @return the service
     */
    public Service getService() {
        return service;
    }

    /**
     * Open a new browser tab or window with the given URL.
     *
     * @param url the URL to open
     */
    public static void openBrowser(String url) throws Exception {
        try {
            String osName = StringUtils.toLowerEnglish(
                    Utils.getProperty("os.name", "linux"));
            Runtime rt = Runtime.getRuntime();
            String browser = Utils.getProperty(SysProperties.H2_BROWSER, null);
            if (browser == null) {
                // under Linux, this will point to the default system browser
                try {
                    browser = System.getenv("BROWSER");
                } catch (SecurityException se) {
                    // ignore
                }
            }
            if (browser != null) {
                if (browser.startsWith("call:")) {
                    browser = browser.substring("call:".length());
                    Utils.callStaticMethod(browser, url);
                } else if (browser.contains("%url")) {
                    String[] args = StringUtils.arraySplit(browser, ',', false);
                    for (int i = 0; i < args.length; i++) {
                        args[i] = StringUtils.replaceAll(args[i], "%url", url);
                    }
                    rt.exec(args);
                } else if (osName.contains("windows")) {
                    rt.exec(new String[] { "cmd.exe", "/C",  browser, url });
                } else {
                    rt.exec(new String[] { browser, url });
                }
                return;
            }
            try {
                Class<?> desktopClass = Class.forName("java.awt.Desktop");
                // Desktop.isDesktopSupported()
                Boolean supported = (Boolean) desktopClass.
                    getMethod("isDesktopSupported").
                    invoke(null, new Object[0]);
                URI uri = new URI(url);
                if (supported) {
                    // Desktop.getDesktop();
                    Object desktop = desktopClass.getMethod("getDesktop").
                        invoke(null);
                    // desktop.browse(uri);
                    desktopClass.getMethod("browse", URI.class).
                        invoke(desktop, uri);
                    return;
                }
            } catch (Exception e) {
                // ignore
            }
            if (osName.contains("windows")) {
                rt.exec(new String[] { "rundll32", "url.dll,FileProtocolHandler", url });
            } else if (osName.contains("mac") || osName.contains("darwin")) {
                // Mac OS: to open a page with Safari, use "open -a Safari"
                Runtime.getRuntime().exec(new String[] { "open", url });
            } else {
                String[] browsers = { "xdg-open", "chromium", "google-chrome",
                        "firefox", "mozilla-firefox", "mozilla", "konqueror",
                        "netscape", "opera", "midori" };
                boolean ok = false;
                for (String b : browsers) {
                    try {
                        rt.exec(new String[] { b, url });
                        ok = true;
                        break;
                    } catch (Exception e) {
                        // ignore and try the next
                    }
                }
                if (!ok) {
                    // No success in detection.
                    throw new Exception(
                            "Browser detection failed and system property " +
                            SysProperties.H2_BROWSER + " not set");
                }
            }
        } catch (Exception e) {
            throw new Exception(
                    "Failed to start a browser to open the URL " +
            url + ": " + e.getMessage());
        }
    }

    /**
     * Start a web server and a browser that uses the given connection. The
     * current transaction is preserved. This is specially useful to manually
     * inspect the database when debugging. This method return as soon as the
     * user has disconnected.
     *
     * @param conn the database connection (the database must be open)
     */
    public static void startWebServer(Connection conn) throws SQLException {
        startWebServer(conn, false);
    }

    /**
     * Start a web server and a browser that uses the given connection. The
     * current transaction is preserved. This is specially useful to manually
     * inspect the database when debugging. This method return as soon as the
     * user has disconnected.
     *
     * @param conn the database connection (the database must be open)
     * @param ignoreProperties if {@code true} properties from
     *         {@code .h2.server.properties} will be ignored
     */
    public static void startWebServer(Connection conn, boolean ignoreProperties) throws SQLException {
        WebServer webServer = new WebServer();
        String[] args;
        if (ignoreProperties) {
            args = new String[] { "-webPort", "0", "-properties", "null"};
        } else {
            args = new String[] { "-webPort", "0" };
        }
        Server web = new Server(webServer, args);
        web.start();
        Server server = new Server();
        server.web = web;
        webServer.setShutdownHandler(server);
        String url = webServer.addSession(conn);
        try {
            Server.openBrowser(url);
            while (!webServer.isStopped()) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            // ignore
        }
    }

}
