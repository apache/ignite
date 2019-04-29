/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.Connection;
import java.sql.SQLException;
import org.h2.server.ShutdownHandler;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;
import org.h2.util.Tool;
import org.h2.util.Utils;

/**
 * Starts the H2 Console (web-) server, as well as the TCP and PG server.
 * @h2.resource
 *
 * @author Thomas Mueller, Ridvan Agar
 */
public class Console extends Tool implements ShutdownHandler {

    Server web;

    private Server tcp, pg;

    boolean isWindows;

    /**
     * When running without options, -tcp, -web, -browser and -pg are started.
     * <br />
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-url]</td>
     * <td>Start a browser and connect to this URL</td></tr>
     * <tr><td>[-driver]</td>
     * <td>Used together with -url: the driver</td></tr>
     * <tr><td>[-user]</td>
     * <td>Used together with -url: the user name</td></tr>
     * <tr><td>[-password]</td>
     * <td>Used together with -url: the password</td></tr>
     * <tr><td>[-web]</td>
     * <td>Start the web server with the H2 Console</td></tr>
     * <tr><td>[-tool]</td>
     * <td>Start the icon or window that allows to start a browser</td></tr>
     * <tr><td>[-browser]</td>
     * <td>Start a browser connecting to the web server</td></tr>
     * <tr><td>[-tcp]</td>
     * <td>Start the TCP server</td></tr>
     * <tr><td>[-pg]</td>
     * <td>Start the PG server</td></tr>
     * </table>
     * For each Server, additional options are available;
     * for details, see the Server tool.<br />
     * If a service can not be started, the program
     * terminates with an exit code of 1.
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        Console console;
        try {
            console = (Console) Utils.newInstance("org.h2.tools.GUIConsole");
        } catch (Exception | NoClassDefFoundError e) {
            console = new Console();
        }
        console.runTool(args);
    }

    /**
     * This tool starts the H2 Console (web-) server, as well as the TCP and PG
     * server. A system tray icon is created, for platforms that
     * support it. Otherwise, a small window opens.
     *
     * @param args the command line arguments
     */
    @Override
    public void runTool(String... args) throws SQLException {
        isWindows = Utils.getProperty("os.name", "").startsWith("Windows");
        boolean tcpStart = false, pgStart = false, webStart = false, toolStart = false;
        boolean browserStart = false;
        boolean startDefaultServers = true;
        boolean printStatus = args != null && args.length > 0;
        String driver = null, url = null, user = null, password = null;
        boolean tcpShutdown = false, tcpShutdownForce = false;
        String tcpPassword = "";
        String tcpShutdownServer = "";
        boolean ifExists = false, webAllowOthers = false;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg == null) {
            } else if ("-?".equals(arg) || "-help".equals(arg)) {
                showUsage();
                return;
            } else if ("-url".equals(arg)) {
                startDefaultServers = false;
                url = args[++i];
            } else if ("-driver".equals(arg)) {
                driver = args[++i];
            } else if ("-user".equals(arg)) {
                user = args[++i];
            } else if ("-password".equals(arg)) {
                password = args[++i];
            } else if (arg.startsWith("-web")) {
                if ("-web".equals(arg)) {
                    startDefaultServers = false;
                    webStart = true;
                } else if ("-webAllowOthers".equals(arg)) {
                    // no parameters
                    webAllowOthers = true;
                } else if ("-webDaemon".equals(arg)) {
                    // no parameters
                } else if ("-webSSL".equals(arg)) {
                    // no parameters
                } else if ("-webPort".equals(arg)) {
                    i++;
                } else {
                    showUsageAndThrowUnsupportedOption(arg);
                }
            } else if ("-tool".equals(arg)) {
                startDefaultServers = false;
                webStart = true;
                toolStart = true;
            } else if ("-browser".equals(arg)) {
                startDefaultServers = false;
                webStart = true;
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
                ifExists = true;
            } else if ("-baseDir".equals(arg)) {
                i++;
            } else {
                showUsageAndThrowUnsupportedOption(arg);
            }
        }
        if (startDefaultServers) {
            webStart = true;
            toolStart = true;
            browserStart = true;
            tcpStart = true;
            pgStart = true;
        }
        if (tcpShutdown) {
            out.println("Shutting down TCP Server at " + tcpShutdownServer);
            Server.shutdownTcpServer(tcpShutdownServer,
                    tcpPassword, tcpShutdownForce, false);
        }
        SQLException startException = null;
        boolean webRunning = false;

        if (url != null) {
            Connection conn = JdbcUtils.getConnection(driver, url, user, password);
            Server.startWebServer(conn);
        }

        if (webStart) {
            try {
                String webKey = webAllowOthers ? null
                        : StringUtils.convertBytesToHex(MathUtils.secureRandomBytes(32));
                web = Server.createWebServer(args, webKey, !ifExists);
                web.setShutdownHandler(this);
                web.start();
                if (printStatus) {
                    out.println(web.getStatus());
                }
                webRunning = true;
            } catch (SQLException e) {
                printProblem(e, web);
                startException = e;
            }
        }

        if (toolStart && webRunning){
            show();
        }

        // start browser in any case (even if the server is already running)
        // because some people don't look at the output,
        // but are wondering why nothing happens
        if (browserStart && web != null) {
            openBrowser(web.getURL());
        }

        if (tcpStart) {
            try {
                tcp = Server.createTcpServer(args);
                tcp.start();
                if (printStatus) {
                    out.println(tcp.getStatus());
                }
                tcp.setShutdownHandler(this);
            } catch (SQLException e) {
                printProblem(e, tcp);
                if (startException == null) {
                    startException = e;
                }
            }
        }
        if (pgStart) {
            try {
                pg = Server.createPgServer(args);
                pg.start();
                if (printStatus) {
                    out.println(pg.getStatus());
                }
            } catch (SQLException e) {
                printProblem(e, pg);
                if (startException == null) {
                    startException = e;
                }
            }
        }
        if (startException != null) {
            shutdown();
            throw startException;
        }
    }

    /**
     * Overridden by GUIConsole to show a window
     */
    void show() {
    }

    private void printProblem(Exception e, Server server) {
        if (server == null) {
            e.printStackTrace();
        } else {
            out.println(server.getStatus());
            out.println("Root cause: " + e.getMessage());
        }
    }

    /**
     * INTERNAL.
     * Stop all servers that were started using the console.
     */
    @Override
    public void shutdown() {
        if (web != null && web.isRunning(false)) {
            web.stop();
            web = null;
        }
        if (tcp != null && tcp.isRunning(false)) {
            tcp.stop();
            tcp = null;
        }
        if (pg != null && pg.isRunning(false)) {
            pg.stop();
            pg = null;
        }
    }

    /**
     * Open a new browser tab or window with the given URL.
     *
     * @param url the URL to open
     */
    void openBrowser(String url) {
        try {
            Server.openBrowser(url);
        } catch (Exception e) {
            out.println(e.getMessage());
        }
    }

}
