/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

//## AWT ##
import java.awt.Button;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GraphicsEnvironment;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Image;
import java.awt.Insets;
import java.awt.Label;
import java.awt.MenuItem;
import java.awt.Panel;
import java.awt.PopupMenu;
import java.awt.SystemColor;
import java.awt.TextField;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.h2.server.ShutdownHandler;
import org.h2.util.JdbcUtils;
import org.h2.util.Tool;
import org.h2.util.Utils;

/**
 * Starts the H2 Console (web-) server, as well as the TCP and PG server.
 * @h2.resource
 *
 * @author Thomas Mueller, Ridvan Agar
 */
public class Console extends Tool implements
//## AWT ##
ActionListener, MouseListener, WindowListener,
//*/
ShutdownHandler {

//## AWT ##
    private Frame frame;
    private boolean trayIconUsed;
    private Font font;
    private Button startBrowser;
    private TextField urlText;
    private Object tray;
    private Object trayIcon;
//*/
    private Server web, tcp, pg;
    private boolean isWindows;
    private long lastOpenNs;

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
        new Console().runTool(args);
    }

    /**
     * This tool starts the H2 Console (web-) server, as well as the TCP and PG
     * server. For JDK 1.6, a system tray icon is created, for platforms that
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
                web = Server.createWebServer(args);
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

//## AWT ##
        if (toolStart && webRunning && !GraphicsEnvironment.isHeadless()) {
            loadFont();
            try {
                if (!createTrayIcon()) {
                    showWindow();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//*/

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

    private void printProblem(Exception e, Server server) {
        if (server == null) {
            e.printStackTrace();
        } else {
            out.println(server.getStatus());
            out.println("Root cause: " + e.getMessage());
        }
    }

    private static Image loadImage(String name) {
        try {
            byte[] imageData = Utils.getResource(name);
            if (imageData == null) {
                return null;
            }
            return Toolkit.getDefaultToolkit().createImage(imageData);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
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
//## AWT ##
        if (frame != null) {
            frame.dispose();
            frame = null;
        }
        if (trayIconUsed) {
            try {
                // tray.remove(trayIcon);
                Utils.callMethod(tray, "remove", trayIcon);
            } catch (Exception e) {
                // ignore
            } finally {
                trayIcon = null;
                tray = null;
                trayIconUsed = false;
            }
            System.gc();
            // Mac OS X: Console tool process did not stop on exit
            String os = System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH);
            if (os.contains("mac")) {
                for (Thread t : Thread.getAllStackTraces().keySet()) {
                    if (t.getName().startsWith("AWT-")) {
                        t.interrupt();
                    }
                }
            }
            Thread.currentThread().interrupt();
            // throw new ThreadDeath();
        }
//*/
    }

//## AWT ##
    private void loadFont() {
        if (isWindows) {
            font = new Font("Dialog", Font.PLAIN, 11);
        } else {
            font = new Font("Dialog", Font.PLAIN, 12);
        }
    }

    private boolean createTrayIcon() {
        try {
            // SystemTray.isSupported();
            boolean supported = (Boolean) Utils.callStaticMethod(
                    "java.awt.SystemTray.isSupported");
            if (!supported) {
                return false;
            }
            PopupMenu menuConsole = new PopupMenu();
            MenuItem itemConsole = new MenuItem("H2 Console");
            itemConsole.setActionCommand("console");
            itemConsole.addActionListener(this);
            itemConsole.setFont(font);
            menuConsole.add(itemConsole);
            MenuItem itemStatus = new MenuItem("Status");
            itemStatus.setActionCommand("status");
            itemStatus.addActionListener(this);
            itemStatus.setFont(font);
            menuConsole.add(itemStatus);
            MenuItem itemExit = new MenuItem("Exit");
            itemExit.setFont(font);
            itemExit.setActionCommand("exit");
            itemExit.addActionListener(this);
            menuConsole.add(itemExit);

            // tray = SystemTray.getSystemTray();
            tray = Utils.callStaticMethod("java.awt.SystemTray.getSystemTray");

            // Dimension d = tray.getTrayIconSize();
            Dimension d = (Dimension) Utils.callMethod(tray, "getTrayIconSize");
            String iconFile;
            if (d.width >= 24 && d.height >= 24) {
                iconFile = "/org/h2/res/h2-24.png";
            } else if (d.width >= 22 && d.height >= 22) {
                // for Mac OS X 10.8.1 with retina display:
                // the reported resolution is 22 x 22, but the image
                // is scaled and the real resolution is 44 x 44
                iconFile = "/org/h2/res/h2-64-t.png";
                // iconFile = "/org/h2/res/h2-22-t.png";
            } else {
                iconFile = "/org/h2/res/h2.png";
            }
            Image icon = loadImage(iconFile);

            // trayIcon = new TrayIcon(image, "H2 Database Engine",
            //         menuConsole);
            trayIcon = Utils.newInstance("java.awt.TrayIcon",
                    icon, "H2 Database Engine", menuConsole);

            // trayIcon.addMouseListener(this);
            Utils.callMethod(trayIcon, "addMouseListener", this);

            // tray.add(trayIcon);
            Utils.callMethod(tray, "add", trayIcon);

            this.trayIconUsed = true;

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void showWindow() {
        if (frame != null) {
            return;
        }
        frame = new Frame("H2 Console");
        frame.addWindowListener(this);
        Image image = loadImage("/org/h2/res/h2.png");
        if (image != null) {
            frame.setIconImage(image);
        }
        frame.setResizable(false);
        frame.setBackground(SystemColor.control);

        GridBagLayout layout = new GridBagLayout();
        frame.setLayout(layout);

        // the main panel keeps everything together
        Panel mainPanel = new Panel(layout);

        GridBagConstraints constraintsPanel = new GridBagConstraints();
        constraintsPanel.gridx = 0;
        constraintsPanel.weightx = 1.0D;
        constraintsPanel.weighty = 1.0D;
        constraintsPanel.fill = GridBagConstraints.BOTH;
        constraintsPanel.insets = new Insets(0, 10, 0, 10);
        constraintsPanel.gridy = 0;

        GridBagConstraints constraintsButton = new GridBagConstraints();
        constraintsButton.gridx = 0;
        constraintsButton.gridwidth = 2;
        constraintsButton.insets = new Insets(10, 0, 0, 0);
        constraintsButton.gridy = 1;
        constraintsButton.anchor = GridBagConstraints.EAST;

        GridBagConstraints constraintsTextField = new GridBagConstraints();
        constraintsTextField.fill = GridBagConstraints.HORIZONTAL;
        constraintsTextField.gridy = 0;
        constraintsTextField.weightx = 1.0;
        constraintsTextField.insets = new Insets(0, 5, 0, 0);
        constraintsTextField.gridx = 1;

        GridBagConstraints constraintsLabel = new GridBagConstraints();
        constraintsLabel.gridx = 0;
        constraintsLabel.gridy = 0;

        Label label = new Label("H2 Console URL:", Label.LEFT);
        label.setFont(font);
        mainPanel.add(label, constraintsLabel);

        urlText = new TextField();
        urlText.setEditable(false);
        urlText.setFont(font);
        urlText.setText(web.getURL());
        if (isWindows) {
            urlText.setFocusable(false);
        }
        mainPanel.add(urlText, constraintsTextField);

        startBrowser = new Button("Start Browser");
        startBrowser.setFocusable(false);
        startBrowser.setActionCommand("console");
        startBrowser.addActionListener(this);
        startBrowser.setFont(font);
        mainPanel.add(startBrowser, constraintsButton);
        frame.add(mainPanel, constraintsPanel);

        int width = 300, height = 120;
        frame.setSize(width, height);
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        frame.setLocation((screenSize.width - width) / 2,
                (screenSize.height - height) / 2);
        try {
            frame.setVisible(true);
        } catch (Throwable t) {
            // ignore
            // some systems don't support this method, for example IKVM
            // however it still works
        }
        try {
            // ensure this window is in front of the browser
            frame.setAlwaysOnTop(true);
            frame.setAlwaysOnTop(false);
        } catch (Throwable t) {
            // ignore
        }
    }

    private void startBrowser() {
        if (web != null) {
            String url = web.getURL();
            if (urlText != null) {
                urlText.setText(url);
            }
            long now = System.nanoTime();
            if (lastOpenNs == 0 || lastOpenNs + TimeUnit.MILLISECONDS.toNanos(100) < now) {
                lastOpenNs = now;
                openBrowser(url);
            }
        }
    }
//*/

    private void openBrowser(String url) {
        try {
            Server.openBrowser(url);
        } catch (Exception e) {
            out.println(e.getMessage());
        }
    }

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void actionPerformed(ActionEvent e) {
        String command = e.getActionCommand();
        if ("exit".equals(command)) {
            shutdown();
        } else if ("console".equals(command)) {
            startBrowser();
        } else if ("status".equals(command)) {
            showWindow();
        } else if (startBrowser == e.getSource()) {
            // for some reason, IKVM ignores setActionCommand
            startBrowser();
        }
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void mouseClicked(MouseEvent e) {
        if (e.getButton() == MouseEvent.BUTTON1) {
            startBrowser();
        }
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void mouseEntered(MouseEvent e) {
        // nothing to do
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void mouseExited(MouseEvent e) {
        // nothing to do
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void mousePressed(MouseEvent e) {
        // nothing to do
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void mouseReleased(MouseEvent e) {
        // nothing to do
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void windowClosing(WindowEvent e) {
        if (trayIconUsed) {
            frame.dispose();
            frame = null;
        } else {
            shutdown();
        }
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void windowActivated(WindowEvent e) {
        // nothing to do
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void windowClosed(WindowEvent e) {
        // nothing to do
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void windowDeactivated(WindowEvent e) {
        // nothing to do
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void windowDeiconified(WindowEvent e) {
        // nothing to do
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void windowIconified(WindowEvent e) {
        // nothing to do
    }
//*/

    /**
     * INTERNAL
     */
//## AWT ##
    @Override
    public void windowOpened(WindowEvent e) {
        // nothing to do
    }
//*/

}
