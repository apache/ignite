/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.awt.Button;
import java.awt.Color;
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
import java.awt.TextArea;
import java.awt.TextField;
import java.awt.Toolkit;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.IOException;
import java.sql.DriverManager;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.h2.util.Utils;

/**
 * Console for environments with AWT support.
 */
public class GUIConsole extends Console implements ActionListener, MouseListener, WindowListener {
    private long lastOpenNs;

    private boolean trayIconUsed;
    private Font font;

    private Frame statusFrame;
    private TextField urlText;
    private Button startBrowser;

    private Frame createFrame;
    private TextField pathField, userField, passwordField, passwordConfirmationField;
    private Button createButton;
    private TextArea errorArea;

    private Object tray;
    private Object trayIcon;

    @Override
    void show() {
        if (!GraphicsEnvironment.isHeadless()) {
            loadFont();
            try {
                if (!createTrayIcon()) {
                    showStatusWindow();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
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

    @Override
    public void shutdown() {
        super.shutdown();
        if (statusFrame != null) {
            statusFrame.dispose();
            statusFrame = null;
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
    }

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
            MenuItem itemCreate = new MenuItem("Create a new database...");
            itemCreate.setActionCommand("showCreate");
            itemCreate.addActionListener(this);
            itemCreate.setFont(font);
            menuConsole.add(itemCreate);
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

    private void showStatusWindow() {
        if (statusFrame != null) {
            return;
        }
        statusFrame = new Frame("H2 Console");
        statusFrame.addWindowListener(this);
        Image image = loadImage("/org/h2/res/h2.png");
        if (image != null) {
            statusFrame.setIconImage(image);
        }
        statusFrame.setResizable(false);
        statusFrame.setBackground(SystemColor.control);

        GridBagLayout layout = new GridBagLayout();
        statusFrame.setLayout(layout);

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
        statusFrame.add(mainPanel, constraintsPanel);

        int width = 300, height = 120;
        statusFrame.setSize(width, height);
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        statusFrame.setLocation((screenSize.width - width) / 2,
                (screenSize.height - height) / 2);
        try {
            statusFrame.setVisible(true);
        } catch (Throwable t) {
            // ignore
            // some systems don't support this method, for example IKVM
            // however it still works
        }
        try {
            // ensure this window is in front of the browser
            statusFrame.setAlwaysOnTop(true);
            statusFrame.setAlwaysOnTop(false);
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

    private void showCreateDatabase() {
        if (createFrame != null) {
            return;
        }
        createFrame = new Frame("H2 Console");
        createFrame.addWindowListener(this);
        Image image = loadImage("/org/h2/res/h2.png");
        if (image != null) {
            createFrame.setIconImage(image);
        }
        createFrame.setResizable(false);
        createFrame.setBackground(SystemColor.control);

        GridBagLayout layout = new GridBagLayout();
        createFrame.setLayout(layout);

        // the main panel keeps everything together
        Panel mainPanel = new Panel(layout);

        GridBagConstraints constraints;

        constraints = new GridBagConstraints();
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.insets = new Insets(10, 0, 0, 0);
        constraints.gridx = 0;
        constraints.gridy = 0;
        Label urlLabel = new Label("Database path:", Label.LEFT);
        urlLabel.setFont(font);
        mainPanel.add(urlLabel, constraints);

        constraints = new GridBagConstraints();
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.gridy = 0;
        constraints.weightx = 1d;
        constraints.insets = new Insets(10, 5, 0, 0);
        constraints.gridx = 1;
        pathField = new TextField();
        pathField.setFont(font);
        pathField.setText("./test");
        mainPanel.add(pathField, constraints);

        constraints = new GridBagConstraints();
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.gridx = 0;
        constraints.gridy = 1;
        Label userLabel = new Label("Username:", Label.LEFT);
        userLabel.setFont(font);
        mainPanel.add(userLabel, constraints);

        constraints = new GridBagConstraints();
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.gridy = 1;
        constraints.weightx = 1d;
        constraints.insets = new Insets(0, 5, 0, 0);
        constraints.gridx = 1;
        userField = new TextField();
        userField.setFont(font);
        userField.setText("sa");
        mainPanel.add(userField, constraints);

        constraints = new GridBagConstraints();
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.gridx = 0;
        constraints.gridy = 2;
        Label passwordLabel = new Label("Password:", Label.LEFT);
        passwordLabel.setFont(font);
        mainPanel.add(passwordLabel, constraints);

        constraints = new GridBagConstraints();
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.gridy = 2;
        constraints.weightx = 1d;
        constraints.insets = new Insets(0, 5, 0, 0);
        constraints.gridx = 1;
        passwordField = new TextField();
        passwordField.setFont(font);
        passwordField.setEchoChar('*');
        mainPanel.add(passwordField, constraints);

        constraints = new GridBagConstraints();
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.gridx = 0;
        constraints.gridy = 3;
        Label passwordConfirmationLabel = new Label("Password confirmation:", Label.LEFT);
        passwordConfirmationLabel.setFont(font);
        mainPanel.add(passwordConfirmationLabel, constraints);

        constraints = new GridBagConstraints();
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.gridy = 3;
        constraints.weightx = 1d;
        constraints.insets = new Insets(0, 5, 0, 0);
        constraints.gridx = 1;
        passwordConfirmationField = new TextField();
        passwordConfirmationField.setFont(font);
        passwordConfirmationField.setEchoChar('*');
        mainPanel.add(passwordConfirmationField, constraints);

        constraints = new GridBagConstraints();
        constraints.gridx = 0;
        constraints.gridwidth = 2;
        constraints.insets = new Insets(10, 0, 0, 0);
        constraints.gridy = 4;
        constraints.anchor = GridBagConstraints.EAST;
        createButton = new Button("Create");
        createButton.setFocusable(false);
        createButton.setActionCommand("create");
        createButton.addActionListener(this);
        createButton.setFont(font);
        mainPanel.add(createButton, constraints);

        constraints = new GridBagConstraints();
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.gridy = 5;
        constraints.weightx = 1d;
        constraints.insets = new Insets(10, 0, 0, 0);
        constraints.gridx = 0;
        constraints.gridwidth = 2;
        errorArea = new TextArea();
        errorArea.setFont(font);
        errorArea.setEditable(false);
        mainPanel.add(errorArea, constraints);

        constraints = new GridBagConstraints();
        constraints.gridx = 0;
        constraints.weightx = 1d;
        constraints.weighty = 1d;
        constraints.fill = GridBagConstraints.BOTH;
        constraints.insets = new Insets(0, 10, 0, 10);
        constraints.gridy = 0;
        createFrame.add(mainPanel, constraints);

        int width = 400, height = 400;
        createFrame.setSize(width, height);
        createFrame.pack();
        createFrame.setLocationRelativeTo(null);
        try {
            createFrame.setVisible(true);
        } catch (Throwable t) {
            // ignore
            // some systems don't support this method, for example IKVM
            // however it still works
        }
        try {
            // ensure this window is in front of the browser
            createFrame.setAlwaysOnTop(true);
            createFrame.setAlwaysOnTop(false);
        } catch (Throwable t) {
            // ignore
        }
    }

    private void createDatabase() {
        if (web == null || createFrame == null) {
            return;
        }
        String path = pathField.getText(), user = userField.getText(), password = passwordField.getText(),
                passwordConfirmation = passwordConfirmationField.getText();
        if (!password.equals(passwordConfirmation)) {
            errorArea.setForeground(Color.RED);
            errorArea.setText("Passwords don't match");
            return;
        }
        if (password.isEmpty()) {
            errorArea.setForeground(Color.RED);
            errorArea.setText("Specify a password");
            return;
        }
        String url = "jdbc:h2:" + path;
        try {
            DriverManager.getConnection(url, user, password).close();
            errorArea.setForeground(new Color(0, 0x99, 0));
            errorArea.setText("Database was created successfully.\n\n"
                    + "JDBC URL for H2 Console:\n"
                    + url);
        } catch (Exception ex) {
            errorArea.setForeground(Color.RED);
            errorArea.setText(ex.getMessage());
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public void actionPerformed(ActionEvent e) {
        String command = e.getActionCommand();
        if ("exit".equals(command)) {
            shutdown();
        } else if ("console".equals(command)) {
            startBrowser();
        } else if ("showCreate".equals(command)) {
            showCreateDatabase();
        } else if ("status".equals(command)) {
            showStatusWindow();
        } else {
            // for some reason, IKVM ignores setActionCommand
            if (startBrowser == e.getSource()) {
                startBrowser();
            } else if (createButton == e.getSource()) {
                createDatabase();
            }
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public void mouseClicked(MouseEvent e) {
        if (e.getButton() == MouseEvent.BUTTON1) {
            startBrowser();
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public void mouseEntered(MouseEvent e) {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void mouseExited(MouseEvent e) {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void mousePressed(MouseEvent e) {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void mouseReleased(MouseEvent e) {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void windowClosing(WindowEvent e) {
        if (trayIconUsed) {
            Window window = e.getWindow();
            if (window == statusFrame) {
                statusFrame.dispose();
                statusFrame = null;
            } else if (window == createFrame) {
                createFrame.dispose();
                createFrame = null;
            }
        } else {
            shutdown();
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public void windowActivated(WindowEvent e) {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void windowClosed(WindowEvent e) {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void windowDeactivated(WindowEvent e) {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void windowDeiconified(WindowEvent e) {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void windowIconified(WindowEvent e) {
        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void windowOpened(WindowEvent e) {
        // nothing to do
    }

}
