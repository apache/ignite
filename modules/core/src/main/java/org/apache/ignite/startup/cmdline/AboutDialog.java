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

package org.apache.ignite.startup.cmdline;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.imageio.ImageIO;
import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.KeyStroke;
import javax.swing.SwingUtilities;
import javax.swing.border.Border;

/**
 * About dialog.
 */
public class AboutDialog extends JDialog {
    /** */
    private static final long serialVersionUID = 0L;

    /** Border color. */
    private static final Color VALUE_BORDER_COLOR = new Color(0xcdcdcd);

    /** Global reference to about dialog to prevent double open. */
    private static AboutDialog aboutDlg;

    /** Application name */
    private final String appName;

    /** Banner icon url spec */
    private final String bannerSpec;

    /** Version. */
    private final String ver;

    /** Release date. */
    private final Date release;

    /** Copyright. */
    private final String copyright;

    /** Grid bag constraints. */
    private final GridBagConstraints gbc;

    /**
     * @param appName Application name.
     * @param bannerSpec Banner icon url spec.
     * @param ver Version.
     * @param release Release date.
     * @param copyright Copyright.
     */
    AboutDialog(String appName, String bannerSpec, String ver, Date release, String copyright) {
        this.appName = appName;

        this.bannerSpec = bannerSpec;

        this.ver = ver;
        this.release = release;
        this.copyright = copyright;

        gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.HORIZONTAL;

        initDialog();
    }

    /** Close action. */
    private Action closeAct = new AbstractAction("Close") {
        @Override public void actionPerformed(ActionEvent e) {
            assert SwingUtilities.isEventDispatchThread();

            dispose();
        }
    };

    /** Close button. */
    private JButton closeBtn = new JButton(closeAct);

    /**
     * Create and initialize dialog controls.
     *
     * @return Panel with dialog controls.
     */
    private JPanel initComponents() {
        JPanel content = new JPanel(new BorderLayout(0, 5));

        content.add(createBannerPanel(), BorderLayout.NORTH);
        content.add(createLicensePanel(), BorderLayout.CENTER);
        content.add(createButtonPanel(), BorderLayout.SOUTH);

        return content;
    }

    /**
     * @return Panel with banner.
     */
    private JPanel createBannerPanel() {
        JPanel bannerPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 5));

        bannerPanel.setBackground(Color.WHITE);

        try {
            URL url = new URL(bannerSpec);

            BufferedImage image = ImageIO.read(url);

            bannerPanel.add(new JLabel(new ImageIcon(image)));
        }
        catch (IOException ioe) {
            ioe.printStackTrace();
        }

        JPanel msgPanel = new JPanel();
        msgPanel.setLayout(new BoxLayout(msgPanel, BoxLayout.Y_AXIS));
        msgPanel.setBackground(Color.WHITE);

        msgPanel.add(new JLabel("<html><b>About Ignite</b></html>"));
        msgPanel.add(Box.createVerticalStrut(5));
        msgPanel.add(new JLabel(appName));

        bannerPanel.add(msgPanel);

        return bannerPanel;
    }

    /**
     * Creates strut.
     *
     * @return Grid bag constraints.
     */
    private GridBagConstraints gbcStrut() {
        gbc.gridx = 0;
        gbc.gridwidth = GridBagConstraints.REMAINDER;

        gbc.insets = new Insets(5, 0, 0, 0);

        return gbc;
    }

    /**
     * @return Grid bag constraints.
     */
    private GridBagConstraints gbcSeparator() {
        gbc.gridx = 0;
        gbc.gridwidth = GridBagConstraints.REMAINDER;

        gbc.insets = new Insets(5, 10, 10, 10);

        return gbc;
    }

    /**
     * License label.
     *
     * @return Grid bag constraints.
     */
    private GridBagConstraints gbcLicenseLabel() {
        gbc.gridx = 0;
        gbc.gridwidth = GridBagConstraints.RELATIVE;
        gbc.anchor = GridBagConstraints.WEST;

        gbc.insets = new Insets(0, 10, 5, 0);

        return gbc;
    }

    /**
     * License field.
     *
     * @return Grid bag constraints.
     */
    private GridBagConstraints gbcLicenseField() {
        gbc.gridx = 1;
        gbc.gridwidth = GridBagConstraints.RELATIVE;
        gbc.weightx = 400;

        gbc.insets = new Insets(0, 10, 5, 10);

        return gbc;
    }

    /**
     * @return Panel with controls to display license.
     */
    private JPanel createLicensePanel() {
        JPanel licPanel = new JPanel(new GridBagLayout());

        licPanel.add(Box.createVerticalGlue(), gbcStrut());

        addAboutItem(licPanel, "Version:", ver);
        addAboutItem(licPanel, "Release Date:", new SimpleDateFormat("dd MMM yyyy").format(release));
        addAboutItem(licPanel, "Copyright:", copyright);

        return licPanel;
    }

    /**
     * @return Panel with close button.
     */
    private JPanel createButtonPanel() {
        closeBtn.setAction(closeAct);
        closeBtn.setToolTipText("<html><b>Closes</b> Dialog</html>");

        JPanel btnPanel = new JPanel();
        btnPanel.add(closeBtn);

        return btnPanel;
    }

    /**
     * Initialize dialog.
     */
    private void initDialog() {
        setContentPane(initComponents());

        pack();

        setDefaultCloseOperation(DISPOSE_ON_CLOSE);

        getRootPane().setDefaultButton(closeBtn);
        setEscAction(closeAct);

        setModal(true);

        setResizable(false);
    }

    /**
     * Add name and value labels.
     *
     * @param c Component.
     * @param name Name.
     * @param val Value.
     */
    private void addAboutItem(JComponent c, String name, Object val) {
        addAboutItem(c, name, val, null);
    }

    /**
     * Add name and value labels.
     *
     * @param c Component.
     * @param name Name.
     * @param val Value.
     * @param border Border.
     */
    private void addAboutItem(JComponent c, String name, Object val, Border border) {
        String v = val != null ? val.toString() : "n/a";
        String tip = String.format("<html><b>%s</b> &#10159; %s</html>", name, val);

        JLabel lb = new JLabel(name);
        lb.setToolTipText(tip);

        JLabel field = new JLabel(v);
        field.setOpaque(true);
        field.setBackground(Color.WHITE);
        field.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createLineBorder(VALUE_BORDER_COLOR, 1),
            BorderFactory.createEmptyBorder(0, 3, 0, 3)));

        field.setToolTipText(tip);

        if (border != null)
            field.setBorder(border);

        c.add(lb, gbcLicenseLabel());
        c.add(field, gbcLicenseField());
    }

    /**
     * Registers ESC button click with given action.
     *
     * @param act Escape button action.
     */
    private void setEscAction(ActionListener act) {
        assert(act != null);

        getRootPane().registerKeyboardAction(act,
            KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0x0),
            JComponent.WHEN_IN_FOCUSED_WINDOW
        );
    }

    /**
     * Show about dialog.
     *
     * @param appName Application name.
     * @param bannerSpec Banner icon url spec.
     * @param ver Version number.
     * @param release Release date.
     * @param copyright Copyright blurb.
     */
    public static void centerShow(final String appName, final String bannerSpec,
        final String ver, final Date release, final String copyright) {
        SwingUtilities.invokeLater(new Runnable() {
            @SuppressWarnings("NonThreadSafeLazyInitialization")
            @Override public void run() {
                if (aboutDlg == null) {
                    try {
                        aboutDlg = new AboutDialog(appName, bannerSpec, ver, release, copyright);

                        aboutDlg.setLocationRelativeTo(null);
                        aboutDlg.setVisible(true);
                    }
                    finally {
                        aboutDlg = null;
                    }
                }
                else {
                    aboutDlg.setLocationRelativeTo(null);
                    aboutDlg.toFront();
                }
            }
        });
    }
}