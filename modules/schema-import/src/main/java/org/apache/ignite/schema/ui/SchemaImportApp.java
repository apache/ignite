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

package org.apache.ignite.schema.ui;

import javafx.application.*;
import javafx.beans.value.*;
import javafx.collections.*;
import javafx.concurrent.*;
import javafx.event.*;
import javafx.geometry.*;
import javafx.geometry.Insets;
import javafx.scene.*;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.*;
import javafx.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.schema.generator.*;
import org.apache.ignite.schema.model.*;
import org.apache.ignite.schema.parser.*;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.*;

import static javafx.embed.swing.SwingFXUtils.*;
import static org.apache.ignite.schema.ui.Controls.*;

/**
 * Schema Import utility application.
 */
@SuppressWarnings("UnnecessaryFullyQualifiedName")
public class SchemaImportApp extends Application {
    /** Logger. */
    private static final Logger log = Logger.getLogger(SchemaImportApp.class.getName());

    /** Presets for database settings. */
    private static class Preset {
        /** Name in preferences. */
        private String pref;

        /** RDBMS name to show on screen. */
        private String name;

        /** Path to JDBC driver jar. */
        private String jar;

        /** JDBC driver class name. */
        private String drv;

        /** JDBC URL. */
        private String url;

        /** User name. */
        private String user;

        /**
         * Preset constructor.
         *
         * @param pref Name in preferences.
         * @param name RDBMS name to show on screen.
         * @param jar Path to JDBC driver jar..
         * @param drv JDBC driver class name.
         * @param url JDBC URL.
         * @param user User name.
         */
        Preset(String pref, String name, String jar, String drv, String url, String user) {
            this.pref = pref;
            this.name = name;
            this.jar = jar;
            this.drv = drv;
            this.url = url;
            this.user = user;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return name;
        }
    }

    /** Default presets for popular databases. */
    private final Preset[] presets = {
        new Preset("h2", "H2 Database", "h2.jar", "org.h2.Driver", "jdbc:h2:[database]", "sa"),
        new Preset("db2", "DB2", "db2jcc4.jar", "com.ibm.db2.jcc.DB2Driver", "jdbc:db2://[host]:[port]/[database]",
            "db2admin"),
        new Preset("oracle", "Oracle", "ojdbc6.jar", "oracle.jdbc.OracleDriver",
            "jdbc:oracle:thin:@[host]:[port]:[database]", "system"),
        new Preset("mysql", "MySQL", "mysql-connector-java-5-bin.jar", "com.mysql.jdbc.Driver",
            "jdbc:mysql://[host]:[port]/[database]", "root"),
        new Preset("mssql", "Microsoft SQL Server", "sqljdbc41.jar", "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "jdbc:sqlserver://[host]:[port][;databaseName=database]", "sa"),
        new Preset("postgresql", "PostgreSQL", "postgresql-9.3.jdbc4.jar", "org.postgresql.Driver",
            "jdbc:postgresql://[host]:[port]/[database]", "sa"),
        new Preset("custom", "Custom server...", "custom-jdbc.jar", "org.custom.Driver", "jdbc:custom", "sa")
    };

    /** */
    private static final String PREF_WINDOW_X = "window.x";
    /** */
    private static final String PREF_WINDOW_Y = "window.y";
    /** */
    private static final String PREF_WINDOW_WIDTH = "window.width";
    /** */
    private static final String PREF_WINDOW_HEIGHT = "window.height";

    /** */
    private static final String PREF_JDBC_DB_PRESET = "jdbc.db.preset";
    /** */
    private static final String PREF_JDBC_DRIVER_JAR = "jdbc.driver.jar";
    /** */
    private static final String PREF_JDBC_DRIVER_CLASS = "jdbc.driver.class";
    /** */
    private static final String PREF_JDBC_URL = "jdbc.url";
    /** */
    private static final String PREF_JDBC_USER = "jdbc.user";

    /** */
    private static final String PREF_OUT_FOLDER = "out.folder";

    /** */
    private static final String PREF_POJO_PACKAGE = "pojo.package";
    /** */
    private static final String PREF_POJO_INCLUDE = "pojo.include";
    /** */
    private static final String PREF_POJO_CONSTRUCTOR = "pojo.constructor";

    /** */
    private static final String PREF_XML_SINGLE = "xml.single";

    /** */
    private static final String PREF_NAMING_PATTERN = "naming.pattern";
    /** */
    private static final String PREF_NAMING_REPLACE = "naming.replace";

    /** */
    private Stage owner;

    /** */
    private BorderPane rootPane;

    /** Header pane. */
    private BorderPane hdrPane;

    /** */
    private HBox dbIcon;

    /** */
    private HBox genIcon;

    /** */
    private Label titleLb;

    /** */
    private Label subTitleLb;

    /** */
    private Button prevBtn;

    /** */
    private Button nextBtn;

    /** */
    private ComboBox<Preset> rdbmsCb;

    /** */
    private TextField jdbcDrvJarTf;

    /** */
    private TextField jdbcDrvClsTf;

    /** */
    private TextField jdbcUrlTf;

    /** */
    private TextField userTf;

    /** */
    private PasswordField pwdTf;

    /** */
    private ComboBox<String> parseCb;

    /** */
    private GridPaneEx connPnl;

    /** */
    private StackPane connLayerPnl;

    /** */
    private TableView<PojoDescriptor> pojosTbl;

    /** */
    private TableView<PojoField> fieldsTbl;

    /** */
    private Node curTbl;

    /** */
    private TextField outFolderTf;

    /** */
    private TextField pkgTf;

    /** */
    private CheckBox pojoConstructorCh;

    /** */
    private CheckBox pojoIncludeKeysCh;

    /** */
    private CheckBox xmlSingleFileCh;

    /** */
    private TextField regexTf;

    /** */
    private TextField replaceTf;

    /** */
    private GridPaneEx genPnl;

    /** */
    private StackPane genLayerPnl;

    /** */
    private ProgressIndicator pi;

    /** List with POJOs descriptors. */
    private ObservableList<PojoDescriptor> pojos = FXCollections.emptyObservableList();

    /** Currently selected POJO. */
    private PojoDescriptor curPojo;

    /** */
    private final Map<String, Driver> drivers = new HashMap<>();

    /** Application preferences. */
    private final Properties prefs = new Properties();

    /** File path for storing on local file system. */
    private final File prefsFile = new File(System.getProperty("user.home"), ".ignite-schema-import");

    /** Empty POJO fields model. */
    private static final ObservableList<PojoField> NO_FIELDS = FXCollections.emptyObservableList();

    /** */
    private final ExecutorService exec = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "ignite-schema-import-worker");

            t.setDaemon(true);

            return t;
        }
    });

    /**
     * Lock UI before start long task.
     *
     * @param layer Stack pane to add progress indicator.
     * @param controls Controls to disable.
     */
    private void lockUI(StackPane layer, Node... controls) {
        for (Node control : controls)
            control.setDisable(true);

        layer.getChildren().add(pi);
    }

    /**
     * Unlock UI after long task finished.
     *
     * @param layer Stack pane to remove progress indicator.
     * @param controls Controls to enable.
     */
    private void unlockUI(StackPane layer, Node... controls) {
        for (Node control : controls)
            control.setDisable(false);

        layer.getChildren().remove(pi);
    }

    /**
     * Perceptual delay to avoid UI flickering.
     *
     * @param started Time when background progress started.
     */
    private void perceptualDelay(long started) {
        long delta = System.currentTimeMillis() - started;

        if (delta < 500)
            try {
                Thread.sleep(500 - delta);
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
    }

    /**
     * Fill tree with database metadata.
     */
    private void fill() {
        lockUI(connLayerPnl, connPnl, nextBtn);

        final String jdbcDrvJarPath = jdbcDrvJarTf.getText().trim();

        final String jdbcDrvCls = jdbcDrvClsTf.getText();

        final String jdbcUrl = jdbcUrlTf.getText();

        String user = userTf.getText().trim();

        String pwd = pwdTf.getText().trim();

        final Properties jdbcInfo = new Properties();

        if (!user.isEmpty())
            jdbcInfo.put("user", user);

        if (!pwd.isEmpty())
            jdbcInfo.put("password", pwd);

        final boolean tblsOnly = parseCb.getSelectionModel().getSelectedIndex() == 0;

        Runnable task = new Task<Void>() {
            /** {@inheritDoc} */
            @Override protected Void call() throws Exception {
                long started = System.currentTimeMillis();

                try (Connection conn = connect(jdbcDrvJarPath, jdbcDrvCls, jdbcUrl, jdbcInfo)) {
                    pojos = DatabaseMetadataParser.parse(conn, tblsOnly);
                }

                perceptualDelay(started);

                return null;
            }

            /** {@inheritDoc} */
            @Override protected void succeeded() {
                try {
                    super.succeeded();

                    pojosTbl.setItems(pojos);

                    if (pojos.isEmpty()) {
                        MessageBox.warningDialog(owner, "No tables found in database. Recheck JDBC URL.\n" +
                            "JDBC URL: " +  jdbcUrl);

                        return;
                    }
                    else
                        pojosTbl.getSelectionModel().clearAndSelect(0);

                    curTbl = pojosTbl;

                    pojosTbl.requestFocus();


                    hdrPane.setLeft(genIcon);

                    titleLb.setText("Generate XML And POJOs");
                    subTitleLb.setText(jdbcUrlTf.getText());

                    rootPane.setCenter(genLayerPnl);

                    prevBtn.setDisable(false);
                    nextBtn.setText("Generate");
                    tooltip(nextBtn, "Generate XML and POJO files");
                }
                finally {
                    unlockUI(connLayerPnl, connPnl, nextBtn);
                }
            }

            /** {@inheritDoc} */
            @Override protected void cancelled() {
                super.cancelled();

                unlockUI(connLayerPnl, connPnl, nextBtn);
            }

            /** {@inheritDoc} */
            @Override protected void failed() {
                super.succeeded();

                unlockUI(connLayerPnl, connPnl, nextBtn);

                MessageBox.errorDialog(owner, "Failed to get tables list from database.", getException());
            }
        };

        exec.submit(task);
    }

    /**
     * Generate XML and POJOs.
     */
    private void generate() {
        final Collection<PojoDescriptor> selPojos = checkedPojos();

        if (selPojos.isEmpty()) {
            MessageBox.warningDialog(owner, "Please select tables to generate XML and POJOs files!");

            return;
        }

        if (checkInput(outFolderTf, true, "Output folder should not be empty!"))
            return;

        lockUI(genLayerPnl, genPnl, prevBtn, nextBtn);

        final String outFolder = outFolderTf.getText();

        final String pkg = pkgTf.getText();

        final File destFolder = new File(outFolder);

        final boolean constructor = pojoConstructorCh.isSelected();

        final boolean includeKeys = pojoIncludeKeysCh.isSelected();

        final boolean singleXml = xmlSingleFileCh.isSelected();

        Runnable task = new Task<Void>() {
            /**
             * @param pojo POJO descriptor to check.
             * @param selected Selected flag.
             * @param msg Message to show in case of check failed.
             */
            private void checkEmpty(final PojoDescriptor pojo, boolean selected, String msg) {
                if (!selected) {
                    Platform.runLater(new Runnable() {
                        @Override public void run() {
                            TableView.TableViewSelectionModel<PojoDescriptor> selMdl = pojosTbl.getSelectionModel();

                            selMdl.clearSelection();
                            selMdl.select(pojo);
                            pojosTbl.scrollTo(selMdl.getSelectedIndex());
                        }
                    });

                    throw new IllegalStateException(msg + pojo.table());
                }
            }

            /** {@inheritDoc} */
            @Override protected Void call() throws Exception {
                long started = System.currentTimeMillis();

                if (!destFolder.exists() && !destFolder.mkdirs())
                    throw new IOException("Failed to create output folder: " + destFolder);

                Collection<PojoDescriptor> all = new ArrayList<>();

                ConfirmCallable askOverwrite = new ConfirmCallable(owner, "File already exists: %s\nOverwrite?");

                // Generate XML and POJO.
                for (PojoDescriptor pojo : selPojos) {
                    if (pojo.checked()) {
                        checkEmpty(pojo, pojo.hasFields(), "No fields selected for type: ");
                        checkEmpty(pojo, pojo.hasKeyFields(), "No key fields selected for type: ");
                        checkEmpty(pojo, pojo.hasValueFields(includeKeys), "No value fields selected for type: ");

                        all.add(pojo);
                    }
                }

                for (PojoDescriptor pojo : all) {
                    if (!singleXml)
                        XmlGenerator.generate(pkg, pojo, includeKeys, new File(destFolder, pojo.table() + ".xml"),
                            askOverwrite);

                    CodeGenerator.pojos(pojo, outFolder, pkg, constructor, includeKeys, askOverwrite);
                }

                if (singleXml)
                    XmlGenerator.generate(pkg, all, includeKeys, new File(outFolder, "ignite-type-metadata.xml"), askOverwrite);

                CodeGenerator.snippet(all, pkg, includeKeys, outFolder, askOverwrite);

                perceptualDelay(started);

                return null;
            }

            /** {@inheritDoc} */
            @Override protected void succeeded() {
                super.succeeded();

                unlockUI(genLayerPnl, genPnl, prevBtn, nextBtn);

                if (MessageBox.confirmDialog(owner, "Generation complete!\n\n" +
                    "Reveal output folder in system default file browser?"))
                    try {
                        java.awt.Desktop.getDesktop().open(destFolder);
                    }
                    catch (IOException e) {
                        MessageBox.errorDialog(owner, "Failed to open folder with results.", e);
                    }
            }

            /** {@inheritDoc} */
            @Override protected void cancelled() {
                super.cancelled();

                unlockUI(genLayerPnl, genPnl, prevBtn, nextBtn);

                MessageBox.warningDialog(owner, "Generation canceled.");
            }

            /** {@inheritDoc} */
            @Override protected void failed() {
                super.succeeded();

                unlockUI(genLayerPnl, genPnl, prevBtn, nextBtn);

                MessageBox.errorDialog(owner, "Generation failed.", getException());
            }
        };

        exec.submit(task);
    }

    /**
     * @return Header pane with title label.
     */
    private BorderPane createHeaderPane() {
        dbIcon = hBox(0, true, imageView("data_connection", 48));
        genIcon = hBox(0, true, imageView("text_tree", 48));

        titleLb = label("");
        titleLb.setId("banner");

        subTitleLb = label("");

        BorderPane bp = borderPane(null, vBox(5, titleLb, subTitleLb), null, dbIcon, hBox(0, true, imageView("ignite", 48)));
        bp.setId("banner");

        return bp;
    }

    /**
     * @return Panel with control buttons.
     */
    private Pane createButtonsPane() {
        prevBtn = button("Prev", "Go to \"Database connection\" page", new EventHandler<ActionEvent>() {
            @Override public void handle(ActionEvent evt) {
                prev();
            }
        });

        nextBtn = button("Next", "Go to \"POJO and XML generation\" page", new EventHandler<ActionEvent>() {
            @Override public void handle(ActionEvent evt) {
                next();
            }
        });

        return buttonsPane(Pos.BOTTOM_RIGHT, true, prevBtn, nextBtn);
    }

    /**
     * @return {@code true} if some changes were made to fields metadata.
     */
    private boolean changed() {
        for (PojoDescriptor pojo : pojos)
            if (pojo.changed())
                return true;

        return false;
    }

    /**
     * Go to &quot;Connect To Database&quot; panel.
     */
    private void prev() {
        if (changed() && !MessageBox.confirmDialog(owner, "Are you sure you want to return to previous page?\n" +
            "This will discard all your changes."))
            return;

        hdrPane.setLeft(dbIcon);

        titleLb.setText("Connect To Database");
        subTitleLb.setText("Specify database connection properties...");

        rootPane.setCenter(connLayerPnl);

        prevBtn.setDisable(true);
        nextBtn.setText("Next");
        tooltip(nextBtn, "Go to \"XML and POJO generation\" page");
    }

    /**
     * Check that text field is non empty.
     *
     * @param tf Text field check.
     * @param trim If {@code true} then
     * @param msg Warning message.
     * @return {@code true} If text field is empty.
     */
    private boolean checkInput(TextField tf, boolean trim, String msg) {
        String s = tf.getText();

        s = trim ? s.trim() : s;

        if (s.isEmpty()) {
            tf.requestFocus();

            MessageBox.warningDialog(owner, msg);

            return true;
        }

        return false;
    }

    /**
     * Go to &quot;Generate XML And POJOs&quot; panel or generate XML and POJOs.
     */
    private void next() {
        if (rootPane.getCenter() == connLayerPnl) {
            if (checkInput(jdbcDrvJarTf, true, "Path to JDBC driver is not specified!") ||
                checkInput(jdbcDrvClsTf, true, "JDBC driver class name is not specified!") ||
                checkInput(jdbcUrlTf, true, "JDBC URL connection string is not specified!") ||
                checkInput(userTf, true, "User name is not specified!"))
                return;

            fill();
        }
        else
            generate();
    }

    /**
     * Connect to database.
     *
     * @param jdbcDrvJarPath Path to JDBC driver.
     * @param jdbcDrvCls JDBC class name.
     * @param jdbcUrl JDBC connection URL.
     * @param jdbcInfo Connection properties.
     * @return Connection to database.
     * @throws SQLException if connection failed.
     */
    private Connection connect(String jdbcDrvJarPath, String jdbcDrvCls, String jdbcUrl, Properties jdbcInfo)
        throws SQLException {
        Driver drv = drivers.get(jdbcDrvCls);

        if (drv == null) {
            if (jdbcDrvJarPath.isEmpty())
                throw new IllegalStateException("Driver jar file name is not specified.");

            File drvJar = new File(jdbcDrvJarPath);

            if (!drvJar.exists())
                throw new IllegalStateException("Driver jar file is not found.");

            try {
                URL u = new URL("jar:" + drvJar.toURI() + "!/");

                URLClassLoader ucl = URLClassLoader.newInstance(new URL[] {u});

                drv = (Driver)Class.forName(jdbcDrvCls, true, ucl).newInstance();

                drivers.put(jdbcDrvCls, drv);
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        Connection conn = drv.connect(jdbcUrl, jdbcInfo);

        if (conn == null)
            throw new IllegalStateException("Connection was not established (JDBC driver returned null value).");

        return conn;
    }

    /**
     * Create connection pane with controls.
     *
     * @return Pane with connection controls.
     */
    private Pane createConnectionPane() {
        connPnl = paneEx(10, 10, 0, 10);

        connPnl.addColumn();
        connPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        connPnl.addColumn(35, 35, 35, Priority.NEVER);

        connPnl.add(text("This utility is designed to automatically generate configuration XML files and" +
            " POJO classes from database schema information.", 550), 3);

        connPnl.wrap();

        GridPaneEx presetPnl = paneEx(0, 0, 0, 0);
        presetPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        presetPnl.addColumn();

        rdbmsCb = presetPnl.add(comboBox("Select database server to get predefined settings", presets));

        presetPnl.add(button("Save preset", "Save current settings in preferences", new EventHandler<ActionEvent>() {
            @Override public void handle(ActionEvent evt) {
                Preset preset = rdbmsCb.getSelectionModel().getSelectedItem();

                savePreset(preset);
            }
        }));

        connPnl.add(label("DB Preset:"));
        connPnl.add(presetPnl, 2);

        jdbcDrvJarTf = connPnl.addLabeled("Driver JAR:", textField("Path to driver jar"));

        connPnl.add(button("...", "Select JDBC driver jar or zip", new EventHandler<ActionEvent>() {
            /** {@inheritDoc} */
            @Override public void handle(ActionEvent evt) {
                FileChooser fc = new FileChooser();

                try {
                    File jarFolder = new File(jdbcDrvJarTf.getText()).getParentFile();

                    if (jarFolder.exists())
                        fc.setInitialDirectory(jarFolder);
                }
                catch (Exception ignored) {
                    // No-op.
                }

                jdbcDrvJarTf.getText();

                fc.getExtensionFilters().addAll(
                    new FileChooser.ExtensionFilter("JDBC Drivers (*.jar)", "*.jar"),
                    new FileChooser.ExtensionFilter("ZIP archives (*.zip)", "*.zip"));

                File drvJar = fc.showOpenDialog(owner);

                if (drvJar != null)
                    jdbcDrvJarTf.setText(drvJar.getAbsolutePath());
            }
        }));

        jdbcDrvClsTf = connPnl.addLabeled("JDBC Driver:", textField("Enter class name for JDBC driver"), 2);

        jdbcUrlTf = connPnl.addLabeled("JDBC URL:", textField("JDBC URL of the database connection string"), 2);

        rdbmsCb.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<Preset>() {
            @Override public void changed(ObservableValue<? extends Preset> val, Preset oldVal, Preset newVal) {
                jdbcDrvJarTf.setText(newVal.jar);
                jdbcDrvClsTf.setText(newVal.drv);
                jdbcUrlTf.setText(newVal.url);
                userTf.setText(newVal.user);
            }
        });

        userTf = connPnl.addLabeled("User:", textField("User name"), 2);

        pwdTf = connPnl.addLabeled("Password:", passwordField("User password"), 2);

        parseCb = connPnl.addLabeled("Parse:", comboBox("Type of tables to parse", "Tables only", "Tables and Views"), 2);

        connLayerPnl = stackPane(connPnl);

        return connLayerPnl;
    }

    /**
     * Check if new class name is unique.
     *
     * @param pojo Current edited POJO.
     * @param newVal New value for class name.
     * @param key {@code true} if key class name is checked.
     * @return {@code true} if class name is valid.
     */
    private boolean checkClassNameUnique(PojoDescriptor pojo, String newVal, boolean key) {
        for (PojoDescriptor otherPojo : pojos)
            if (pojo != otherPojo) {
                String otherKeyCls = otherPojo.keyClassName();
                String otherValCls = otherPojo.valueClassName();

                if (newVal.equals(otherKeyCls) || newVal.equals(otherValCls)) {
                    MessageBox.warningDialog(owner, (key ? "Key" : "Value") + " class name must be unique!");

                    return false;
                }
            }

        return true;
    }

    /**
     * Check if new class name is valid.
     *
     * @param pojo Current edited POJO.
     * @param newVal New value for class name.
     * @param key {@code true} if key class name is checked.
     * @return {@code true} if class name is valid.
     */
    private boolean checkClassName(PojoDescriptor pojo, String newVal, boolean key) {
        if (newVal.trim().isEmpty()) {
            MessageBox.warningDialog(owner, (key ? "Key" : "Value") + " class name must be non empty!");

            return false;
        }

        if (key) {
            if (newVal.equals(pojo.valueClassName())) {
                MessageBox.warningDialog(owner, "Key class name must be different from value class name!");

                return false;
            }
        }
        else if (newVal.equals(pojo.keyClassName())) {
            MessageBox.warningDialog(owner, "Value class name must be different from key class name!");

            return false;
        }

        return checkClassNameUnique(pojo, newVal, key);
    }

    /**
     * Create generate pane with controls.
     */
    private void createGeneratePane() {
        genPnl = paneEx(10, 10, 0, 10);

        genPnl.addColumn();
        genPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        genPnl.addColumn(35, 35, 35, Priority.NEVER);

        genPnl.addRow(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        genPnl.addRows(7);

        TableColumn<PojoDescriptor, Boolean> useCol = customColumn("Schema / Table", "use",
            "If checked then this table will be used for XML and POJOs generation", PojoDescriptorCell.cellFactory());

        TableColumn<PojoDescriptor, String> keyClsCol = textColumn("Key Class Name", "keyClassName", "Key class name",
            new TextColumnValidator<PojoDescriptor>() {
                @Override public boolean valid(PojoDescriptor rowVal, String newVal) {
                    boolean valid = checkClassName(rowVal, newVal, true);

                    if (valid)
                        rowVal.keyClassName(newVal);

                    return valid;
                }
            });

        TableColumn<PojoDescriptor, String> valClsCol = textColumn("Value Class Name", "valueClassName", "Value class name",
            new TextColumnValidator<PojoDescriptor>() {
                @Override public boolean valid(PojoDescriptor rowVal, String newVal) {
                    boolean valid = checkClassName(rowVal, newVal, false);

                    if (valid)
                        rowVal.valueClassName(newVal);

                    return valid;
                }
            });

        pojosTbl = tableView("Tables not found in database", useCol, keyClsCol, valClsCol);

        TableColumn<PojoField, Boolean> useFldCol = customColumn("Use", "use",
            "Check to use this field for XML and POJO generation\n" +
            "Note that NOT NULL columns cannot be unchecked", PojoFieldUseCell.cellFactory());
        useFldCol.setMinWidth(50);
        useFldCol.setMaxWidth(50);

        TableColumn<PojoField, Boolean> keyCol = booleanColumn("Key", "key",
            "Check to include this field into key object");

        TableColumn<PojoField, Boolean> akCol = booleanColumn("AK", "affinityKey",
            "Check to annotate key filed with @AffinityKeyMapped annotation in generated POJO class\n" +
            "Note that a class can have only ONE key field annotated with @AffinityKeyMapped annotation");

        TableColumn<PojoField, String> dbNameCol = tableColumn("DB Name", "dbName", "Field name in database");

        TableColumn<PojoField, String> dbTypeNameCol = tableColumn("DB Type", "dbTypeName", "Field type in database");

        TableColumn<PojoField, String> javaNameCol = textColumn("Java Name", "javaName", "Field name in POJO class",
            new TextColumnValidator<PojoField>() {
                @Override public boolean valid(PojoField rowVal, String newVal) {
                    if (newVal.trim().isEmpty()) {
                        MessageBox.warningDialog(owner, "Java name must be non empty!");

                        return false;
                    }

                    for (PojoField field : curPojo.fields())
                        if (rowVal != field && newVal.equals(field.javaName())) {
                            MessageBox.warningDialog(owner, "Java name must be unique!");

                            return false;
                        }

                    rowVal.javaName(newVal);

                    return true;
                }
            });

        TableColumn<PojoField, String> javaTypeNameCol = customColumn("Java Type", "javaTypeName",
            "Field java type in POJO class", JavaTypeCell.cellFactory());

        fieldsTbl = tableView("Select table to see table columns",
            useFldCol, keyCol, akCol, dbNameCol, dbTypeNameCol, javaNameCol, javaTypeNameCol);

        genPnl.add(splitPane(pojosTbl, fieldsTbl, 0.6), 3);

        final GridPaneEx keyValPnl = paneEx(0, 0, 0, 0);
        keyValPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        keyValPnl.addColumn();
        keyValPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        keyValPnl.addColumn();

        pkgTf = genPnl.addLabeled("Package:", textField("Package that will be used for POJOs generation"), 2);

        outFolderTf = genPnl.addLabeled("Output Folder:", textField("Output folder for XML and POJOs files"));

        genPnl.add(button("...", "Select output folder", new EventHandler<ActionEvent>() {
            @Override public void handle(ActionEvent evt) {
                DirectoryChooser dc = new DirectoryChooser();

                try {
                    File outFolder = new File(outFolderTf.getText());

                    if (outFolder.exists())
                        dc.setInitialDirectory(outFolder);
                }
                catch (Exception ignored) {
                    // No-op.
                }

                File folder = dc.showDialog(owner);

                if (folder != null)
                    outFolderTf.setText(folder.getAbsolutePath());
            }
        }));

        pojoIncludeKeysCh = genPnl.add(checkBox("Include key fields into value POJOs",
            "If selected then include key fields into value object", true), 3);

        pojoConstructorCh = genPnl.add(checkBox("Generate constructors for POJOs",
            "If selected then generate empty and full constructors for POJOs", false), 3);

        xmlSingleFileCh = genPnl.add(checkBox("Write all configurations to a single XML file",
            "If selected then all configurations will be saved into the file 'ignite-type-metadata.xml'", true), 3);

        GridPaneEx regexPnl = paneEx(5, 5, 5, 5);
        regexPnl.addColumn();
        regexPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        regexPnl.addColumn();
        regexPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);

        regexTf = regexPnl.addLabeled("  Regexp:", textField("Regular expression. For example: (\\w+)"));

        replaceTf = regexPnl.addLabeled("  Replace with:", textField("Replace text. For example: $1_SomeText"));

        final ComboBox<String> replaceCb = regexPnl.addLabeled("  Replace:", comboBox("Replacement target",
            "Key class names", "Value class names", "Java names"));

        regexPnl.add(buttonsPane(Pos.CENTER_LEFT, false,
            button("Rename Selected", "Replaces each substring of this string that matches the given regular expression" +
                    " with the given replacement",
                new EventHandler<ActionEvent>() {
                    @Override public void handle(ActionEvent evt) {
                        if (checkInput(regexTf, false, "Regular expression should not be empty!"))
                            return;

                        String sel = replaceCb.getSelectionModel().getSelectedItem();

                        boolean isFields = "Java names".equals(sel) && curTbl == fieldsTbl;

                        String src = isFields ? "fields" : "tables";

                        String target = "\"" + sel + "\"";

                        Collection<PojoDescriptor> selPojos = pojosTbl.getSelectionModel().getSelectedItems();

                        Collection<PojoField> selFields = fieldsTbl.getSelectionModel().getSelectedItems();

                        boolean isEmpty = isFields ? selFields.isEmpty() : selPojos.isEmpty();

                        if (isEmpty) {
                            MessageBox.warningDialog(owner, "Please select " + src + " to rename " + target + "!");

                            return;
                        }

                        if (!MessageBox.confirmDialog(owner, "Are you sure you want to rename " + target +
                            " for all selected " + src + "?"))
                            return;

                        String regex = regexTf.getText();

                        String replace = replaceTf.getText();

                        try {
                            switch (replaceCb.getSelectionModel().getSelectedIndex()) {
                                case 0:
                                    renameKeyClassNames(selPojos, regex, replace);
                                    break;

                                case 1:
                                    renameValueClassNames(selPojos, regex, replace);
                                    break;

                                default:
                                    if (isFields)
                                        renameFieldsJavaNames(selFields, regex, replace);
                                    else
                                        renamePojosJavaNames(selPojos, regex, replace);
                            }
                        }
                        catch (Exception e) {
                            MessageBox.errorDialog(owner, "Failed to rename " + target + "!", e);
                        }
                    }
                }),
            button("Reset Selected", "Revert changes for selected items to initial auto-generated values", new EventHandler<ActionEvent>() {
                @Override public void handle(ActionEvent evt) {
                    String sel = replaceCb.getSelectionModel().getSelectedItem();

                    boolean isFields = "Java names".equals(sel) && curTbl == fieldsTbl;

                    String src = isFields ? "fields" : "tables";

                    String target = "\"" + sel + "\"";

                    Collection<PojoDescriptor> selPojos = pojosTbl.getSelectionModel().getSelectedItems();

                    Collection<PojoField> selFields = fieldsTbl.getSelectionModel().getSelectedItems();

                    boolean isEmpty = isFields ? selFields.isEmpty() : selPojos.isEmpty();

                    if (isEmpty) {
                        MessageBox.warningDialog(owner, "Please select " + src + "to revert " + target + "!");

                        return;
                    }

                    if (!MessageBox.confirmDialog(owner,
                        "Are you sure you want to revert " + target + " for all selected " + src + "?"))
                        return;

                    switch (replaceCb.getSelectionModel().getSelectedIndex()) {
                        case 0:
                            revertKeyClassNames(selPojos);
                            break;

                        case 1:
                            revertValueClassNames(selPojos);
                            break;

                        default:
                            if (isFields)
                                revertFieldsJavaNames(selFields);
                            else
                                revertPojosJavaNames(selPojos);
                    }
                }
            })
        ), 2).setPadding(new Insets(0, 0, 0, 10));

        pojosTbl.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<PojoDescriptor>() {
            @Override public void changed(ObservableValue<? extends PojoDescriptor> val,
                PojoDescriptor oldVal, PojoDescriptor newItem) {
                if (newItem != null && newItem.parent() != null) {
                    curPojo = newItem;

                    fieldsTbl.setItems(curPojo.fields());
                    fieldsTbl.getSelectionModel().clearSelection();

                    keyValPnl.setDisable(false);
                }
                else {
                    curPojo = null;
                    fieldsTbl.setItems(NO_FIELDS);

                    keyValPnl.setDisable(true);
                }
            }
        });

        pojosTbl.focusedProperty().addListener(new ChangeListener<Boolean>() {
            @Override public void changed(ObservableValue<? extends Boolean> val, Boolean oldVal, Boolean newVal) {
                if (newVal)
                    curTbl = pojosTbl;
            }
        });

        fieldsTbl.getSelectionModel().selectedIndexProperty().addListener(new ChangeListener<Number>() {
            @Override public void changed(ObservableValue<? extends Number> val, Number oldVal, Number newVal) {
                if (curPojo != null) {
                    TableView.TableViewSelectionModel<PojoDescriptor> selMdl = pojosTbl.getSelectionModel();

                    List<Integer> selIndices = new ArrayList<>(selMdl.getSelectedIndices());

                    if (selIndices.size() > 1) {
                        for (Integer idx : selIndices) {
                            if (pojos.get(idx) != curPojo)
                                selMdl.clearSelection(idx);
                        }
                    }
                }
            }
        });

        fieldsTbl.focusedProperty().addListener(new ChangeListener<Boolean>() {
            @Override public void changed(ObservableValue<? extends Boolean> val, Boolean oldVal, Boolean newVal) {
                if (newVal)
                    curTbl = fieldsTbl;
            }
        });

        genPnl.add(titledPane("Rename \"Key class name\", \"Value class name\" or  \"Java name\" for selected tables",
            regexPnl), 3);

        genLayerPnl = stackPane(genPnl);
    }

    /**
     * Rename key class name for selected POJOs.
     *
     * @param selPojos Selected POJOs to rename.
     * @param regex Regex to search.
     * @param replace Text for replacement.
     */
    private void renameKeyClassNames(Collection<PojoDescriptor> selPojos, String regex, String replace) {
        for (PojoDescriptor pojo : selPojos)
            pojo.keyClassName(pojo.keyClassName().replaceAll(regex, replace));
    }

    /**
     * Rename value class name for selected POJOs.
     *
     * @param selPojos Selected POJOs to rename.
     * @param regex Regex to search.
     * @param replace Text for replacement.
     */
    private void renameValueClassNames(Collection<PojoDescriptor> selPojos, String regex, String replace) {
        for (PojoDescriptor pojo : selPojos)
            pojo.valueClassName(pojo.valueClassName().replaceAll(regex, replace));
    }

    /**
     * Rename fields java name for selected POJOs.
     *
     * @param selPojos Selected POJOs to rename.
     * @param regex Regex to search.
     * @param replace Text for replacement.
     */
    private void renamePojosJavaNames(Collection<PojoDescriptor> selPojos, String regex, String replace) {
        for (PojoDescriptor pojo : selPojos)
            for (PojoField field : pojo.fields())
                field.javaName(field.javaName().replaceAll(regex, replace));
    }

    /**
     * Rename fields java name for current POJO.
     *
     * @param selFields Selected fields for current POJO to rename.
     * @param regex Regex to search.
     * @param replace Text for replacement.
     */
    private void renameFieldsJavaNames(Collection<PojoField> selFields, String regex, String replace) {
        for (PojoField field : selFields)
            field.javaName(field.javaName().replaceAll(regex, replace));
    }

    /**
     * Revert key class name for selected POJOs to initial value.
     *
     * @param selPojos Selected POJOs to revert.
     */
    private void revertKeyClassNames(Collection<PojoDescriptor> selPojos) {
        for (PojoDescriptor pojo : selPojos)
            pojo.revertKeyClassName();
    }

    /**
     * Revert value class name for selected POJOs to initial value.
     *
     * @param selPojos Selected POJOs to revert.
     */
    private void revertValueClassNames(Collection<PojoDescriptor> selPojos) {
        for (PojoDescriptor pojo : selPojos)
            pojo.revertValueClassName();
    }

    /**
     * Revert fields java name for selected POJOs to initial value.
     *
     * @param selPojos Selected POJOs to revert.
     */
    private void revertPojosJavaNames(Collection<PojoDescriptor> selPojos) {
        for (PojoDescriptor pojo : selPojos)
            pojo.revertJavaNames();
    }

    /**
     * Revert fields java name for current POJO to initial value.
     *
     * @param selFields Selected POJO fields to revert.
     */
    private void revertFieldsJavaNames(Collection<PojoField> selFields) {
        for (PojoField field : selFields)
            field.resetJavaName();
    }

    /**
     * @return POJOs checked in table-tree-view.
     */
    private Collection<PojoDescriptor> checkedPojos() {
        Collection<PojoDescriptor> res = new ArrayList<>();

        for (PojoDescriptor pojo : pojos)
            if (pojo.checked())
                res.add(pojo);

        return res;
    }

    /**
     * Gets string property.
     *
     * @param key Property key.
     * @param dflt Default value.
     * @return Property value as string.
     */
    private String getStringProp(String key, String dflt) {
        String val = prefs.getProperty(key);

        if (val != null)
            return val;

        return dflt;
    }

    /**
     * Sets string property.
     *
     * @param key Property key.
     * @param val Value to set.
     */
    private void setStringProp(String key, String val) {
        prefs.put(key, val);
    }

    /**
     * Gets int property.
     *
     * @param key Property key.
     * @param dflt Default value.
     * @return Property value as int.
     */
    private int getIntProp(String key, int dflt) {
        String val = prefs.getProperty(key);

        if (val != null)
            try {
                return Integer.parseInt(val);
            }
            catch (NumberFormatException ignored) {
                return dflt;
            }

        return dflt;
    }

    /**
     * Sets int property.
     *
     * @param key Property key.
     * @param val Value to set.
     */
    private void setIntProp(String key, int val) {
        prefs.put(key, String.valueOf(val));
    }

    /**
     * Gets boolean property.
     *
     * @param key Property key.
     * @param dflt Default value.
     * @return Property value as boolean.
     */
    private boolean getBoolProp(String key, boolean dflt) {
        String val = prefs.getProperty(key);

        if (val != null)
            return Boolean.parseBoolean(val);

        return dflt;
    }

    /**
     * Sets boolean property.
     *
     * @param key Property key.
     * @param val Value to set.
     */
    private void setBoolProp(String key, boolean val) {
        prefs.put(key, String.valueOf(val));
    }

    /**
     * Resolve path.
     *
     * @param key Preferences key.
     * @param dflt Default value.
     * @return String with full file path or default value.
     */
    private String resolveFilePath(String key, String dflt) {
        String path = prefs.getProperty(key);

        if (path != null) {
            File file = U.resolveIgnitePath(path);

            if (file != null)
                return file.getAbsolutePath();
        }

        return dflt;
    }

    /** {@inheritDoc} */
    @Override public void start(Stage primaryStage) {
        owner = primaryStage;

        if (prefsFile.exists())
            try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(prefsFile))) {
                prefs.load(in);
            }
            catch (IOException e) {
                log.log(Level.SEVERE, "Failed to load preferences. Default preferences will be used", e);
            }

        // Load custom preferences.
        List<String> params = getParameters().getRaw();

        if (!params.isEmpty()) {
            String customPrefsFileName = params.get(0);

            if (customPrefsFileName.isEmpty())
                log.log(Level.WARNING, "Path to file with custom preferences is not specified.");
            else {
                File customPrefsFile = U.resolveIgnitePath(customPrefsFileName);

                if (customPrefsFile == null)
                    log.log(Level.WARNING, "Failed to resolve path to file with custom preferences: " +
                        customPrefsFile);
                else {
                    Properties customPrefs = new Properties();

                    try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(customPrefsFile))) {
                        customPrefs.load(in);
                    }
                    catch (IOException e) {
                        log.log(Level.SEVERE, "Failed to load custom preferences.", e);
                    }

                    prefs.putAll(customPrefs);
                }
            }
        }

        // Restore presets.
        for (Preset preset : presets) {
            String key = "presets." + preset.pref + ".";

            preset.jar = getStringProp(key + "jar", preset.jar);
            preset.drv = getStringProp(key + "drv", preset.drv);
            preset.url = getStringProp(key + "url", preset.url);
            preset.user = getStringProp(key + "user", preset.user);
        }

        primaryStage.setTitle("Apache Ignite Auto Schema Import Utility");

        primaryStage.getIcons().addAll(
            image("ignite", 16),
            image("ignite", 24),
            image("ignite", 32),
            image("ignite", 48),
            image("ignite", 64),
            image("ignite", 128));

        pi = progressIndicator(50);

        createGeneratePane();

        hdrPane = createHeaderPane();
        rootPane = borderPane(hdrPane, createConnectionPane(), createButtonsPane(), null, null);

        primaryStage.setScene(scene(rootPane));

        primaryStage.setWidth(650);
        primaryStage.setMinWidth(650);

        primaryStage.setHeight(650);
        primaryStage.setMinHeight(650);

        prev();

        // Restore window pos and size.
        if (prefs.getProperty(PREF_WINDOW_X) != null) {
            int x = getIntProp(PREF_WINDOW_X, 100);
            int y = getIntProp(PREF_WINDOW_Y, 100);
            int w = getIntProp(PREF_WINDOW_WIDTH, 650);
            int h = getIntProp(PREF_WINDOW_HEIGHT, 650);

            // Ensure that window fit any available screen.
            if (!Screen.getScreensForRectangle(x, y, w, h).isEmpty()) {
                primaryStage.setX(x);
                primaryStage.setY(y);

                primaryStage.setWidth(w);
                primaryStage.setHeight(h);
            }
        }
        else
            primaryStage.centerOnScreen();

        String userHome = System.getProperty("user.home").replace('\\', '/');

        // Restore connection pane settings.
        rdbmsCb.getSelectionModel().select(getIntProp(PREF_JDBC_DB_PRESET, 0));
        jdbcDrvJarTf.setText(resolveFilePath(PREF_JDBC_DRIVER_JAR, "h2.jar"));
        jdbcDrvClsTf.setText(getStringProp(PREF_JDBC_DRIVER_CLASS, "org.h2.Driver"));
        jdbcUrlTf.setText(getStringProp(PREF_JDBC_URL, "jdbc:h2:" + userHome + "/ignite-schema-import/db"));
        userTf.setText(getStringProp(PREF_JDBC_USER, "sa"));

        // Restore generation pane settings.
        outFolderTf.setText(resolveFilePath(PREF_OUT_FOLDER, userHome + "/ignite-schema-import/out"));

        pkgTf.setText(getStringProp(PREF_POJO_PACKAGE, "org.apache.ignite"));
        pojoIncludeKeysCh.setSelected(getBoolProp(PREF_POJO_INCLUDE, true));
        pojoConstructorCh.setSelected(getBoolProp(PREF_POJO_CONSTRUCTOR, false));

        xmlSingleFileCh.setSelected(getBoolProp(PREF_XML_SINGLE, true));

        regexTf.setText(getStringProp(PREF_NAMING_PATTERN, "(\\w+)"));
        replaceTf.setText(getStringProp(PREF_NAMING_REPLACE, "$1_SomeText"));

        primaryStage.show();
    }

    /**
     * Save preset.
     *
     * @param preset Preset to save.
     */
    private void savePreset(Preset preset) {
        String key = "presets." + preset.pref + ".";

        preset.jar = jdbcDrvJarTf.getText();
        setStringProp(key + "jar", preset.jar);

        preset.drv = jdbcDrvClsTf.getText();
        setStringProp(key + "drv", preset.drv);

        preset.url = jdbcUrlTf.getText();
        setStringProp(key + "url", preset.url);

        preset.user = userTf.getText();
        setStringProp(key + "user", preset.user);

        savePreferences();
    }

    /**
     * Save user preferences.
     */
    private void savePreferences() {
        try (FileOutputStream out = new FileOutputStream(prefsFile)) {
            prefs.store(out, "Apache Ignite Schema Import Utility");
        }
        catch (IOException e) {
            MessageBox.errorDialog(owner, "Failed to save preferences!", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        // Save window pos and size.
        setIntProp(PREF_WINDOW_X, (int)owner.getX());
        setIntProp(PREF_WINDOW_Y, (int)owner.getY());
        setIntProp(PREF_WINDOW_WIDTH, (int)owner.getWidth());
        setIntProp(PREF_WINDOW_HEIGHT, (int)owner.getHeight());

        // Save connection pane settings.
        setIntProp(PREF_JDBC_DB_PRESET, rdbmsCb.getSelectionModel().getSelectedIndex());
        setStringProp(PREF_JDBC_DRIVER_JAR, jdbcDrvJarTf.getText());
        setStringProp(PREF_JDBC_DRIVER_CLASS, jdbcDrvClsTf.getText());
        setStringProp(PREF_JDBC_URL, jdbcUrlTf.getText());
        setStringProp(PREF_JDBC_USER, userTf.getText());

        // Save generation pane settings.
        setStringProp(PREF_OUT_FOLDER, outFolderTf.getText());

        setStringProp(PREF_POJO_PACKAGE, pkgTf.getText());
        setBoolProp(PREF_POJO_INCLUDE, pojoIncludeKeysCh.isSelected());
        setBoolProp(PREF_POJO_CONSTRUCTOR, pojoConstructorCh.isSelected());

        setBoolProp(PREF_XML_SINGLE, xmlSingleFileCh.isSelected());

        setStringProp(PREF_NAMING_PATTERN, regexTf.getText());
        setStringProp(PREF_NAMING_REPLACE, replaceTf.getText());

        savePreferences();
    }

    /**
     * Schema Import utility launcher.
     *
     * @param args Command line arguments passed to the application.
     */
    public static void main(String[] args) {
        // Workaround for JavaFX ugly text AA.
        System.setProperty("prism.lcdtext", "false");
        System.setProperty("prism.text", "t2k");

        // Workaround for AWT + JavaFX: we should initialize AWT before JavaFX.
        java.awt.Toolkit.getDefaultToolkit();

        // Workaround for JavaFX + Mac OS dock icon.
        if (System.getProperty("os.name").toLowerCase().contains("mac os")) {
            System.setProperty("javafx.macosx.embedded", "true");

            try {
                Class<?> appCls = Class.forName("com.apple.eawt.Application");

                Object osxApp = appCls.getDeclaredMethod("getApplication").invoke(null);

                appCls.getDeclaredMethod("setDockIconImage", java.awt.Image.class)
                    .invoke(osxApp, fromFXImage(image("ignite", 128), null));
            }
            catch (Exception ignore) {
                // No-op.
            }
        }

        launch(args);
    }

    /**
     * Special table cell to select possible java type conversions.
     */
    private static class JavaTypeCell extends TableCell<PojoField, String> {
        /** Combo box. */
        private final ComboBox<String> comboBox;

        /**
         * Creates a ComboBox cell factory for use in TableColumn controls.
         *
         * @return Cell factory for cell with java types combobox.
         */
        public static Callback<TableColumn<PojoField, String>, TableCell<PojoField, String>> cellFactory() {
            return new Callback<TableColumn<PojoField, String>, TableCell<PojoField, String>>() {
                @Override public TableCell<PojoField, String> call(TableColumn<PojoField, String> col) {
                    return new JavaTypeCell();
                }
            };
        }

        /**
         * Default constructor.
         */
        private JavaTypeCell() {
            comboBox = new ComboBox<>(FXCollections.<String>emptyObservableList());

            comboBox.valueProperty().addListener(new ChangeListener<String>() {
                @Override public void changed(ObservableValue<? extends String> val, String oldVal, String newVal) {
                    if (isEditing())
                        commitEdit(newVal);
                }
            });

            getStyleClass().add("combo-box-table-cell");
        }

        /** {@inheritDoc} */
        @Override public void startEdit() {
            if (comboBox.getItems().size() > 1) {
                comboBox.getSelectionModel().select(getItem());

                super.startEdit();

                setText(null);
                setGraphic(comboBox);
            }
        }

        /** {@inheritDoc} */
        @Override public void cancelEdit() {
            super.cancelEdit();

            setText(getItem());

            setGraphic(null);
        }

        /** {@inheritDoc} */
        @Override public void updateItem(String item, boolean empty) {
            super.updateItem(item, empty);

            setGraphic(null);
            setText(null);

            if (!empty) {
                setText(item);

                TableRow row = getTableRow();

                if (row != null) {
                    PojoField pojo = (PojoField)row.getItem();

                    if (pojo != null) {
                        comboBox.setItems(pojo.conversions());
                        comboBox.getSelectionModel().select(pojo.javaTypeName());
                    }
                }
            }
        }
    }

    /**
     * Special table cell to select schema or table.
     */
    private static class PojoDescriptorCell extends TableCell<PojoDescriptor, Boolean> {
        /**
         * Creates a ComboBox cell factory for use in TableColumn controls.
         *
         * @return Cell factory for schema / table selection.
         */
        public static Callback<TableColumn<PojoDescriptor, Boolean>, TableCell<PojoDescriptor, Boolean>> cellFactory() {
            return new Callback<TableColumn<PojoDescriptor, Boolean>, TableCell<PojoDescriptor, Boolean>>() {
                @Override public TableCell<PojoDescriptor, Boolean> call(TableColumn<PojoDescriptor, Boolean> col) {
                    return new PojoDescriptorCell();
                }
            };
        }

        /** Previous POJO bound to cell. */
        private PojoDescriptor prevPojo;

        /** Previous cell graphic. */
        private Pane prevGraphic;

        /** {@inheritDoc} */
        @Override public void updateItem(Boolean item, boolean empty) {
            super.updateItem(item, empty);

            setGraphic(null);

            if (!empty) {
                TableRow row = getTableRow();

                if (row != null) {
                    final PojoDescriptor pojo = (PojoDescriptor)row.getItem();

                    if (pojo != null) {
                        if (prevGraphic == null || pojo != prevPojo) {
                            boolean isTbl = pojo.parent() != null;

                            CheckBox ch = new CheckBox();
                            ch.setAllowIndeterminate(false);
                            ch.indeterminateProperty().bindBidirectional(pojo.indeterminate());
                            ch.selectedProperty().bindBidirectional(pojo.useProperty());

                            Label lb = new Label(isTbl ? pojo.table() : pojo.schema());

                            Pane pnl = new HBox(5);
                            pnl.setPadding(new Insets(0, 0, 0, isTbl ? 25 : 5));
                            pnl.getChildren().addAll(ch, lb);

                            prevPojo = pojo;
                            prevGraphic = pnl;
                        }

                        setGraphic(prevGraphic);
                    }
                }
            }
        }
    }

    /**
     * Special table cell to select &quot;used&quot; fields for code generation.
     */
    private static class PojoFieldUseCell extends TableCell<PojoField, Boolean> {
        /**
         * Creates a ComboBox cell factory for use in TableColumn controls.
         *
         * @return Cell factory for used fields selection.
         */
        public static Callback<TableColumn<PojoField, Boolean>, TableCell<PojoField, Boolean>> cellFactory() {
            return new Callback<TableColumn<PojoField, Boolean>, TableCell<PojoField, Boolean>>() {
                @Override public TableCell<PojoField, Boolean> call(TableColumn<PojoField, Boolean> col) {
                    return new PojoFieldUseCell();
                }
            };
        }

        /** Previous POJO field bound to cell. */
        private PojoField prevField;

        /** Previous cell graphic. */
        private CheckBox prevGraphic;

        /** {@inheritDoc} */
        @Override public void updateItem(Boolean item, boolean empty) {
            super.updateItem(item, empty);

            setGraphic(null);

            if (!empty) {
                TableRow row = getTableRow();

                if (row != null) {
                    final PojoField field = (PojoField)row.getItem();

                    if (field != null) {
                        if (prevGraphic == null || prevField != field) {
                            setAlignment(Pos.CENTER);

                            CheckBox ch = new CheckBox();
                            ch.setDisable(!field.nullable());
                            ch.selectedProperty().bindBidirectional(field.useProperty());

                            prevField = field;
                            prevGraphic = ch;
                        }

                        setGraphic(prevGraphic);
                    }
                }
            }
        }
    }
}
