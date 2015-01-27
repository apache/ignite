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
import javafx.scene.*;
import javafx.scene.control.*;
import javafx.scene.input.*;
import javafx.scene.layout.*;
import javafx.stage.*;
import javafx.util.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.schema.generator.*;
import org.apache.ignite.schema.model.*;
import org.apache.ignite.schema.parser.*;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.prefs.*;

import static javafx.embed.swing.SwingFXUtils.*;
import static org.apache.ignite.schema.ui.Controls.*;

/**
 * Schema load application.
 */
@SuppressWarnings("UnnecessaryFullyQualifiedName")
public class SchemaLoadApp extends Application {
    /** */
    private Stage owner;

    /** */
    private BorderPane rootPane;

    /** */
    private Label titleLb;

    /** */
    private Button prevBtn;

    /** */
    private Button nextBtn;

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
    private CheckBox openFolderCh;

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

    /** */
    private static final ObservableList<PojoField> NO_FIELDS = FXCollections.emptyObservableList();

    /** */
    private final ExecutorService exec = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "schema-load-worker");

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

        final boolean tblsOnly = parseCb.getSelectionModel().getSelectedIndex() == 0;

        Runnable task = new Task<Void>() {
            /** {@inheritDoc} */
            @Override protected Void call() throws Exception {
                long started = System.currentTimeMillis();

                try (Connection conn = connect(jdbcDrvJarPath)) {
                    pojos = DatabaseMetadataParser.parse(conn/*, tblsOnly*/);
                }

                perceptualDelay(started);

                return null;
            }

            /** {@inheritDoc} */
            @Override protected void succeeded() {
                super.succeeded();

                pojosTbl.setItems(pojos);

                if (!pojos.isEmpty())
                    pojosTbl.getSelectionModel().select(pojos.get(0));

                unlockUI(connLayerPnl, connPnl, nextBtn);

                titleLb.setText("Generate XML And POJOs");
                titleLb.setGraphic(imageView("text_tree", 48));

                rootPane.setCenter(genLayerPnl);

                prevBtn.setDisable(false);
                nextBtn.setText("Generate");
                tooltip(nextBtn, "Generate XML and POJO files");
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
        final Collection<PojoDescriptor> selPojos = selectedItems();

        if (selPojos.isEmpty()) {
            MessageBox.warningDialog(owner, "Please select tables to generate XML and POJOs files!");

            return;
        }

        lockUI(genLayerPnl, genPnl, prevBtn, nextBtn);

        final String outFolder = outFolderTf.getText();

        final String pkg = pkgTf.getText();

        final File destFolder = new File(outFolder);

        Runnable task = new Task<Void>() {
            private void checkEmpty(final PojoDescriptor pojo, Collection<CacheQueryTypeDescriptor> descs,
                String msg) {
                if (descs.isEmpty()) {
                    Platform.runLater(new Runnable() {
                        @Override public void run() {
                            pojosTbl.getSelectionModel().select(pojo);
                            pojosTbl.scrollTo(pojosTbl.getSelectionModel().getSelectedIndex());
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

                Collection<CacheQueryTypeMetadata> all = new ArrayList<>();

                boolean constructor = pojoConstructorCh.isSelected();
                boolean includeKeys = pojoIncludeKeysCh.isSelected();
                boolean singleXml = xmlSingleFileCh.isSelected();

                ConfirmCallable askOverwrite = new ConfirmCallable(owner, "File already exists: %s\nOverwrite?");

                // Generate XML and POJO.
                for (PojoDescriptor pojo : selPojos) {
                    if (pojo.selected()) {
                        CacheQueryTypeMetadata meta = pojo.metadata(includeKeys);

                        checkEmpty(pojo, meta.getKeyDescriptors(), "No key fields specified for type: ");

                        checkEmpty(pojo, meta.getValueDescriptors(), "No value fields specified for type: ");

                        all.add(meta);

                        if (!singleXml)
                            XmlGenerator.generate(pkg, meta, new File(destFolder, meta.getType() + ".xml"),
                                askOverwrite);

                        PojoGenerator.generate(pojo, outFolder, pkg, constructor, includeKeys, askOverwrite);
                    }
                }

                if (singleXml)
                    XmlGenerator.generate(pkg, all, new File(outFolder, "Ignite.xml"), askOverwrite);

                perceptualDelay(started);

                return null;
            }

            /** {@inheritDoc} */
            @Override protected void succeeded() {
                super.succeeded();

                unlockUI(genLayerPnl, genPnl, prevBtn, nextBtn);

                MessageBox.informationDialog(owner, "Generation complete!");

                if (openFolderCh.isSelected())
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
    private Pane createHeaderPane() {
        titleLb = label("");
        titleLb.setId("banner");

        BorderPane bp = borderPane(null, hBox(10, true, titleLb), null, null, hBox(0, true, imageView("ignite", 48)));
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

        titleLb.setText("Connect To Database");
        titleLb.setGraphic(imageView("data_connection", 48));

        rootPane.setCenter(connLayerPnl);

        prevBtn.setDisable(true);
        nextBtn.setText("Next");
        tooltip(nextBtn, "Go to \"XML and POJO generation\" page");
    }

    /**
     * Check that text field is non empty.
     *
     * @param tf Text field to check.
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
     * @return Connection to database.
     * @throws SQLException if connection failed.
     */
    private Connection connect(String jdbcDrvJarPath) throws SQLException {
        String drvCls = jdbcDrvClsTf.getText();

        Driver drv = drivers.get(drvCls);

        if (drv == null) {
            if (jdbcDrvJarPath.isEmpty())
                throw new IllegalStateException("Driver jar file name is not specified");

            File drvJar = new File(jdbcDrvJarPath);

            if (!drvJar.exists())
                throw new IllegalStateException("Driver jar file is not found");

            try {
                URL u = new URL("jar:" + drvJar.toURI() + "!/");

                URLClassLoader ucl = URLClassLoader.newInstance(new URL[] {u});

                drv = (Driver)Class.forName(drvCls, true, ucl).newInstance();

                drivers.put(drvCls, drv);
            }
            catch (Throwable e) {
                throw new IllegalStateException(e);
            }
        }

        String user = userTf.getText().trim();

        String pwd = pwdTf.getText().trim();

        Properties info = new Properties();

        if (!user.isEmpty())
            info.put("user", user);

        if (!pwd.isEmpty())
            info.put("password", pwd);

        Connection conn = drv.connect(jdbcUrlTf.getText(), info);

        if (conn == null)
            throw new IllegalStateException("Connection was not established (JDBC driver returned null value).");

        return conn;
    }

    /**
     * Create connection pane with controls.
     */
    private Pane createConnectionPane() {
        connPnl = paneEx(10, 10, 0, 10);

        connPnl.addColumn();
        connPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        connPnl.addColumn(35, 35, 35, Priority.NEVER);

        connPnl.add(text("This utility is designed to automatically generate configuration XML files and" +
            " POGO classes from database schema information.", 550), 3);

        connPnl.wrap();

        jdbcDrvJarTf = connPnl.addLabeled("Driver JAR:", textField("Path to driver jar"));

        connPnl.add(button("...", "Select JDBC driver jar or zip", new EventHandler<ActionEvent>() {
            /** {@inheritDoc} */
            @Override public void handle(ActionEvent evt) {
                FileChooser fc = new FileChooser();

                fc.getExtensionFilters().addAll(
                    new FileChooser.ExtensionFilter("JDBC Drivers (*.jar)", "*.jar"),
                    new FileChooser.ExtensionFilter("ZIP archives (*.zip)", "*.zip"));

                File drvJar = fc.showOpenDialog(owner);

                if (drvJar != null)
                    jdbcDrvJarTf.setText(drvJar.getAbsolutePath());
            }
        }));

        jdbcDrvClsTf = connPnl.addLabeled("JDBC Driver:", textField("Class name for JDBC driver"), 2);

        jdbcUrlTf = connPnl.addLabeled("JDBC URL:", textField("JDBC URL of the database connection string"), 2);

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
        genPnl.addRows(8);

        TableColumn<PojoDescriptor, Boolean> useCol = customColumn("Schema / Table", "use",
            "If checked then this table will be used for XML and POJOs generation", PojoDescriptorCell.cellFactory());

        TableColumn<PojoDescriptor, String> keyClsCol = textColumn("Key Class Name", "keyClassName", "Key class name",
            new TextColumnValidator<PojoDescriptor>() {
                @Override public boolean valid(PojoDescriptor rowVal, String newVal) {
                    return checkClassName(rowVal, newVal, true);
                }
            });

        TableColumn<PojoDescriptor, String> valClsCol = textColumn("Value Class Name", "valueClassName", "Value class name",
            new TextColumnValidator<PojoDescriptor>() {
                @Override public boolean valid(PojoDescriptor rowVal, String newVal) {
                    return checkClassName(rowVal, newVal, false);
                }
            });

        pojosTbl = tableView("Tables not found in database", useCol, keyClsCol, valClsCol);

        TableColumn<PojoField, Boolean> useFldCol = booleanColumn("Use", "use",
            "If checked then this field will used for XML and POJO generation");

        TableColumn<PojoField, Boolean> keyCol = booleanColumn("Key", "key",
            "If checked then this field will be part of key object");

        TableColumn<PojoField, Boolean> akCol = booleanColumn("AK", "affinityKey",
            "If checked then this field will be marked with @CacheAffinityKeyMapped annotation in generated POJO");

        TableColumn<PojoField, String> dbNameCol = tableColumn("DB Name", "dbName", "Field name in database");

        TableColumn<PojoField, String> dbTypeNameCol = tableColumn("DB Type", "dbTypeName", "Field type in database");

        TableColumn<PojoField, String> javaNameCol = textColumn("Java Name", "javaName", "Field name in POJO class",
            new TextColumnValidator<PojoField>() {
                @Override public boolean valid(PojoField rowVal, String newVal) {
                    for (PojoField field : curPojo.fields())
                        if (rowVal != field && newVal.equals(field.javaName())) {
                            MessageBox.warningDialog(owner, "Java name must be unique!");

                            return false;
                        }

                    return true;
                }
            });

        TableColumn<PojoField, String> javaTypeNameCol = customColumn("Java Type", "javaTypeName",
            "Field java type in POJO class", JavaTypeCell.cellFactory());

        final TableView<PojoField> fieldsTbl = tableView("Select table to see table columns",
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
            "If selected then all configurations will be saved into the file 'Ignite.xml'", true), 3);

        openFolderCh = genPnl.add(checkBox("Reveal output folder",
            "Open output folder in system file manager after generation complete", true), 3);

        genPnl.add(new Separator(), 3);

        GridPaneEx regexPnl = paneEx(0, 0, 0, 0);
        regexPnl.addColumn();
        regexPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        regexPnl.addColumn();
        regexPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);

        regexPnl.add(label("Replace \"Key class name\", \"Value class name\" or  \"Java name\" for selected tables:"), 4);

        regexTf = regexPnl.addLabeled("  Regexp:", textField("Regular expression. For example: (\\w+)"));

        replaceTf = regexPnl.addLabeled("  Replace with:", textField("Replace text. For example: $1_SomeText"));

        final ComboBox<String> replaceCb = regexPnl.addLabeled("  Replace:", comboBox("Replacement target",
            "Key class names", "Value class names", "Java names"));

        regexPnl.add(buttonsPane(Pos.CENTER_LEFT, false,
            button("Rename Selected", "Replaces each substring of this string that matches the given regular expression" +
                    " with the given replacement.",
                new EventHandler<ActionEvent>() {
                    @Override public void handle(ActionEvent evt) {
                        if (checkInput(regexTf, false, "Regular expression should not be empty!"))
                            return;

                        String sel = replaceCb.getSelectionModel().getSelectedItem();

                        boolean renFields = "Java names".equals(sel);

                        String src = (renFields ? "fields" : "tables");

                        String target = "\"" + sel + "\"";

                        Collection<PojoDescriptor> selItems = selectedItems();

                        if (selItems.isEmpty()) {
                            MessageBox.warningDialog(owner, "Please select " + src + " to rename " + target + "!");

                            return;
                        }

                        if (!MessageBox.confirmDialog(owner, "Are you sure you want to rename " + target +
                            " for all selected " + src + "?"))
                            return;

                        String regex = regexTf.getText();

                        String replace = replaceTf.getText();

                        try {
                            for (PojoDescriptor pojo : selItems)
                                for (PojoField field : pojo.fields())
                                    field.javaName(field.javaName().replaceAll(regex, replace));
                        }
                        catch (Exception e) {
                            MessageBox.errorDialog(owner, "Failed to rename " + target + "!", e);
                        }
                    }
                }),
            button("Reset Selected", "Revert changes for selected items to initial values", new EventHandler<ActionEvent>() {
                @Override public void handle(ActionEvent evt) {
                    Collection<PojoDescriptor> selItems = selectedItems();

                    String sel = replaceCb.getSelectionModel().getSelectedItem();

                    boolean renFields = "Java names".equals(sel);

                    String src = (renFields ? "fields" : "tables");

                    String target = "\"" + sel + "\"";

                    if (selItems.isEmpty()) {
                        MessageBox.warningDialog(owner, "Please select " + src + "to revert " + target + "!");

                        return;
                    }

                    if (!MessageBox.confirmDialog(owner,
                        "Are you sure you want to revert " + target + " for all selected " + src + "?"))
                        return;

                    for (PojoDescriptor pojo : selItems)
                        pojo.revertJavaNames();
                }
            })
        ), 2).setPadding(new Insets(0, 0, 0, 10));

        pojosTbl.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<PojoDescriptor>() {
            @Override public void changed(ObservableValue<? extends PojoDescriptor> val,
                PojoDescriptor oldVal, PojoDescriptor newItem) {
                if (newItem != null && newItem.parent() != null) {
                    curPojo = newItem;

                    fieldsTbl.setItems(curPojo.fields());
                    fieldsTbl.getSelectionModel().select(0);

                    keyValPnl.setDisable(false);
                }
                else {
                    curPojo = null;
                    fieldsTbl.setItems(NO_FIELDS);

                    keyValPnl.setDisable(true);
                }
            }
        });

        genPnl.add(regexPnl, 3);

        genLayerPnl = stackPane(genPnl);
    }

    /**
     * @return Selected tree view items.
     */
    private Collection<PojoDescriptor> selectedItems() {
        Collection<PojoDescriptor> res = new ArrayList<>();

        for (PojoDescriptor pojo : pojos)
            if (pojo.selected())
                res.add(pojo);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void start(Stage primaryStage) {
        owner = primaryStage;

        primaryStage.setTitle("Apache Ignite Auto Schema Load Utility");

        primaryStage.getIcons().addAll(
            image("ignite", 16),
            image("ignite", 24),
            image("ignite", 32),
            image("ignite", 48),
            image("ignite", 64),
            image("ignite", 128));

        pi = progressIndicator(50);

        createGeneratePane();

        rootPane = borderPane(createHeaderPane(), createConnectionPane(), createButtonsPane(), null, null);

        primaryStage.setScene(scene(rootPane));

        primaryStage.setWidth(650);
        primaryStage.setMinWidth(650);

        primaryStage.setHeight(650);
        primaryStage.setMinHeight(650);

        prev();

        Preferences userPrefs = Preferences.userNodeForPackage(getClass());

        // Restore window pos and size.
        if (userPrefs.get("window.x", null) != null) {
            double x = userPrefs.getDouble("window.x", 100);
            double y = userPrefs.getDouble("window.y", 100);
            double w = userPrefs.getDouble("window.width", 650);
            double h = userPrefs.getDouble("window.height", 650);

            // Ensure that window fit any available screen.
            if (!Screen.getScreensForRectangle(x, y, w, h).isEmpty()) {
                if (x > 0)
                    primaryStage.setX(x);

                if (y > 0)
                    primaryStage.setY(y);

                primaryStage.setWidth(w);
                primaryStage.setHeight(h);
            }
        }
        else
            primaryStage.centerOnScreen();

        String userHome = System.getProperty("user.home").replace('\\', '/');

        // Restore connection pane settings.
        jdbcDrvJarTf.setText(userPrefs.get("jdbc.driver.jar", "h2.jar"));
        jdbcDrvClsTf.setText(userPrefs.get("jdbc.driver.class", "org.h2.Driver"));
        jdbcUrlTf.setText(userPrefs.get("jdbc.url", "jdbc:h2:" + userHome + "/schema-load/db"));
        userTf.setText(userPrefs.get("jdbc.user", "sa"));

        // Restore generation pane settings.
        outFolderTf.setText(userPrefs.get("out.folder", userHome + "/schema-load/out"));
        openFolderCh.setSelected(userPrefs.getBoolean("out.folder.open", true));

        pkgTf.setText(userPrefs.get("pojo.package", "org.apache.ignite"));
        pojoIncludeKeysCh.setSelected(userPrefs.getBoolean("pojo.include", true));
        pojoConstructorCh.setSelected(userPrefs.getBoolean("pojo.constructor", false));

        xmlSingleFileCh.setSelected(userPrefs.getBoolean("xml.single", true));

        regexTf.setText(userPrefs.get("naming.pattern", ""));
        replaceTf.setText(userPrefs.get("naming.replace", ""));

        primaryStage.show();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws Exception {
        Preferences userPrefs = Preferences.userNodeForPackage(getClass());

        // Save window pos and size.
        userPrefs.putDouble("window.x", owner.getX());
        userPrefs.putDouble("window.y", owner.getY());
        userPrefs.putDouble("window.width", owner.getWidth());
        userPrefs.putDouble("window.height", owner.getHeight());

        // Save connection pane settings.
        userPrefs.put("jdbc.driver.jar", jdbcDrvJarTf.getText());
        userPrefs.put("jdbc.driver.class", jdbcDrvClsTf.getText());
        userPrefs.put("jdbc.url", jdbcUrlTf.getText());
        userPrefs.put("jdbc.user", userTf.getText());

        // Save generation pane settings.
        userPrefs.put("out.folder", outFolderTf.getText());
        userPrefs.putBoolean("out.folder.open", openFolderCh.isSelected());

        userPrefs.put("pojo.package", pkgTf.getText());
        userPrefs.putBoolean("pojo.include", pojoIncludeKeysCh.isSelected());
        userPrefs.putBoolean("pojo.constructor", pojoConstructorCh.isSelected());

        userPrefs.putBoolean("xml.single", xmlSingleFileCh.isSelected());

        userPrefs.put("naming.pattern", regexTf.getText());
        userPrefs.put("naming.replace", replaceTf.getText());
    }

    /**
     * Schema load utility launcher.
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
            catch (Throwable ignore) {
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

        /** Creates a ComboBox cell factory for use in TableColumn controls. */
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
        /** Creates a ComboBox cell factory for use in TableColumn controls. */
        public static Callback<TableColumn<PojoDescriptor, Boolean>, TableCell<PojoDescriptor, Boolean>> cellFactory() {
            return new Callback<TableColumn<PojoDescriptor, Boolean>, TableCell<PojoDescriptor, Boolean>>() {
                @Override public TableCell<PojoDescriptor, Boolean> call(TableColumn<PojoDescriptor, Boolean> col) {
                    return new PojoDescriptorCell();
                }
            };
        }

        /** Previous POJO bound to cell. */
        private PojoDescriptor prevPojo;

        /** {@inheritDoc} */
        @Override public void updateItem(Boolean item, boolean empty) {
            super.updateItem(item, empty);

            if (!empty) {
                TableRow row = getTableRow();

                if (row != null) {
                    final PojoDescriptor pojo = (PojoDescriptor)row.getItem();

                    if (pojo != prevPojo) {
                        prevPojo = pojo;

                        boolean isTbl = pojo.parent() != null;

                        CheckBox ch = new CheckBox(isTbl ? pojo.table() : pojo.schema());

                        ch.setAllowIndeterminate(false);
                        ch.setMnemonicParsing(false);

                        ch.indeterminateProperty().bindBidirectional(pojo.indeterminate());
                        ch.selectedProperty().bindBidirectional(pojo.useProperty());

                        ch.setOnMouseClicked(new EventHandler<MouseEvent>() {
                            @Override public void handle(MouseEvent evt) {
                                TableView<PojoDescriptor> view = getTableView();

                                view.getSelectionModel().select(getIndex());
                                view.requestFocus();
                            }
                        });

                        Pane pnl = new HBox();

                        pnl.setPadding(new Insets(0, 0, 0, isTbl ? 25 : 5));
                        pnl.getChildren().add(ch);

                        setGraphic(pnl);
                    }
                }
            }
        }
    }
}
