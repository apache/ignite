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
import javafx.beans.property.*;
import javafx.beans.value.*;
import javafx.collections.*;
import javafx.concurrent.*;
import javafx.event.*;
import javafx.geometry.*;
import javafx.scene.*;
import javafx.scene.control.*;
import javafx.scene.control.cell.*;
import javafx.scene.layout.*;
import javafx.stage.*;
import javafx.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.schema.generator.*;
import org.gridgain.grid.cache.query.*;

import java.io.*;
import java.math.*;
import java.net.*;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.*;
import java.util.prefs.*;

import static java.sql.Types.*;
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
    private GridPaneEx connPnl;

    /** */
    private StackPane connLayerPnl;

    /** */
    private TreeView<String> tree;

    /** */
    private CheckBoxTreeItem<String> rootItem;

    /** */
    private TextField keyClsTf;

    /** */
    private TextField valClsTf;

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

    /** Map with schema - table - POJO descriptors. */
    private Map<String, Map<String, PojoDescriptor>> schemas = Collections.emptyMap();

    /** Map all POJO descriptors. */
    private Collection<PojoDescriptor> pojos = Collections.emptyList();

    /** Currently selected POJO. */
    private PojoDescriptor curPojo;

    /** */
    private final Map<String, Driver> drivers = new HashMap<>();

    /** */
    private final ObservableList<PojoField> NO_DATA = FXCollections.emptyObservableList();

    /** */
    private final ExecutorService exec = Executors.newSingleThreadExecutor(new ThreadFactory() {
        /** {@inheritDoc} */
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

        Runnable task = new Task<Void>() {
            /** {@inheritDoc} */
            @Override protected Void call() throws Exception {
                long started = System.currentTimeMillis();

                try (Connection conn = connect()) {
                    schemas = parse(conn);
                }

                pojos = new ArrayList<>();

                for (Map<String, PojoDescriptor> schema : schemas.values())
                    for (PojoDescriptor pojo : schema.values())
                        pojos.add(pojo);

                perceptualDelay(started);

                return null;
            }

            private CheckBoxTreeItem<String> addCheckBoxTreeItem(CheckBoxTreeItem<String> parent, String text) {
                CheckBoxTreeItem<String> item = new CheckBoxTreeItem<>(text);

                item.setSelected(true);
                item.setExpanded(true);

                parent.getChildren().add(item);

                return item;
            }

            /** {@inheritDoc} */
            @Override protected void succeeded() {
                super.succeeded();

                rootItem.getChildren().clear();

                for (Map.Entry<String, Map<String, PojoDescriptor>> schema : schemas.entrySet()) {
                    CheckBoxTreeItem<String> schemaItem = addCheckBoxTreeItem(rootItem, schema.getKey());

                    for (String tbl : schema.getValue().keySet())
                        addCheckBoxTreeItem(schemaItem, tbl);
                }

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
        Collection<PojoDescriptor> selItems = selectedItems();

        if (selItems.isEmpty()) {
            MessageBox.warningDialog(owner, "Please select tables to generate POJOs and XML files!");

            return;
        }

        lockUI(genLayerPnl, genPnl, prevBtn, nextBtn);

        final String outFolder = outFolderTf.getText();

        final String pkg = pkgTf.getText();

        final File destFolder = new File(outFolder);

        Runnable task = new Task<Void>() {
            private void checkEmpty(Collection<GridCacheQueryTypeDescriptor> items,
                final TreeItem<String> treeItem, String msg) {
                if (items.isEmpty()) {
                    Platform.runLater(new Runnable() {
                        /** {@inheritDoc} */
                        @Override public void run() {
                            tree.getSelectionModel().select(treeItem);
                        }
                    });

                    throw new IllegalStateException(msg);
                }
            }

            /** {@inheritDoc} */
            @Override protected Void call() throws Exception {
                long started = System.currentTimeMillis();

                if (!destFolder.exists() && !destFolder.mkdirs())
                    throw new IOException("Failed to create output folder: " + destFolder);

                Collection<GridCacheQueryTypeMetadata> all = new ArrayList<>();

                boolean constructor = pojoConstructorCh.isSelected();
                boolean include = pojoIncludeKeysCh.isSelected();
                boolean singleXml = xmlSingleFileCh.isSelected();

                ConfirmCallable askOverwrite = new ConfirmCallable(owner, "File already exists: %s\nOverwrite?");

                // Generate POJO and XML.
                for (TreeItem<String> schemeItem : rootItem.getChildren()) {
                    for (TreeItem<String> tblItem : schemeItem.getChildren()) {
                        if (((CheckBoxTreeItem)tblItem).isSelected()) {
                            String scheme = schemeItem.getValue();
                            String tbl = tblItem.getValue();

                            PojoDescriptor pojo = schemas.get(scheme).get(tbl);

                            GridCacheQueryTypeMetadata meta = pojo.metadata();

                            Collection<GridCacheQueryTypeDescriptor> keys = new ArrayList<>();

                            Collection<GridCacheQueryTypeDescriptor> vals = new ArrayList<>();

                            // Fill list with key and value type descriptors.
                            for (PojoField fld : pojo.fields()) {
                                GridCacheQueryTypeDescriptor desc = fld.descriptor();

                                if (fld.key()) {
                                    keys.add(desc);

                                    if (include)
                                        vals.add(desc);
                                }
                                else
                                    vals.add(desc);
                            }

                            checkEmpty(keys, tblItem, "No key fields specified for type: " + tbl);

                            checkEmpty(vals, tblItem, "No value fields specified for type: " + tbl);

                            meta.setKeyDescriptors(keys);

                            meta.setValueDescriptors(vals);

                            all.add(meta);

                            if (!singleXml)
                                XmlGenerator.transform(pkg, meta, new File(destFolder, meta.getType() + ".xml"),
                                    askOverwrite);

                            PojoGenerator.generate(meta, outFolder, pkg, constructor, askOverwrite);
                        }
                    }
                }

                if (all.isEmpty())
                    throw new IllegalStateException("Nothing selected!");
                else if (singleXml)
                    XmlGenerator.transform(pkg, all, new File(outFolder, "Ignite.xml"), askOverwrite);

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
        titleLb = new Label("");
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
     * @return Connection to database.
     * @throws SQLException if connection failed.
     */
    private Connection connect() throws SQLException {
        String drvCls = jdbcDrvClsTf.getText();

        Driver drv = drivers.get(drvCls);

        if (drv == null) {
            String path = jdbcDrvJarTf.getText().trim();

            if (path.isEmpty())
                throw new IllegalStateException("Driver jar file name is not specified");

            File drvJar = new File(jdbcDrvJarTf.getText());

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

        return drv.connect(jdbcUrlTf.getText(), info);
    }

    /**
     * Create connection pane with controls.
     */
    private Pane createConnectionPane() {
        connPnl = paneEx(10, 10, 0, 10);

        connPnl.addColumn();
        connPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        connPnl.addColumn(35, 35, 35, Priority.NEVER);

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

        connLayerPnl = stackPane(connPnl);

        return connLayerPnl;
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

        TableColumn<PojoField, Boolean> keyCol = tableColumn("Key", "key",
            "If checked then this field will be part of key object", 70, 70);

        keyCol.setCellFactory(new Callback<TableColumn<PojoField, Boolean>, TableCell<PojoField, Boolean>>() {
            /** {@inheritDoc} */
            @Override public TableCell<PojoField, Boolean> call(TableColumn<PojoField, Boolean> col) {
                CheckBoxTableCell<PojoField, Boolean> cell = new CheckBoxTableCell<>();

                cell.setAlignment(Pos.CENTER);

                return cell;
            }
        });

        keyCol.setEditable(true);

        TableColumn<PojoField, String> dbNameCol = tableColumn("DB Name", "dbName", "Field name in database", 100, 0);

        TableColumn<PojoField, String> dbTypeNameCol = tableColumn("DB Type", "dbTypeName", "Field type in database", 100, 0);

        TableColumn<PojoField, String> javaNameCol = tableColumn("Ignite Name", "javaName",
            "Field name in POJO class", 100, 0);
        javaNameCol.setCellFactory(TextFieldTableCell.<PojoField>forTableColumn());
        javaNameCol.setEditable(true);

        TableColumn<PojoField, String> javaTypeNameCol = tableColumn("Java Type", "javaTypeName",
            "Field java type in POJO class", 100, 0);
        javaTypeNameCol.setCellFactory(JavaTypeCell.forTableColumn());
        javaTypeNameCol.setEditable(true);

        final TableView<PojoField> tbl = new TableView<>();

        tbl.setMinHeight(50);
        tbl.setPlaceholder(text("Select table to see table columns", 0));

        final TableColumn[] cols = {keyCol, dbNameCol, dbTypeNameCol, javaNameCol, javaTypeNameCol};

        //noinspection unchecked
        tbl.getColumns().addAll(cols);
        tbl.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
        tbl.setEditable(true);

        tbl.getColumns().addListener(new ListChangeListener<TableColumn<PojoField, ?>>() {
            private boolean suspended;

            /** {@inheritDoc} */
            @Override public void onChanged(Change change) {
                change.next();
                if (change.wasReplaced() && !suspended) {
                    suspended = true;

                    try {
                        //noinspection unchecked
                        tbl.getColumns().setAll(cols);
                    }
                    finally {
                        suspended = false;
                    }
                }
            }
        });

        final Button upBtn = button(imageView("navigate_up", 24), "Move selected row up",
            new EventHandler<ActionEvent>() {
                @Override public void handle(ActionEvent evt) {
                    TableView.TableViewSelectionModel<PojoField> selMdl = tbl.getSelectionModel();

                    int selIdx = selMdl.getSelectedIndex();

                    if (selIdx > 0) {
                        ObservableList<PojoField> items = tbl.getItems();

                        int newId = selIdx - 1;

                        items.add(newId, items.remove(selIdx));

                        if (newId == 0)
                            tbl.requestFocus();

                        selMdl.select(newId);
                    }
                }
            });

        upBtn.setDisable(true);

        final Button downBtn = button(imageView("navigate_down", 24), "Move selected row down",
            new EventHandler<ActionEvent>() {
                @Override public void handle(ActionEvent evt) {
                    TableView.TableViewSelectionModel<PojoField> selMdl = tbl.getSelectionModel();

                    int selIdx = selMdl.getSelectedIndex();

                    ObservableList<PojoField> items = tbl.getItems();

                    int maxIdx = items.size() - 1;

                    if (selIdx < maxIdx) {
                        int newId = selIdx + 1;

                        items.add(newId, items.remove(selIdx));

                        if (newId == maxIdx)
                            tbl.requestFocus();

                        selMdl.select(newId);

                    }
                }
            });

        downBtn.setDisable(true);

        tbl.getSelectionModel().selectedIndexProperty().addListener(new ChangeListener<Number>() {
            @Override public void changed(ObservableValue<? extends Number> observable, Number oldVal, Number newVal) {
                upBtn.setDisable(newVal == null || newVal.intValue() == 0);
                downBtn.setDisable(newVal == null || newVal.intValue() == tbl.getItems().size() - 1);
            }
        });

        rootItem = new CheckBoxTreeItem<>("Database");

        tree = new TreeView<>(rootItem);

        tree.setMinHeight(50);
        tree.setShowRoot(false);
        tree.setCellFactory(CheckBoxTreeCell.<String>forTreeView());

        genPnl.add(splitPane(tree, borderPane(null, tbl, null, null, vBox(10, upBtn, downBtn)), 0.6), 3);

        final GridPaneEx keyValPnl = paneEx(0, 0, 0, 0);
        keyValPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        keyValPnl.addColumn();
        keyValPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        keyValPnl.addColumn();

        genPnl.add(new Label("Key class:"));
        keyClsTf = keyValPnl.add(textField("Key class name"));

        valClsTf = keyValPnl.addLabeled("  Value class:", textField("Value class name"));

        keyValPnl.add(button("Apply", "Change key and value class names", new EventHandler<ActionEvent>() {
            @Override public void handle(ActionEvent evt) {
                if (checkInput(keyClsTf, true, "Key class name must not be empty!") ||
                    checkInput(valClsTf, true, "Value class name must not be empty!"))
                    return;

                String keyCls = keyClsTf.getText().trim();

                String valCls = valClsTf.getText().trim();

                if (keyCls.equals(valCls)) {
                    MessageBox.warningDialog(owner, "Key class name must be different from value class name!");

                    keyClsTf.requestFocus();

                    return;
                }

                for (PojoDescriptor pojo : pojos)
                    if (pojo != curPojo) {
                        String pojoKeyCls = pojo.keyClassName();

                        String pojoValCls = pojo.valueClassName();

                        if (keyCls.equals(pojoKeyCls) || keyCls.equals(pojoValCls)) {
                            MessageBox.warningDialog(owner, "Key class name must be unique!");

                            keyClsTf.requestFocus();

                            return;
                        }

                        if (valCls.equals(pojoKeyCls) || valCls.equals(pojoValCls)) {
                            MessageBox.warningDialog(owner, "Value class name must be unique!");

                            valClsTf.requestFocus();

                            return;
                        }
                    }

                curPojo.keyClassName(keyCls);
                curPojo.valueClassName(valCls);
            }
        }));

        keyValPnl.setDisable(true);

        genPnl.add(keyValPnl, 2);

        pkgTf = genPnl.addLabeled("Package:", textField("Package that will be used for POJOs generation"), 2);

        outFolderTf = genPnl.addLabeled("Output Folder:", textField("Output folder for POJOs and XML files"));

        genPnl.add(button("...", "Select output folder", new EventHandler<ActionEvent>() {
            /** {@inheritDoc} */
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

        GridPaneEx regexPnl = paneEx(0, 0, 0, 0);
        regexPnl.addColumn();
        regexPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        regexPnl.addColumn();
        regexPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);

        regexPnl.add(new Label("Replace Ignite name for selected or all tables:"), 4);
        regexTf = regexPnl.addLabeled("  Regexp:", textField("Regular expression. For example: (\\w+)"));
        replaceTf = regexPnl.addLabeled("  Replace with:", textField("Replace text. For example: $1_Suffix"));

        final Button renBtn = button("Rename", "Replace Ignite names by provided regular expression for current table",
            new EventHandler<ActionEvent>() {
                /** {@inheritDoc} */
                @Override public void handle(ActionEvent evt) {
                    if (curPojo == null) {
                        MessageBox.warningDialog(owner, "Please select table to rename Ignite names!");

                        return;
                    }

                    if (checkInput(regexTf, false, "Regular expression should not be empty!"))
                        return;

                    String regex = regexTf.getText();

                    String replace = replaceTf.getText();

                    try {
                        for (PojoField field : curPojo.fields())
                            field.javaName(field.javaName().replaceAll(regex, replace));
                    }
                    catch (Exception e) {
                        MessageBox.errorDialog(owner, "Failed to rename Ignite names!", e);
                    }
                }
            });
        renBtn.setDisable(true);

        final Button revertBtn = button("Revert", "Revert changes to Ignite names for current table", new EventHandler<ActionEvent>() {
            /** {@inheritDoc} */
            @Override public void handle(ActionEvent evt) {
                if (curPojo != null)
                    curPojo.revertJavaNames();
                else
                    MessageBox.warningDialog(owner, "Please select table to revert changes to Ignite names!");
            }
        });
        revertBtn.setDisable(true);

        regexPnl.add(buttonsPane(Pos.BOTTOM_RIGHT, false,
            renBtn,
            button("Rename All", "Replace Ignite names by provided regular expression for all selected tables",
                new EventHandler<ActionEvent>() {
                    /** {@inheritDoc} */
                    @Override public void handle(ActionEvent evt) {
                        if (checkInput(regexTf, false, "Regular expression should not be empty!"))
                            return;

                        Collection<PojoDescriptor> selItems = selectedItems();

                        if (selItems.isEmpty()) {
                            MessageBox.warningDialog(owner, "Please select tables to rename Ignite names!");

                            return;
                        }

                        if (!MessageBox.confirmDialog(owner,
                            "Are you sure you want to rename Ignite names in all selected tables?"))
                            return;

                        String regex = regexTf.getText();

                        String replace = replaceTf.getText();

                        try {
                            for (PojoDescriptor pojo : selItems)
                                for (PojoField field : pojo.fields())
                                    field.javaName(field.javaName().replaceAll(regex, replace));
                        }
                        catch (Exception e) {
                            MessageBox.errorDialog(owner, "Failed to rename Ignite names!", e);
                        }
                    }
                }),
            revertBtn,
            button("Revert All", "Revert changes to Ignite names for all selected tables", new EventHandler<ActionEvent>() {
                /** {@inheritDoc} */
                @Override public void handle(ActionEvent evt) {
                    Collection<PojoDescriptor> selItems = selectedItems();

                    if (selItems.isEmpty()) {
                        MessageBox.warningDialog(owner, "Please select tables to revert Ignite names!");

                        return;
                    }

                    if (!MessageBox.confirmDialog(owner,
                        "Are you sure you want to revert Ignite names for all selected tables?"))
                        return;

                    for (PojoDescriptor pojo : selItems)
                        pojo.revertJavaNames();
                }
            })
        ), 4);

        tree.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<TreeItem<String>>() {
            /** {@inheritDoc} */
            @Override public void changed(ObservableValue<? extends TreeItem<String>> val, TreeItem<String> oldItem,
                TreeItem<String> newItem) {
                if (newItem != null && newItem.getParent() != null && newItem.isLeaf()) {
                    curPojo = schemas.get(newItem.getParent().getValue()).get(newItem.getValue());

                    tbl.setItems(curPojo.fields());
                    tbl.getSelectionModel().select(0);

                    keyClsTf.setText(curPojo.keyClassName());
                    valClsTf.setText(curPojo.valueClassName());

                    keyValPnl.setDisable(false);

                    renBtn.setDisable(false);
                    revertBtn.setDisable(false);
                }
                else {
                    curPojo = null;
                    tbl.setItems(NO_DATA);

                    keyClsTf.setText("");
                    valClsTf.setText("");

                    keyValPnl.setDisable(true);

                    renBtn.setDisable(true);
                    revertBtn.setDisable(true);
                    upBtn.setDisable(true);
                    downBtn.setDisable(true);
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

        for (TreeItem<String> schemeItem : rootItem.getChildren())
            for (TreeItem<String> tblItem : schemeItem.getChildren())
                if (((CheckBoxTreeItem)tblItem).isSelected())
                    res.add(schemas.get(schemeItem.getValue()).get(tblItem.getValue()));

        return res;
    }

    /** {@inheritDoc} */
    @Override public void start(Stage primaryStage) {
        owner = primaryStage;

        primaryStage.setTitle("Schema Load");

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

        primaryStage.setWidth(600);
        primaryStage.setMinWidth(600);

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
     * @param name Source name.
     * @return String converted to java class name notation.
     */
    private String toJavaClassName(String name) {
        int len = name.length();

        StringBuilder buf = new StringBuilder(len);

        boolean capitalizeNext = true;

        for (int i = 0; i < len; i++) {
            char ch = name.charAt(i);

            if (Character.isWhitespace(ch) || '_' == ch)
                capitalizeNext = true;
            else if (capitalizeNext) {
                buf.append(Character.toUpperCase(ch));

                capitalizeNext = false;
            }
            else
                buf.append(Character.toLowerCase(ch));
        }

        return buf.toString();
    }

    /**
     * @param name Source name.
     * @return String converted to java field name notation.
     */
    private String toJavaFieldName(String name) {
        String javaName = toJavaClassName(name);

        return Character.toLowerCase(javaName.charAt(0)) + javaName.substring(1);
    }

    /**
     * Convert JDBC data type to java type.
     *
     * @param type JDBC SQL data type.
     * @return Java data type.
     */
    private Class<?> dataType(int type) {
        switch (type) {
            case BIT:
            case BOOLEAN:
                return Boolean.class;

            case TINYINT:
                return Byte.class;

            case SMALLINT:
                return Short.class;

            case INTEGER:
                return Integer.class;

            case BIGINT:
                return Long.class;

            case REAL:
                return Float.class;

            case FLOAT:
            case DOUBLE:
                return Double.class;

            case NUMERIC:
            case DECIMAL:
                return BigDecimal.class;

            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
                return String.class;

            case DATE:
                return Date.class;

            case TIME:
                return Time.class;

            case TIMESTAMP:
                return Timestamp.class;

            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case ARRAY:
            case BLOB:
            case CLOB:
            case NCLOB:
                return Array.class;

            case NULL:
                return Void.class;

            case DATALINK:
                return URL.class;

            // OTHER, JAVA_OBJECT, DISTINCT, STRUCT, REF, ROWID, SQLXML
            default:
                return Object.class;
        }
    }

    /**
     * Parse database metadata.
     *
     * @param meta Database metadata.
     * @param catalog Catalog name.
     * @param schema Schema name.
     * @param tbl Table name.
     * @return New initialized instance of {@code GridCacheQueryTypeMetadata}.
     * @throws SQLException If parsing failed.
     */
    private PojoDescriptor parse(DatabaseMetaData meta, String catalog, String schema, String tbl) throws SQLException {
        GridCacheQueryTypeMetadata res = new GridCacheQueryTypeMetadata();

        res.setSchema(schema);
        res.setTableName(tbl);

        res.setType(toJavaClassName(tbl));
        res.setKeyType(res.getType() + "Key");

        Collection<GridCacheQueryTypeDescriptor> keyDescs = res.getKeyDescriptors();
        Collection<GridCacheQueryTypeDescriptor> valDescs = res.getValueDescriptors();

        Map<String, Class<?>> qryFields = res.getQueryFields();
        Map<String, Class<?>> ascFields = res.getAscendingFields();
        Map<String, Class<?>> descFields = res.getDescendingFields();
        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> groups = res.getGroups();

        Set<String> pkFlds = new LinkedHashSet<>();

        try (ResultSet pk = meta.getPrimaryKeys(catalog, schema, tbl)) {
            while (pk.next())
                pkFlds.add(pk.getString(4));
        }

        try (ResultSet flds = meta.getColumns(catalog, schema, tbl, null)) {
            while (flds.next()) {
                String dbName = flds.getString(4);
                int dbType = flds.getInt(5);

                String javaName = toJavaFieldName(dbName);
                Class<?> javaType = dataType(dbType);

                GridCacheQueryTypeDescriptor desc = new GridCacheQueryTypeDescriptor(javaName, javaType, dbName, dbType);

                if (pkFlds.contains(dbName))
                    keyDescs.add(desc);
                else
                    valDescs.add(desc);

                qryFields.put(javaName, javaType);
            }
        }

        try (ResultSet idxs = meta.getIndexInfo(catalog, schema, tbl, false, true)) {
            while (idxs.next()) {
                String idx = toJavaFieldName(idxs.getString(6));
                String col = toJavaFieldName(idxs.getString(9));
                String askOrDesc = idxs.getString(10);

                LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> idxCols = groups.get(idx);

                if (idxCols == null) {
                    idxCols = new LinkedHashMap<>();

                    groups.put(idx, idxCols);
                }

                Class<?> dataType = qryFields.get(col);

                Boolean desc = askOrDesc != null ? "D".equals(askOrDesc) : null;

                if (desc != null) {
                    if (desc)
                        descFields.put(col, dataType);
                    else
                        ascFields.put(col, dataType);
                }

                idxCols.put(col, new IgniteBiTuple<Class<?>, Boolean>(dataType, desc));
            }
        }

        return new PojoDescriptor(res);
    }

    /**
     * Parse database metadata.
     *
     * @param conn Connection to database.
     * @return Map with schemes and tables metadata.
     * @throws SQLException If parsing failed.
     */
    private Map<String, Map<String, PojoDescriptor>> parse(Connection conn) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        Map<String, Map<String, PojoDescriptor>> res = new TreeMap<>();

        try (ResultSet schemas = meta.getSchemas()) {
            while (schemas.next()) {
                String schema = schemas.getString(1);

                // Skip system tables from INFORMATION_SCHEMA.
                if ("INFORMATION_SCHEMA".equalsIgnoreCase(schema))
                    continue;

                String catalog = schemas.getString(2);

                Map<String, PojoDescriptor> items = new TreeMap<>();

                try (ResultSet tbls = meta.getTables(catalog, schema, "%", null)) {
                    while (tbls.next()) {
                        String tbl = tbls.getString(3);

                        items.put(tbl, parse(meta, catalog, schema, tbl));
                    }
                }

                if (!items.isEmpty())
                    res.put(schema, items);
            }
        }

        return res;
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
     * Field descriptor with properties for JavaFX GUI bindings.
     */
    public static class PojoField {
        /** If this field belongs to primary key. */
        private final BooleanProperty key;

        /** Field name for POJO. */
        private final StringProperty javaName;

        /** Field type for POJO. */
        private final StringProperty javaTypeName;

        /** Field name in database. */
        private final StringProperty dbName;

        /** Field type in database. */
        private final StringProperty dbTypeName;

        /** Field type descriptor. */
        private final GridCacheQueryTypeDescriptor desc;

        /** List of possible java type conversions. */
        private final ObservableList<String> conversions;

        /** */
        private static final Map<String, Class<?>> classesMap = new HashMap<>();

        /**
         * @param clss Class to add.
         */
        private static void fillClassesMap(Class<?>... clss) {
            for (Class<?> cls : clss)
                classesMap.put(cls.getName(), cls);
        }

        /**
         * @param clss List of classes to get class names.
         * @return List of classes names to show in UI for manual select.
         */
        private static List<String> classNames(Class<?>... clss) {
            List<String> names = new ArrayList<>(clss.length);

            for (Class<?> cls : clss)
                names.add(cls.getName());

            return names;
        }

        /** Null number conversions. */
        private static final ObservableList<String> NULL_NUM_CONVERSIONS = FXCollections.observableArrayList();

        /** Not null number conversions. */
        private static final ObservableList<String> NOT_NULL_NUM_CONVERSIONS = FXCollections.observableArrayList();

        static {
            List<String> primitives = classNames(boolean.class, byte.class, short.class,
                int.class, long.class, float.class, double.class);

            List<String> objects = classNames(Boolean.class, Byte.class, Short.class, Integer.class,
                Long.class, Float.class, Double.class, BigDecimal.class);

            NULL_NUM_CONVERSIONS.addAll(objects);

            NOT_NULL_NUM_CONVERSIONS.addAll(primitives);
            NOT_NULL_NUM_CONVERSIONS.addAll(objects);

            fillClassesMap(boolean.class, Boolean.class,
                byte.class, Byte.class,
                short.class, Short.class,
                int.class, Integer.class,
                long.class, Long.class,
                float.class, Float.class,
                double.class, Double.class,
                BigDecimal.class,
                String.class,
                java.sql.Date.class, java.sql.Time.class, java.sql.Timestamp.class,
                Array.class, Void.class, URL.class, Object.class);
        }

        /**
         * @param dbType Database type.
         * @param nullable Nullable.
         * @param dflt Default.
         * @return List of possible type conversions.
         */
        private static ObservableList<String> conversions(int dbType, boolean nullable, String dflt) {
            switch (dbType) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case REAL:
                case FLOAT:
                case DOUBLE:
                    return nullable ? NULL_NUM_CONVERSIONS : NOT_NULL_NUM_CONVERSIONS;

                default:
                    return FXCollections.singletonObservableList(dflt);
            }
        }

        /**
         * @param key {@code true} if this field belongs to primary key.
         * @param desc Field type descriptor.
         */
        public PojoField(boolean key, GridCacheQueryTypeDescriptor desc) {
            this.desc = desc;
            this.key = new SimpleBooleanProperty(key);

            javaName = new SimpleStringProperty(desc.getJavaName());

            String typeName = desc.getJavaType().getName();

            javaTypeName = new SimpleStringProperty(typeName);

            dbName = new SimpleStringProperty(desc.getDbName());

            dbTypeName = new SimpleStringProperty(jdbcTypeName(desc.getDbType()));

            boolean nullable = true;

            conversions = conversions(desc.getDbType(), nullable, typeName);
        }

        /**
         * @param jdbcType String name for JDBC type.
         */
        private String jdbcTypeName(int jdbcType) {
            switch (jdbcType) {
                case BIT:
                    return "BIT";
                case TINYINT:
                    return "TINYINT";
                case SMALLINT:
                    return "SMALLINT";
                case INTEGER:
                    return "INTEGER";
                case BIGINT:
                    return "BIGINT";
                case FLOAT:
                    return "FLOAT";
                case REAL:
                    return "REAL";
                case DOUBLE:
                    return "DOUBLE";
                case NUMERIC:
                    return "NUMERIC";
                case DECIMAL:
                    return "DECIMAL";
                case CHAR:
                    return "CHAR";
                case VARCHAR:
                    return "VARCHAR";
                case LONGVARCHAR:
                    return "LONGVARCHAR";
                case DATE:
                    return "DATE";
                case TIME:
                    return "TIME";
                case TIMESTAMP:
                    return "TIMESTAMP";
                case BINARY:
                    return "BINARY";
                case VARBINARY:
                    return "VARBINARY";
                case LONGVARBINARY:
                    return "LONGVARBINARY";
                case NULL:
                    return "NULL";
                case OTHER:
                    return "OTHER";
                case JAVA_OBJECT:
                    return "JAVA_OBJECT";
                case DISTINCT:
                    return "DISTINCT";
                case STRUCT:
                    return "STRUCT";
                case ARRAY:
                    return "ARRAY";
                case BLOB:
                    return "BLOB";
                case CLOB:
                    return "CLOB";
                case REF:
                    return "REF";
                case DATALINK:
                    return "DATALINK";
                case BOOLEAN:
                    return "BOOLEAN";
                case ROWID:
                    return "ROWID";
                case NCHAR:
                    return "NCHAR";
                case NVARCHAR:
                    return "NVARCHAR";
                case LONGNVARCHAR:
                    return "LONGNVARCHAR";
                case NCLOB:
                    return "NCLOB";
                case SQLXML:
                    return "SQLXML";
                default:
                    return "Unknown";
            }
        }

        /**
         * @return {@code true} if this field belongs to primary key.
         */
        public boolean key() {
            return key.get();
        }

        /**
         * @param pk {@code true} if this field belongs to primary key.
         */
        public void key(boolean pk) {
            key.set(pk);
        }

        /**
         * @return POJO field java name.
         */
        public String javaName() {
            return javaName.get();
        }

        /**
         * @param name POJO field java name.
         */
        public void javaName(String name) {
            javaName.set(name);
        }

        /**
         * @return POJO field java type name.
         */
        public String javaTypeName() {
            return javaTypeName.get();
        }

        /**
         * @return Type descriptor.
         */
        public GridCacheQueryTypeDescriptor descriptor() {
            desc.setJavaName(javaName.get());
            desc.setJavaType(classesMap.get(javaTypeName()));

            return desc;
        }

        /**
         * @return POJO field JDBC type in database.
         */
        public int dbType() {
            return desc.getDbType();
        }

        /**
         * @return Boolean property support for {@code key} property.
         */
        public BooleanProperty keyProperty() {
            return key;
        }

        /**
         * @return String property support for {@code javaName} property.
         */
        public StringProperty javaNameProperty() {
            return javaName;
        }

        /**
         * @return String property support for {@code javaTypeName} property.
         */
        public StringProperty javaTypeNameProperty() {
            return javaTypeName;
        }

        /**
         * @return String property support for {@code dbName} property.
         */
        public StringProperty dbNameProperty() {
            return dbName;
        }

        /**
         * @return String property support for {@code dbName} property.
         */
        public StringProperty dbTypeNameProperty() {
            return dbTypeName;
        }

        /**
         * @return List of possible java type conversions.
         */
        public ObservableList<String> conversions() {
            return conversions;
        }
    }

    /**
     * Descriptor for java type.
     */
    private static class PojoDescriptor {
        /** Previous name for key class. */
        private final String keyClsNamePrev;

        /** Previous name for value class. */
        private final String valClsNamePrev;

        /** Java class fields. */
        private final ObservableList<PojoField> fields;

        /** Java class fields. */
        private final List<PojoField> fieldsPrev;

        /** Type metadata. */
        private final GridCacheQueryTypeMetadata meta;

        /**
         * Type descriptor.
         *
         * @param meta Type metadata.
         */
        public PojoDescriptor(GridCacheQueryTypeMetadata meta) {
            this.meta = meta;

            keyClsNamePrev = meta.getKeyType();

            valClsNamePrev = meta.getType();

            Collection<GridCacheQueryTypeDescriptor> keys = meta.getKeyDescriptors();

            Collection<GridCacheQueryTypeDescriptor> vals = meta.getValueDescriptors();

            int sz = keys.size() + vals.size();

            List<PojoField> flds = new ArrayList<>(sz);
            fieldsPrev = new ArrayList<>(sz);

            for (GridCacheQueryTypeDescriptor key : keys) {
                flds.add(new PojoField(true, key));
                fieldsPrev.add(new PojoField(true, key));
            }

            for (GridCacheQueryTypeDescriptor val : vals) {
                flds.add(new PojoField(false, val));
                fieldsPrev.add(new PojoField(false, val));
            }

            fields = FXCollections.observableList(flds);
        }

        /**
         * @return {@code true} if descriptor was changed by user via GUI.
         */
        public boolean changed() {
            boolean diff = !meta.getKeyType().equals(keyClsNamePrev) || !meta.getType().equals(valClsNamePrev);

            if (!diff)
                for (int i = 0; i < fields.size(); i++) {
                    PojoField cur = fields.get(i);
                    PojoField prev = fieldsPrev.get(i);

                    // User can change via GUI only key and java name properties.
                    if (cur.key() != prev.key() || !cur.javaName().equals(prev.javaName())) {
                        diff = true;

                        break;
                    }
                }

            return diff;
        }

        /**
         * Revert changes to java names made by user.
         */
        public void revertJavaNames() {
            for (int i = 0; i < fields.size(); i++)
                fields.get(i).javaName(fieldsPrev.get(i).javaName());
        }

        /**
         * @return Name for key class.
         */
        public String keyClassName() {
            return meta.getKeyType();
        }

        /**
         * @param name Name for key class.
         */
        public void keyClassName(String name) {
            meta.setKeyType(name);
        }

        /**
         * @return Name for value class.
         */
        public String valueClassName() {
            return meta.getType();
        }

        /**
         * @param name Name for value class.
         */
        public void valueClassName(String name) {
            meta.setType(name);
        }

        /**
         * @return Java class fields.
         */
        public ObservableList<PojoField> fields() {
            return fields;
        }

        /**
         * @return Type metadata.
         */
        public GridCacheQueryTypeMetadata metadata() {
            return meta;
        }
    }

    /**
     * Special table cell to select possible java type conversions.
     */
    private static class JavaTypeCell extends TableCell<PojoField, String> {
        /** Combo box. */
        private final ComboBox<String> comboBox;

        /** Creates a ComboBox cell factory for use in TableColumn controls. */
        public static Callback<TableColumn<PojoField, String>, TableCell<PojoField, String>> forTableColumn() {
            return new Callback<TableColumn<PojoField, String>, TableCell<PojoField, String>>() {
                public TableCell<PojoField, String> call(TableColumn<PojoField, String> col) {
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
                comboBox.getSelectionModel().select(this.getItem());

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

                PojoField pojo = (PojoField)getTableRow().getItem();

                if (pojo != null) {
                    comboBox.setItems(pojo.conversions());

                    comboBox.getSelectionModel().select(pojo.javaTypeName());
                }
            }
        }
    }
}
