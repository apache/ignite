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

import java.awt.Desktop;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.embed.swing.SwingFXUtils;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.geometry.VPos;
import javafx.scene.Node;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.PasswordField;
import javafx.scene.control.ProgressIndicator;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.scene.control.TitledPane;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Pane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import javafx.stage.Screen;
import javafx.stage.Stage;
import javafx.util.Callback;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.schema.generator.CodeGenerator;
import org.apache.ignite.schema.generator.XmlGenerator;
import org.apache.ignite.schema.model.PojoDescriptor;
import org.apache.ignite.schema.model.PojoField;
import org.apache.ignite.schema.model.SchemaDescriptor;
import org.apache.ignite.schema.parser.DatabaseMetadataParser;
import org.apache.ignite.schema.parser.DbMetadataReader;

/**
 * Schema Import utility application.
 */
@SuppressWarnings("UnnecessaryFullyQualifiedName")
public class SchemaImportApp extends Application {
    /** Logger. */
    private static final Logger log = Logger.getLogger(SchemaImportApp.class.getName());

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
    private static final String PREF_GENERATE_ALIASES = "sql.aliases";

    /** */
    private static final String PREF_XML_SINGLE = "xml.single";

    /** */
    private static final String PREF_NAMING_PATTERN = "naming.pattern";

    /** */
    private static final String PREF_NAMING_REPLACE = "naming.replace";

    /** Empty POJO fields model. */
    private static final ObservableList<PojoField> NO_FIELDS = FXCollections.emptyObservableList();

    /** Ability to use xdg-open utility flag. */
    private static final boolean HAS_XDG_OPEN = U.isUnix() && new File("/usr/bin/xdg-open").canExecute();

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
    private ListView<SchemaDescriptor> schemaLst;

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
    private CheckBox generateAliasesCh;

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

    /** */
    private ObservableList<SchemaDescriptor> schemas = FXCollections.emptyObservableList();

    /** List with POJOs descriptors. */
    private ObservableList<PojoDescriptor> pojos = FXCollections.emptyObservableList();

    /** Currently selected POJO. */
    private PojoDescriptor curPojo;

    /** Application preferences. */
    private final Properties prefs = new Properties();

    /** File path for storing on local file system. */
    private final File prefsFile = new File(System.getProperty("user.home"), ".ignite-schema-import");

    /** */
    private final ExecutorService exec = Executors.newSingleThreadExecutor(new ThreadFactory() {
        /** {@inheritDoc} */
        @Override public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "ignite-schema-import-worker");

            t.setDaemon(true);

            return t;
        }
    });

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
                    .invoke(osxApp, SwingFXUtils.fromFXImage(Controls.image("ignite", 128), null));
            }
            catch (Exception ignore) {
                // No-op.
            }

            // Workaround for JDK 7/JavaFX 2 application on Mac OSX El Capitan.
            try {
                Class<?> fontFinderCls = Class.forName("com.sun.t2k.MacFontFinder");

                Field psNameToPathMap = fontFinderCls.getDeclaredField("psNameToPathMap");

                psNameToPathMap.setAccessible(true);
                psNameToPathMap.set(null, new HashMap<String, String>());
            }
            catch (Exception ignore) {
                // No-op.
            }
        }

        launch(args);
    }

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
     * Open connection to database.
     *
     * @return Connection to database.
     * @throws SQLException If connection failed.
     */
    private Connection connect() throws SQLException {
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

        return DbMetadataReader.getInstance().connect(jdbcDrvJarPath, jdbcDrvCls, jdbcUrl, jdbcInfo);
    }

    /**
     * Fill tree with database metadata.
     */
    private void fill() {
        final List<String> selSchemas = new ArrayList<>();

        for (SchemaDescriptor schema: schemas)
            if (schema.selected().getValue())
                selSchemas.add(schema.schema());

        if (selSchemas.isEmpty())
            if (!MessageBox.confirmDialog(owner, "No schemas selected.\nExtract tables for all available schemas?"))
                return;

        lockUI(connLayerPnl, connPnl, nextBtn);

        final String jdbcUrl = jdbcUrlTf.getText();

        final boolean tblsOnly = parseCb.getSelectionModel().getSelectedIndex() == 0;

        Runnable task = new Task<Void>() {
            /** {@inheritDoc} */
            @Override protected Void call() throws Exception {
                long started = System.currentTimeMillis();

                try (Connection conn = connect()) {
                    pojos = DatabaseMetadataParser.parse(conn, selSchemas, tblsOnly);
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
                    Controls.tooltip(nextBtn, "Generate XML and POJO files");
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
     * Load schemas list from database.
     */
    private void loadSchemas() {
        lockUI(connLayerPnl, connPnl, nextBtn);

        final String jdbcUrl = jdbcUrlTf.getText();

        Runnable task = new Task<Void>() {
            /** {@inheritDoc} */
            @Override protected Void call() throws Exception {
                long started = System.currentTimeMillis();

                try (Connection conn = connect()) {
                    schemas = DatabaseMetadataParser.schemas(conn);
                }

                perceptualDelay(started);

                return null;
            }

            /** {@inheritDoc} */
            @Override protected void succeeded() {
                try {
                    super.succeeded();

                    schemaLst.setItems(schemas);

                    if (schemas.isEmpty()) {
                        MessageBox.warningDialog(owner, "No schemas found in database. Recheck JDBC URL.\n" +
                            "JDBC URL: " +  jdbcUrl);

                        return;
                    }

                    nextBtn.setDisable(false);
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

                MessageBox.errorDialog(owner, "Failed to get schemas list from database.", getException());
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

        if (!checkInput(outFolderTf, true, "Output folder should not be empty!"))
            return;

        lockUI(genLayerPnl, genPnl, prevBtn, nextBtn);

        final String outFolder = outFolderTf.getText();

        final String pkg = pkgTf.getText();

        final File destFolder = new File(outFolder);

        final boolean constructor = pojoConstructorCh.isSelected();

        final boolean includeKeys = pojoIncludeKeysCh.isSelected();

        final boolean generateAliases = generateAliasesCh.isSelected();

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
                        XmlGenerator.generate(pkg, pojo, includeKeys, generateAliases,
                            new File(destFolder, pojo.table() + ".xml"), askOverwrite);

                    CodeGenerator.pojos(pojo, outFolder, pkg, constructor, includeKeys, askOverwrite);
                }

                if (singleXml)
                    XmlGenerator.generate(pkg, all, includeKeys, generateAliases,
                        new File(outFolder, "ignite-type-metadata.xml"), askOverwrite);

                CodeGenerator.snippet(all, pkg, includeKeys, generateAliases, outFolder, askOverwrite);

                perceptualDelay(started);

                return null;
            }

            /**
             * Running of command with reading of first printed line.
             *
             * @param cmdLine Process to run.
             * @return First printed by command line.
             */
            private String execAndReadLine(Process cmdLine) {
                InputStream stream = cmdLine.getInputStream();
                Charset cs = Charset.defaultCharset();

                try(BufferedReader reader = new BufferedReader(
                        cs == null ? new InputStreamReader(stream) : new InputStreamReader(stream, cs))) {
                    return reader.readLine();
                }
                catch (IOException ignored){
                    return null;
                }
            }

            /**
             * Start specified command in separate process.
             *
             * @param commands Executable file and command parameters in array.
             * @return Process instance for run command.
             */
            private Process startProcess(List<String> commands) throws IOException {
                ProcessBuilder builder = new ProcessBuilder(commands);

                Map<String, String> environment = builder.environment();

                environment.clear();

                environment.putAll(System.getenv());

                return builder.start();
            }

            /**
             * Convert specified command parameters to system specific parameters.
             *
             * @param cmd Path to executable file.
             * @param parameters Params for created process.
             * @return List of converted system specific parameters.
             */
            private List<String> toCommandLine(String cmd, String... parameters) {
                boolean isWin = U.isWindows();

                List<String> params = new ArrayList<>(parameters.length + 1);

                params.add(cmd.replace('/', File.separatorChar).replace('\\', File.separatorChar));

                for (String parameter: parameters) {
                    if (isWin) {
                        if (parameter.contains("\"")) params.add(parameter.replace("\"", "\\\""));
                        else if (parameter.isEmpty()) params.add("\"\"");
                        else params.add(parameter);
                    }
                    else
                        params.add(parameter);
                }

                return params;
            }

            /**
             * Create process for run specified command.
             *
             * @param execPath Path to executable file.
             * @param params Params for created process.
             * @return Process instance for run command.
             */
            private Process createProcess(String execPath, String... params) throws IOException {
                if (F.isEmpty(execPath))
                    throw new IllegalArgumentException("Executable not specified");

                return startProcess(toCommandLine(execPath, params));
            }

            /**
             * Compare two version strings.
             *
             * @param v1 Version string 1.
             * @param v2 Version string 2.
             * @return The value 0 if the argument version is equal to this version.
             * A value less than 0 if this version is less than the version argument.
             * A value greater than 0 if this version is greater than the version argument.
             */
            private int compareVersionNumbers(String v1, String v2) {
                if (v1 == null && v2 == null)
                    return 0;

                if (v1 == null)
                    return -1;

                if (v2 == null)
                    return 1;

                String[] part1 = v1.split("[._-]");
                String[] part2 = v2.split("[._-]");

                int idx = 0;

                while (idx < part1.length && idx < part2.length) {
                    String p1 = part1[idx];
                    String p2 = part2[idx];

                    int cmp = p1.matches("\\d+") && p2.matches("\\d+") ? Integer.valueOf(p1).compareTo(Integer.valueOf(p2)) :
                            part1[idx].compareTo(part2[idx]);

                    if (cmp != 0)
                        return cmp;

                    idx += 1;
                }

                if (part1.length == part2.length)
                    return 0;
                else {
                    boolean left = part1.length > idx;
                    String[] parts = left ? part1 : part2;

                    while (idx < parts.length) {
                        String p = parts[idx];

                        int cmp = p.matches("\\d+") ? Integer.valueOf(p).compareTo(0) : 1;

                        if (cmp != 0) return left ? cmp : -cmp;

                        idx += 1;
                    }

                    return 0;
                }
            }

            /**
             * Check that system has Nautilus.
             * @return {@code True} when Nautilus is installed or {@code false} otherwise.
             * @throws IOException
             */
            private boolean canUseNautilus() throws IOException {
                if (U.isUnix() || new File("/usr/bin/xdg-mime").canExecute() || new File("/usr/bin/nautilus").canExecute()) {
                    String appName = execAndReadLine(createProcess("xdg-mime", "query", "default", "inode/directory"));

                    if (appName == null || !appName.matches("nautilus.*\\.desktop"))
                        return false;
                    else {
                        String ver = execAndReadLine(createProcess("nautilus", "--version"));

                        if (ver != null) {
                            Matcher m = Pattern.compile("GNOME nautilus ([0-9.]+)").matcher(ver);

                            return m.find() && compareVersionNumbers(m.group(1), "3") >= 0;
                        }
                        else
                            return false;
                    }
                }
                else
                    return false;
            }

            /**
             * Open specified folder with selection of specified file in system file manager.
             *
             * @param dir Opened folder.
             */
            private void openFolder(File dir) throws IOException {
                if (U.isWindows())
                    Runtime.getRuntime().exec("explorer /root," + dir.getAbsolutePath());
                else if (U.isMacOs())
                    createProcess("open", dir.getAbsolutePath());
                else if (canUseNautilus())
                    createProcess("nautilus", dir.getAbsolutePath());
                else {
                    String path = dir.getAbsolutePath();

                    if (HAS_XDG_OPEN)
                        createProcess("/usr/bin/xdg-open", path);
                    else if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.OPEN))
                        Desktop.getDesktop().open(new File(path));
                    else
                        MessageBox.warningDialog(owner, "This action isn't supported on the current platform" +
                            ((U.isLinux() || U.isUnix() || U.isSolaris()) ?
                            ".\nTo fix this issue you should install library libgnome2-0." : ""));
                }
            }

            /** {@inheritDoc} */
            @Override protected void succeeded() {
                super.succeeded();

                unlockUI(genLayerPnl, genPnl, prevBtn, nextBtn);

                if (MessageBox.confirmDialog(owner, "Generation complete!\n\n" +
                    "Reveal output folder in system default file browser?"))
                    try {
                        openFolder(destFolder);
                    }
                    catch (Exception e) {
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
        dbIcon = Controls.hBox(0, true, Controls.imageView("data_connection", 48));
        genIcon = Controls.hBox(0, true, Controls.imageView("text_tree", 48));

        titleLb = Controls.label("");
        titleLb.setId("banner");

        subTitleLb = Controls.label("");

        BorderPane bp = Controls.borderPane(null, Controls.vBox(5, titleLb, subTitleLb), null, dbIcon,
            Controls.hBox(0, true, Controls.imageView("ignite", 48)));
        bp.setId("banner");

        return bp;
    }

    /**
     * @return Panel with control buttons.
     */
    private Pane createButtonsPane() {
        prevBtn = Controls.button("Prev", "Go to \"Database connection\" page", new EventHandler<ActionEvent>() {
            @Override public void handle(ActionEvent evt) {
                prev();
            }
        });

        nextBtn = Controls.button("Next", "Go to \"POJO and XML generation\" page", new EventHandler<ActionEvent>() {
            @Override public void handle(ActionEvent evt) {
                next();
            }
        });

        return Controls.buttonsPane(Pos.BOTTOM_RIGHT, true, prevBtn, nextBtn);
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
        Controls.tooltip(nextBtn, "Go to \"XML and POJO generation\" page");
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

            return false;
        }

        return true;
    }

    /**
     * Go to &quot;Generate XML And POJOs&quot; panel or generate XML and POJOs.
     */
    private void next() {
        if (rootPane.getCenter() == connLayerPnl) {
            if (checkInput(jdbcDrvJarTf, true, "Path to JDBC driver is not specified!") &&
                checkInput(jdbcDrvClsTf, true, "JDBC driver class name is not specified!") &&
                checkInput(jdbcUrlTf, true, "JDBC URL connection string is not specified!") &&
                checkInput(userTf, true, "User name is not specified!"))
                fill();
        }
        else
            generate();
    }

    /**
     * Create connection pane with controls.
     *
     * @return Pane with connection controls.
     */
    private Pane createConnectionPane() {
        connPnl = Controls.paneEx(10, 10, 0, 10);

        connPnl.addColumn();
        connPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        connPnl.addColumn(35, 35, 35, Priority.NEVER);

        connPnl.addRows(9);
        connPnl.addRow(100, 100, Double.MAX_VALUE, Priority.ALWAYS);

        connPnl.add(Controls.text("This utility is designed to automatically generate configuration XML files and" +
            " POJO classes from database schema information.", 550), 3);

        connPnl.wrap();

        GridPaneEx presetPnl = Controls.paneEx(0, 0, 0, 0);
        presetPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        presetPnl.addColumn();

        rdbmsCb = presetPnl.add(Controls.comboBox("Select database server to get predefined settings", presets));

        presetPnl.add(Controls.button("Save preset", "Save current settings in preferences", new EventHandler<ActionEvent>() {
            @Override public void handle(ActionEvent evt) {
                Preset preset = rdbmsCb.getSelectionModel().getSelectedItem();

                savePreset(preset);
            }
        }));

        connPnl.add(Controls.label("DB Preset:"));
        connPnl.add(presetPnl, 2);

        jdbcDrvJarTf = connPnl.addLabeled("Driver JAR:", Controls.textField("Path to driver jar"));

        connPnl.add(Controls.button("...", "Select JDBC driver jar or zip", new EventHandler<ActionEvent>() {
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

        jdbcDrvClsTf = connPnl.addLabeled("JDBC Driver:", Controls.textField("Enter class name for JDBC driver"), 2);

        jdbcUrlTf = connPnl.addLabeled("JDBC URL:", Controls.textField("JDBC URL of the database connection string"), 2);

        rdbmsCb.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<Preset>() {
            @Override public void changed(ObservableValue<? extends Preset> val, Preset oldVal, Preset newVal) {
                jdbcDrvJarTf.setText(newVal.jar);
                jdbcDrvClsTf.setText(newVal.drv);
                jdbcUrlTf.setText(newVal.url);
                userTf.setText(newVal.user);
            }
        });

        userTf = connPnl.addLabeled("User:", Controls.textField("User name"), 2);

        pwdTf = connPnl.addLabeled("Password:", Controls.passwordField("User password"), 2);

        parseCb = connPnl.addLabeled("Parse:", Controls.comboBox("Type of tables to parse", "Tables only", "Tables and Views"), 2);

        GridPaneEx schemaPnl = Controls.paneEx(5, 5, 5, 5);
        schemaPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        schemaPnl.addColumn();

        schemaLst = schemaPnl.add(Controls.list("Select schemas to load", new SchemaCell()));

        schemaPnl.wrap();

        schemaPnl.add(Controls.button("Load schemas", "Load schemas for specified database", new EventHandler<ActionEvent>() {
            /** {@inheritDoc} */
            @Override public void handle(ActionEvent evt) {
                loadSchemas();
            }
        }));

        TitledPane titledPnl = connPnl.add(Controls.titledPane("Schemas", schemaPnl, false), 3);

        titledPnl.setExpanded(true);

        GridPaneEx.setValignment(titledPnl, VPos.TOP);

        connLayerPnl = Controls.stackPane(connPnl);

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
        genPnl = Controls.paneEx(10, 10, 0, 10);

        genPnl.addColumn();
        genPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        genPnl.addColumn(35, 35, 35, Priority.NEVER);

        genPnl.addRow(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        genPnl.addRows(8);

        TableColumn<PojoDescriptor, Boolean> useCol = Controls.customColumn("Schema / Table", "use",
            "If checked then this table will be used for XML and POJOs generation", PojoDescriptorCell.cellFactory());

        TableColumn<PojoDescriptor, String> keyClsCol = Controls.textColumn("Key Class Name", "keyClassName", "Key class name",
            new TextColumnValidator<PojoDescriptor>() {
                @Override public boolean valid(PojoDescriptor rowVal, String newVal) {
                    boolean valid = checkClassName(rowVal, newVal, true);

                    if (valid)
                        rowVal.keyClassName(newVal);

                    return valid;
                }
            });

        TableColumn<PojoDescriptor, String> valClsCol = Controls.textColumn("Value Class Name", "valueClassName",
            "Value class name", new TextColumnValidator<PojoDescriptor>() {
                @Override public boolean valid(PojoDescriptor rowVal, String newVal) {
                    boolean valid = checkClassName(rowVal, newVal, false);

                    if (valid)
                        rowVal.valueClassName(newVal);

                    return valid;
                }
            });

        pojosTbl = Controls.tableView("Tables not found in database", useCol, keyClsCol, valClsCol);

        TableColumn<PojoField, Boolean> useFldCol = Controls.customColumn("Use", "use",
            "Check to use this field for XML and POJO generation\n" +
            "Note that NOT NULL columns cannot be unchecked", PojoFieldUseCell.cellFactory());
        useFldCol.setMinWidth(50);
        useFldCol.setMaxWidth(50);

        TableColumn<PojoField, Boolean> keyCol = Controls.booleanColumn("Key", "key",
            "Check to include this field into key object");

        TableColumn<PojoField, Boolean> akCol = Controls.booleanColumn("AK", "affinityKey",
            "Check to annotate key filed with @AffinityKeyMapped annotation in generated POJO class\n" +
            "Note that a class can have only ONE key field annotated with @AffinityKeyMapped annotation");

        TableColumn<PojoField, String> dbNameCol = Controls.tableColumn("DB Name", "dbName", "Field name in database");

        TableColumn<PojoField, String> dbTypeNameCol = Controls.tableColumn("DB Type", "dbTypeName", "Field type in database");

        TableColumn<PojoField, String> javaNameCol = Controls.textColumn("Java Name", "javaName", "Field name in POJO class",
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

        TableColumn<PojoField, String> javaTypeNameCol = Controls.customColumn("Java Type", "javaTypeName",
            "Field java type in POJO class", JavaTypeCell.cellFactory());

        fieldsTbl = Controls.tableView("Select table to see table columns",
            useFldCol, keyCol, akCol, dbNameCol, dbTypeNameCol, javaNameCol, javaTypeNameCol);

        genPnl.add(Controls.splitPane(pojosTbl, fieldsTbl, 0.6), 3);

        final GridPaneEx keyValPnl = Controls.paneEx(0, 0, 0, 0);
        keyValPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        keyValPnl.addColumn();
        keyValPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        keyValPnl.addColumn();

        pkgTf = genPnl.addLabeled("Package:", Controls.textField("Package that will be used for POJOs generation"), 2);

        outFolderTf = genPnl.addLabeled("Output Folder:", Controls.textField("Output folder for XML and POJOs files"));

        genPnl.add(Controls.button("...", "Select output folder", new EventHandler<ActionEvent>() {
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

        pojoIncludeKeysCh = genPnl.add(Controls.checkBox("Include key fields into value POJOs",
            "If selected then include key fields into value object", true), 3);

        pojoConstructorCh = genPnl.add(Controls.checkBox("Generate constructors for POJOs",
            "If selected then generate empty and full constructors for POJOs", false), 3);

        generateAliasesCh = genPnl.add(Controls.checkBox("Generate aliases for SQL fields",
            "If selected then generate aliases for SQL fields with db names", true), 3);

        xmlSingleFileCh = genPnl.add(Controls.checkBox("Write all configurations to a single XML file",
            "If selected then all configurations will be saved into the file 'ignite-type-metadata.xml'", true), 3);

        GridPaneEx regexPnl = Controls.paneEx(5, 5, 5, 5);
        regexPnl.addColumn();
        regexPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);
        regexPnl.addColumn();
        regexPnl.addColumn(100, 100, Double.MAX_VALUE, Priority.ALWAYS);

        regexTf = regexPnl.addLabeled("  Regexp:", Controls.textField("Regular expression. For example: (\\w+)"));

        replaceTf = regexPnl.addLabeled("  Replace with:", Controls.textField("Replace text. For example: $1_SomeText"));

        final ComboBox<String> replaceCb = regexPnl.addLabeled("  Replace:", Controls.comboBox("Replacement target",
            "Key class names", "Value class names", "Java names"));

        regexPnl.add(Controls.buttonsPane(Pos.CENTER_LEFT, false,
            Controls.button("Rename Selected", "Replaces each substring of this string that matches the given regular expression" +
                    " with the given replacement",
                new EventHandler<ActionEvent>() {
                    @Override public void handle(ActionEvent evt) {
                        if (!checkInput(regexTf, false, "Regular expression should not be empty!"))
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
            Controls.button("Reset Selected", "Revert changes for selected items to initial auto-generated values",
                new EventHandler<ActionEvent>() {
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

        genPnl.add(Controls.titledPane("Rename \"Key class name\", \"Value class name\" or  \"Java name\" for selected tables",
            regexPnl, true), 3);

        genLayerPnl = Controls.stackPane(genPnl);
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
                        customPrefsFileName);
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
            Controls.image("ignite", 16),
            Controls.image("ignite", 24),
            Controls.image("ignite", 32),
            Controls.image("ignite", 48),
            Controls.image("ignite", 64),
            Controls.image("ignite", 128));

        pi = Controls.progressIndicator(50);

        createGeneratePane();

        hdrPane = createHeaderPane();
        rootPane = Controls.borderPane(hdrPane, createConnectionPane(), createButtonsPane(), null, null);

        primaryStage.setScene(Controls.scene(rootPane));

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
        generateAliasesCh.setSelected(getBoolProp(PREF_GENERATE_ALIASES, true));

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
        setBoolProp(PREF_GENERATE_ALIASES, generateAliasesCh.isSelected());

        setBoolProp(PREF_XML_SINGLE, xmlSingleFileCh.isSelected());

        setStringProp(PREF_NAMING_PATTERN, regexTf.getText());
        setStringProp(PREF_NAMING_REPLACE, replaceTf.getText());

        savePreferences();
    }

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
     * Special list view cell to select loaded schemas.
     */
    private static class SchemaCell implements Callback<SchemaDescriptor, ObservableValue<Boolean>> {
        /** {@inheritDoc} */
        @Override public ObservableValue<Boolean> call(SchemaDescriptor item) {
            return item.selected();
        }
    }

    /**
     * Special table cell to select schema or table.
     */
    private static class PojoDescriptorCell extends TableCell<PojoDescriptor, Boolean> {
        /** Previous POJO bound to cell. */
        private PojoDescriptor prevPojo;

        /** Previous cell graphic. */
        private Pane prevGraphic;

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
        /** Previous POJO field bound to cell. */
        private PojoField prevField;

        /** Previous cell graphic. */
        private CheckBox prevGraphic;

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
