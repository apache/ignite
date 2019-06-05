/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.h2.api.ErrorCode;
import org.h2.bnf.Bnf;
import org.h2.bnf.context.DbColumn;
import org.h2.bnf.context.DbContents;
import org.h2.bnf.context.DbSchema;
import org.h2.bnf.context.DbTableOrView;
import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.DbException;
import org.h2.security.SHA256;
import org.h2.tools.Backup;
import org.h2.tools.ChangeFileEncryption;
import org.h2.tools.ConvertTraceFile;
import org.h2.tools.CreateCluster;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Recover;
import org.h2.tools.Restore;
import org.h2.tools.RunScript;
import org.h2.tools.Script;
import org.h2.tools.SimpleResultSet;
import org.h2.util.JdbcUtils;
import org.h2.util.New;
import org.h2.util.Profiler;
import org.h2.util.ScriptReader;
import org.h2.util.SortedProperties;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.util.Tool;
import org.h2.util.Utils;

/**
 * For each connection to a session, an object of this class is created.
 * This class is used by the H2 Console.
 */
public class WebApp {

    /**
     * The web server.
     */
    protected final WebServer server;

    /**
     * The session.
     */
    protected WebSession session;

    /**
     * The session attributes
     */
    protected Properties attributes;

    /**
     * The mime type of the current response.
     */
    protected String mimeType;

    /**
     * Whether the response can be cached.
     */
    protected boolean cache;

    /**
     * Whether to close the connection.
     */
    protected boolean stop;

    /**
     * The language in the HTTP header.
     */
    protected String headerLanguage;

    private Profiler profiler;

    WebApp(WebServer server) {
        this.server = server;
    }

    /**
     * Set the web session and attributes.
     *
     * @param session the session
     * @param attributes the attributes
     */
    void setSession(WebSession session, Properties attributes) {
        this.session = session;
        this.attributes = attributes;
    }

    /**
     * Process an HTTP request.
     *
     * @param file the file that was requested
     * @param hostAddr the host address
     * @return the name of the file to return to the client
     */
    String processRequest(String file, String hostAddr) {
        int index = file.lastIndexOf('.');
        String suffix;
        if (index >= 0) {
            suffix = file.substring(index + 1);
        } else {
            suffix = "";
        }
        if ("ico".equals(suffix)) {
            mimeType = "image/x-icon";
            cache = true;
        } else if ("gif".equals(suffix)) {
            mimeType = "image/gif";
            cache = true;
        } else if ("css".equals(suffix)) {
            cache = true;
            mimeType = "text/css";
        } else if ("html".equals(suffix) ||
                "do".equals(suffix) ||
                "jsp".equals(suffix)) {
            cache = false;
            mimeType = "text/html";
            if (session == null) {
                session = server.createNewSession(hostAddr);
                if (!"notAllowed.jsp".equals(file)) {
                    file = "index.do";
                }
            }
        } else if ("js".equals(suffix)) {
            cache = true;
            mimeType = "text/javascript";
        } else {
            cache = true;
            mimeType = "application/octet-stream";
        }
        trace("mimeType=" + mimeType);
        trace(file);
        if (file.endsWith(".do")) {
            file = process(file);
        }
        return file;
    }

    private static String getComboBox(String[] elements, String selected) {
        StringBuilder buff = new StringBuilder();
        for (String value : elements) {
            buff.append("<option value=\"").
                append(PageParser.escapeHtmlData(value)).
                append('\"');
            if (value.equals(selected)) {
                buff.append(" selected");
            }
            buff.append('>').
                append(PageParser.escapeHtml(value)).
                append("</option>");
        }
        return buff.toString();
    }

    private static String getComboBox(String[][] elements, String selected) {
        StringBuilder buff = new StringBuilder();
        for (String[] n : elements) {
            buff.append("<option value=\"").
                append(PageParser.escapeHtmlData(n[0])).
                append('\"');
            if (n[0].equals(selected)) {
                buff.append(" selected");
            }
            buff.append('>').
                append(PageParser.escapeHtml(n[1])).
                append("</option>");
        }
        return buff.toString();
    }

    private String process(String file) {
        trace("process " + file);
        while (file.endsWith(".do")) {
            if ("login.do".equals(file)) {
                file = login();
            } else if ("index.do".equals(file)) {
                file = index();
            } else if ("logout.do".equals(file)) {
                file = logout();
            } else if ("settingRemove.do".equals(file)) {
                file = settingRemove();
            } else if ("settingSave.do".equals(file)) {
                file = settingSave();
            } else if ("test.do".equals(file)) {
                file = test();
            } else if ("query.do".equals(file)) {
                file = query();
            } else if ("tables.do".equals(file)) {
                file = tables();
            } else if ("editResult.do".equals(file)) {
                file = editResult();
            } else if ("getHistory.do".equals(file)) {
                file = getHistory();
            } else if ("admin.do".equals(file)) {
                file = admin();
            } else if ("adminSave.do".equals(file)) {
                file = adminSave();
            } else if ("adminStartTranslate.do".equals(file)) {
                file = adminStartTranslate();
            } else if ("adminShutdown.do".equals(file)) {
                file = adminShutdown();
            } else if ("autoCompleteList.do".equals(file)) {
                file = autoCompleteList();
            } else if ("tools.do".equals(file)) {
                file = tools();
            } else {
                file = "error.jsp";
            }
        }
        trace("return " + file);
        return file;
    }

    private String autoCompleteList() {
        String query = (String) attributes.get("query");
        boolean lowercase = false;
        if (query.trim().length() > 0 &&
                Character.isLowerCase(query.trim().charAt(0))) {
            lowercase = true;
        }
        try {
            String sql = query;
            if (sql.endsWith(";")) {
                sql += " ";
            }
            ScriptReader reader = new ScriptReader(new StringReader(sql));
            reader.setSkipRemarks(true);
            String lastSql = "";
            while (true) {
                String n = reader.readStatement();
                if (n == null) {
                    break;
                }
                lastSql = n;
            }
            String result = "";
            if (reader.isInsideRemark()) {
                if (reader.isBlockRemark()) {
                    result = "1#(End Remark)# */\n" + result;
                } else {
                    result = "1#(Newline)#\n" + result;
                }
            } else {
                sql = lastSql;
                while (sql.length() > 0 && sql.charAt(0) <= ' ') {
                    sql = sql.substring(1);
                }
                if (sql.trim().length() > 0 && Character.isLowerCase(sql.trim().charAt(0))) {
                    lowercase = true;
                }
                Bnf bnf = session.getBnf();
                if (bnf == null) {
                    return "autoCompleteList.jsp";
                }
                HashMap<String, String> map = bnf.getNextTokenList(sql);
                String space = "";
                if (sql.length() > 0) {
                    char last = sql.charAt(sql.length() - 1);
                    if (!Character.isWhitespace(last) && (last != '.' &&
                            last >= ' ' && last != '\'' && last != '"')) {
                        space = " ";
                    }
                }
                ArrayList<String> list = new ArrayList<>(map.size());
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String type = "" + key.charAt(0);
                    if (Integer.parseInt(type) > 2) {
                        continue;
                    }
                    key = key.substring(2);
                    if (Character.isLetter(key.charAt(0)) && lowercase) {
                        key = StringUtils.toLowerEnglish(key);
                        value = StringUtils.toLowerEnglish(value);
                    }
                    if (key.equals(value) && !".".equals(value)) {
                        value = space + value;
                    }
                    key = StringUtils.urlEncode(key);
                    key = key.replace('+', ' ');
                    value = StringUtils.urlEncode(value);
                    value = value.replace('+', ' ');
                    list.add(type + "#" + key + "#" + value);
                }
                Collections.sort(list);
                if (query.endsWith("\n") || query.trim().endsWith(";")) {
                    list.add(0, "1#(Newline)#\n");
                }
                StatementBuilder buff = new StatementBuilder();
                for (String s : list) {
                    buff.appendExceptFirst("|");
                    buff.append(s);
                }
                result = buff.toString();
            }
            session.put("autoCompleteList", result);
        } catch (Throwable e) {
            server.traceError(e);
        }
        return "autoCompleteList.jsp";
    }

    private String admin() {
        session.put("port", "" + server.getPort());
        session.put("allowOthers", "" + server.getAllowOthers());
        session.put("ssl", String.valueOf(server.getSSL()));
        session.put("sessions", server.getSessions());
        return "admin.jsp";
    }

    private String adminSave() {
        try {
            Properties prop = new SortedProperties();
            int port = Integer.decode((String) attributes.get("port"));
            prop.setProperty("webPort", String.valueOf(port));
            server.setPort(port);
            boolean allowOthers = Utils.parseBoolean((String) attributes.get("allowOthers"), false, false);
            prop.setProperty("webAllowOthers", String.valueOf(allowOthers));
            server.setAllowOthers(allowOthers);
            boolean ssl = Utils.parseBoolean((String) attributes.get("ssl"), false, false);
            prop.setProperty("webSSL", String.valueOf(ssl));
            server.setSSL(ssl);
            server.saveProperties(prop);
        } catch (Exception e) {
            trace(e.toString());
        }
        return admin();
    }

    private String tools() {
        try {
            String toolName = (String) attributes.get("tool");
            session.put("tool", toolName);
            String args = (String) attributes.get("args");
            String[] argList = StringUtils.arraySplit(args, ',', false);
            Tool tool = null;
            if ("Backup".equals(toolName)) {
                tool = new Backup();
            } else if ("Restore".equals(toolName)) {
                tool = new Restore();
            } else if ("Recover".equals(toolName)) {
                tool = new Recover();
            } else if ("DeleteDbFiles".equals(toolName)) {
                tool = new DeleteDbFiles();
            } else if ("ChangeFileEncryption".equals(toolName)) {
                tool = new ChangeFileEncryption();
            } else if ("Script".equals(toolName)) {
                tool = new Script();
            } else if ("RunScript".equals(toolName)) {
                tool = new RunScript();
            } else if ("ConvertTraceFile".equals(toolName)) {
                tool = new ConvertTraceFile();
            } else if ("CreateCluster".equals(toolName)) {
                tool = new CreateCluster();
            } else {
                throw DbException.throwInternalError(toolName);
            }
            ByteArrayOutputStream outBuff = new ByteArrayOutputStream();
            PrintStream out = new PrintStream(outBuff, false, "UTF-8");
            tool.setOut(out);
            try {
                tool.runTool(argList);
                out.flush();
                String o = new String(outBuff.toByteArray(), StandardCharsets.UTF_8);
                String result = PageParser.escapeHtml(o);
                session.put("toolResult", result);
            } catch (Exception e) {
                session.put("toolResult", getStackTrace(0, e, true));
            }
        } catch (Exception e) {
            server.traceError(e);
        }
        return "tools.jsp";
    }

    private String adminStartTranslate() {
        Map<?, ?> p = Map.class.cast(session.map.get("text"));
        @SuppressWarnings("unchecked")
        Map<Object, Object> p2 = (Map<Object, Object>) p;
        String file = server.startTranslate(p2);
        session.put("translationFile", file);
        return "helpTranslate.jsp";
    }

    /**
     * Stop the application and the server.
     *
     * @return the page to display
     */
    protected String adminShutdown() {
        server.shutdown();
        return "admin.jsp";
    }

    private String index() {
        String[][] languageArray = WebServer.LANGUAGES;
        String language = (String) attributes.get("language");
        Locale locale = session.locale;
        if (language != null) {
            if (locale == null || !StringUtils.toLowerEnglish(
                    locale.getLanguage()).equals(language)) {
                locale = new Locale(language, "");
                server.readTranslations(session, locale.getLanguage());
                session.put("language", language);
                session.locale = locale;
            }
        } else {
            language = (String) session.get("language");
        }
        if (language == null) {
            // if the language is not yet known
            // use the last header
            language = headerLanguage;
        }
        session.put("languageCombo", getComboBox(languageArray, language));
        String[] settingNames = server.getSettingNames();
        String setting = attributes.getProperty("setting");
        if (setting == null && settingNames.length > 0) {
            setting = settingNames[0];
        }
        String combobox = getComboBox(settingNames, setting);
        session.put("settingsList", combobox);
        ConnectionInfo info = server.getSetting(setting);
        if (info == null) {
            info = new ConnectionInfo();
        }
        session.put("setting", PageParser.escapeHtmlData(setting));
        session.put("name", PageParser.escapeHtmlData(setting));
        session.put("driver", PageParser.escapeHtmlData(info.driver));
        session.put("url", PageParser.escapeHtmlData(info.url));
        session.put("user", PageParser.escapeHtmlData(info.user));
        return "index.jsp";
    }

    private String getHistory() {
        int id = Integer.parseInt(attributes.getProperty("id"));
        String sql = session.getCommand(id);
        session.put("query", PageParser.escapeHtmlData(sql));
        return "query.jsp";
    }

    private static int addColumns(boolean mainSchema, DbTableOrView table,
            StringBuilder buff, int treeIndex, boolean showColumnTypes,
            StringBuilder columnsBuffer) {
        DbColumn[] columns = table.getColumns();
        for (int i = 0; columns != null && i < columns.length; i++) {
            DbColumn column = columns[i];
            if (columnsBuffer.length() > 0) {
                columnsBuffer.append(' ');
            }
            columnsBuffer.append(column.getName());
            String col = escapeIdentifier(column.getName());
            String level = mainSchema ? ", 1, 1" : ", 2, 2";
            buff.append("setNode(").append(treeIndex).append(level)
                    .append(", 'column', '")
                    .append(PageParser.escapeJavaScript(column.getName()))
                    .append("', 'javascript:ins(\\'").append(col).append("\\')');\n");
            treeIndex++;
            if (mainSchema && showColumnTypes) {
                buff.append("setNode(").append(treeIndex)
                        .append(", 2, 2, 'type', '")
                        .append(PageParser.escapeJavaScript(column.getDataType()))
                        .append("', null);\n");
                treeIndex++;
            }
        }
        return treeIndex;
    }

    private static String escapeIdentifier(String name) {
        return StringUtils.urlEncode(
                PageParser.escapeJavaScript(name)).replace('+', ' ');
    }

    /**
     * This class represents index information for the GUI.
     */
    static class IndexInfo {

        /**
         * The index name.
         */
        String name;

        /**
         * The index type name.
         */
        String type;

        /**
         * The indexed columns.
         */
        String columns;
    }

    private static int addIndexes(boolean mainSchema, DatabaseMetaData meta,
            String table, String schema, StringBuilder buff, int treeIndex)
            throws SQLException {
        ResultSet rs;
        try {
            rs = meta.getIndexInfo(null, schema, table, false, true);
        } catch (SQLException e) {
            // SQLite
            return treeIndex;
        }
        HashMap<String, IndexInfo> indexMap = new HashMap<>();
        while (rs.next()) {
            String name = rs.getString("INDEX_NAME");
            IndexInfo info = indexMap.get(name);
            if (info == null) {
                int t = rs.getInt("TYPE");
                String type;
                if (t == DatabaseMetaData.tableIndexClustered) {
                    type = "";
                } else if (t == DatabaseMetaData.tableIndexHashed) {
                    type = " (${text.tree.hashed})";
                } else if (t == DatabaseMetaData.tableIndexOther) {
                    type = "";
                } else {
                    type = null;
                }
                if (name != null && type != null) {
                    info = new IndexInfo();
                    info.name = name;
                    type = (rs.getBoolean("NON_UNIQUE") ?
                            "${text.tree.nonUnique}" : "${text.tree.unique}") + type;
                    info.type = type;
                    info.columns = rs.getString("COLUMN_NAME");
                    indexMap.put(name, info);
                }
            } else {
                info.columns += ", " + rs.getString("COLUMN_NAME");
            }
        }
        rs.close();
        if (indexMap.size() > 0) {
            String level = mainSchema ? ", 1, 1" : ", 2, 1";
            String levelIndex = mainSchema ? ", 2, 1" : ", 3, 1";
            String levelColumnType = mainSchema ? ", 3, 2" : ", 4, 2";
            buff.append("setNode(").append(treeIndex).append(level)
                    .append(", 'index_az', '${text.tree.indexes}', null);\n");
            treeIndex++;
            for (IndexInfo info : indexMap.values()) {
                buff.append("setNode(").append(treeIndex).append(levelIndex)
                        .append(", 'index', '")
                        .append(PageParser.escapeJavaScript(info.name))
                        .append("', null);\n");
                treeIndex++;
                buff.append("setNode(").append(treeIndex).append(levelColumnType)
                        .append(", 'type', '").append(info.type).append("', null);\n");
                treeIndex++;
                buff.append("setNode(").append(treeIndex).append(levelColumnType)
                        .append(", 'type', '")
                        .append(PageParser.escapeJavaScript(info.columns))
                        .append("', null);\n");
                treeIndex++;
            }
        }
        return treeIndex;
    }

    private int addTablesAndViews(DbSchema schema, boolean mainSchema,
            StringBuilder buff, int treeIndex) throws SQLException {
        if (schema == null) {
            return treeIndex;
        }
        Connection conn = session.getConnection();
        DatabaseMetaData meta = session.getMetaData();
        int level = mainSchema ? 0 : 1;
        boolean showColumns = mainSchema || !schema.isSystem;
        String indentation = ", " + level + ", " + (showColumns ? "1" : "2") + ", ";
        String indentNode = ", " + (level + 1) + ", 2, ";
        DbTableOrView[] tables = schema.getTables();
        if (tables == null) {
            return treeIndex;
        }
        boolean isOracle = schema.getContents().isOracle();
        boolean notManyTables = tables.length < SysProperties.CONSOLE_MAX_TABLES_LIST_INDEXES;
        for (DbTableOrView table : tables) {
            if (table.isView()) {
                continue;
            }
            int tableId = treeIndex;
            String tab = table.getQuotedName();
            if (!mainSchema) {
                tab = schema.quotedName + "." + tab;
            }
            tab = escapeIdentifier(tab);
            buff.append("setNode(").append(treeIndex).append(indentation)
                    .append(" 'table', '")
                    .append(PageParser.escapeJavaScript(table.getName()))
                    .append("', 'javascript:ins(\\'").append(tab).append("\\',true)');\n");
            treeIndex++;
            if (mainSchema || showColumns) {
                StringBuilder columnsBuffer = new StringBuilder();
                treeIndex = addColumns(mainSchema, table, buff, treeIndex,
                        notManyTables, columnsBuffer);
                if (!isOracle && notManyTables) {
                    treeIndex = addIndexes(mainSchema, meta, table.getName(),
                            schema.name, buff, treeIndex);
                }
                buff.append("addTable('")
                        .append(PageParser.escapeJavaScript(table.getName())).append("', '")
                        .append(PageParser.escapeJavaScript(columnsBuffer.toString())).append("', ")
                        .append(tableId).append(");\n");
            }
        }
        tables = schema.getTables();
        for (DbTableOrView view : tables) {
            if (!view.isView()) {
                continue;
            }
            int tableId = treeIndex;
            String tab = view.getQuotedName();
            if (!mainSchema) {
                tab = view.getSchema().quotedName + "." + tab;
            }
            tab = escapeIdentifier(tab);
            buff.append("setNode(").append(treeIndex).append(indentation)
                    .append(" 'view', '")
                    .append(PageParser.escapeJavaScript(view.getName()))
                    .append("', 'javascript:ins(\\'").append(tab).append("\\',true)');\n");
            treeIndex++;
            if (mainSchema) {
                StringBuilder columnsBuffer = new StringBuilder();
                treeIndex = addColumns(mainSchema, view, buff,
                        treeIndex, notManyTables, columnsBuffer);
                if (schema.getContents().isH2()) {

                    try (PreparedStatement prep = conn.prepareStatement("SELECT * FROM " +
                                "INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME=?")) {
                        prep.setString(1, view.getName());
                        ResultSet rs = prep.executeQuery();
                        if (rs.next()) {
                            String sql = rs.getString("SQL");
                            buff.append("setNode(").append(treeIndex)
                                    .append(indentNode)
                                    .append(" 'type', '")
                                    .append(PageParser.escapeJavaScript(sql))
                                    .append("', null);\n");
                            treeIndex++;
                        }
                        rs.close();
                    }
                }
                buff.append("addTable('")
                        .append(PageParser.escapeJavaScript(view.getName())).append("', '")
                        .append(PageParser.escapeJavaScript(columnsBuffer.toString())).append("', ")
                        .append(tableId).append(");\n");
            }
        }
        return treeIndex;
    }

    private String tables() {
        DbContents contents = session.getContents();
        boolean isH2 = false;
        try {
            String url = (String) session.get("url");
            Connection conn = session.getConnection();
            contents.readContents(url, conn);
            session.loadBnf();
            isH2 = contents.isH2();

            StringBuilder buff = new StringBuilder()
                    .append("setNode(0, 0, 0, 'database', '")
                    .append(PageParser.escapeJavaScript(url))
                    .append("', null);\n");
            int treeIndex = 1;

            DbSchema defaultSchema = contents.getDefaultSchema();
            treeIndex = addTablesAndViews(defaultSchema, true, buff, treeIndex);
            DbSchema[] schemas = contents.getSchemas();
            for (DbSchema schema : schemas) {
                if (schema == defaultSchema || schema == null) {
                    continue;
                }
                buff.append("setNode(").append(treeIndex).append(", 0, 1, 'folder', '")
                        .append(PageParser.escapeJavaScript(schema.name))
                        .append("', null);\n");
                treeIndex++;
                treeIndex = addTablesAndViews(schema, false, buff, treeIndex);
            }
            if (isH2) {
                try (Statement stat = conn.createStatement()) {
                    ResultSet rs = stat.executeQuery("SELECT * FROM " +
                            "INFORMATION_SCHEMA.SEQUENCES ORDER BY SEQUENCE_NAME");
                    for (int i = 0; rs.next(); i++) {
                        if (i == 0) {
                            buff.append("setNode(").append(treeIndex)
                                    .append(", 0, 1, 'sequences', '${text.tree.sequences}', null);\n");
                            treeIndex++;
                        }
                        String name = rs.getString("SEQUENCE_NAME");
                        String current = rs.getString("CURRENT_VALUE");
                        String increment = rs.getString("INCREMENT");
                        buff.append("setNode(").append(treeIndex)
                                .append(", 1, 1, 'sequence', '")
                                .append(PageParser.escapeJavaScript(name))
                                .append("', null);\n");
                        treeIndex++;
                        buff.append("setNode(").append(treeIndex)
                                .append(", 2, 2, 'type', '${text.tree.current}: ")
                                .append(PageParser.escapeJavaScript(current))
                                .append("', null);\n");
                        treeIndex++;
                        if (!"1".equals(increment)) {
                            buff.append("setNode(").append(treeIndex)
                                    .append(", 2, 2, 'type', '${text.tree.increment}: ")
                                    .append(PageParser.escapeJavaScript(increment))
                                    .append("', null);\n");
                            treeIndex++;
                        }
                    }
                    rs.close();
                    rs = stat.executeQuery("SELECT * FROM " +
                            "INFORMATION_SCHEMA.USERS ORDER BY NAME");
                    for (int i = 0; rs.next(); i++) {
                        if (i == 0) {
                            buff.append("setNode(").append(treeIndex)
                                    .append(", 0, 1, 'users', '${text.tree.users}', null);\n");
                            treeIndex++;
                        }
                        String name = rs.getString("NAME");
                        String admin = rs.getString("ADMIN");
                        buff.append("setNode(").append(treeIndex)
                                .append(", 1, 1, 'user', '")
                                .append(PageParser.escapeJavaScript(name))
                                .append("', null);\n");
                        treeIndex++;
                        if (admin.equalsIgnoreCase("TRUE")) {
                            buff.append("setNode(").append(treeIndex)
                                    .append(", 2, 2, 'type', '${text.tree.admin}', null);\n");
                            treeIndex++;
                        }
                    }
                    rs.close();
                }
            }
            DatabaseMetaData meta = session.getMetaData();
            String version = meta.getDatabaseProductName() + " " +
                    meta.getDatabaseProductVersion();
            buff.append("setNode(").append(treeIndex)
                    .append(", 0, 0, 'info', '")
                    .append(PageParser.escapeJavaScript(version))
                    .append("', null);\n")
                    .append("refreshQueryTables();");
            session.put("tree", buff.toString());
        } catch (Exception e) {
            session.put("tree", "");
            session.put("error", getStackTrace(0, e, isH2));
        }
        return "tables.jsp";
    }

    private String getStackTrace(int id, Throwable e, boolean isH2) {
        try {
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            String stackTrace = writer.toString();
            stackTrace = PageParser.escapeHtml(stackTrace);
            if (isH2) {
                stackTrace = linkToSource(stackTrace);
            }
            stackTrace = StringUtils.replaceAll(stackTrace, "\t",
                    "&nbsp;&nbsp;&nbsp;&nbsp;");
            String message = PageParser.escapeHtml(e.getMessage());
            String error = "<a class=\"error\" href=\"#\" " +
                    "onclick=\"var x=document.getElementById('st" + id +
                    "').style;x.display=x.display==''?'none':'';\">" + message +
                    "</a>";
            if (e instanceof SQLException) {
                SQLException se = (SQLException) e;
                error += " " + se.getSQLState() + "/" + se.getErrorCode();
                if (isH2) {
                    int code = se.getErrorCode();
                    error += " <a href=\"http://h2database.com/javadoc/" +
                            "org/h2/api/ErrorCode.html#c" + code +
                            "\">(${text.a.help})</a>";
                }
            }
            error += "<span style=\"display: none;\" id=\"st" + id +
                    "\"><br />" + stackTrace + "</span>";
            error = formatAsError(error);
            return error;
        } catch (OutOfMemoryError e2) {
            server.traceError(e);
            return e.toString();
        }
    }

    private static String linkToSource(String s) {
        try {
            StringBuilder result = new StringBuilder(s.length());
            int idx = s.indexOf("<br />");
            result.append(s, 0, idx);
            while (true) {
                int start = s.indexOf("org.h2.", idx);
                if (start < 0) {
                    result.append(s.substring(idx));
                    break;
                }
                result.append(s, idx, start);
                int end = s.indexOf(')', start);
                if (end < 0) {
                    result.append(s.substring(idx));
                    break;
                }
                String element = s.substring(start, end);
                int open = element.lastIndexOf('(');
                int dotMethod = element.lastIndexOf('.', open - 1);
                int dotClass = element.lastIndexOf('.', dotMethod - 1);
                String packageName = element.substring(0, dotClass);
                int colon = element.lastIndexOf(':');
                String file = element.substring(open + 1, colon);
                String lineNumber = element.substring(colon + 1, element.length());
                String fullFileName = packageName.replace('.', '/') + "/" + file;
                result.append("<a href=\"http://h2database.com/html/source.html?file=");
                result.append(fullFileName);
                result.append("&line=");
                result.append(lineNumber);
                result.append("&build=");
                result.append(Constants.BUILD_ID);
                result.append("\">");
                result.append(element);
                result.append("</a>");
                idx = end;
            }
            return result.toString();
        } catch (Throwable t) {
            return s;
        }
    }

    private static String formatAsError(String s) {
        return "<div class=\"error\">" + s + "</div>";
    }

    private String test() {
        String driver = attributes.getProperty("driver", "");
        String url = attributes.getProperty("url", "");
        String user = attributes.getProperty("user", "");
        String password = attributes.getProperty("password", "");
        session.put("driver", driver);
        session.put("url", url);
        session.put("user", user);
        boolean isH2 = url.startsWith("jdbc:h2:");
        try {
            long start = System.currentTimeMillis();
            String profOpen = "", profClose = "";
            Profiler prof = new Profiler();
            prof.startCollecting();
            Connection conn;
            try {
                conn = server.getConnection(driver, url, user, password);
            } finally {
                prof.stopCollecting();
                profOpen = prof.getTop(3);
            }
            prof = new Profiler();
            prof.startCollecting();
            try {
                JdbcUtils.closeSilently(conn);
            } finally {
                prof.stopCollecting();
                profClose = prof.getTop(3);
            }
            long time = System.currentTimeMillis() - start;
            String success;
            if (time > 1000) {
                success = "<a class=\"error\" href=\"#\" " +
                    "onclick=\"var x=document.getElementById('prof').style;x." +
                    "display=x.display==''?'none':'';\">" +
                    "${text.login.testSuccessful}</a>" +
                    "<span style=\"display: none;\" id=\"prof\"><br />" +
                    PageParser.escapeHtml(profOpen) +
                    "<br />" +
                    PageParser.escapeHtml(profClose) +
                    "</span>";
            } else {
                success = "<div class=\"success\">${text.login.testSuccessful}</div>";
            }
            session.put("error", success);
            // session.put("error", "${text.login.testSuccessful}");
            return "login.jsp";
        } catch (Exception e) {
            session.put("error", getLoginError(e, isH2));
            return "login.jsp";
        }
    }

    /**
     * Get the formatted login error message.
     *
     * @param e the exception
     * @param isH2 if the current database is a H2 database
     * @return the formatted error message
     */
    private String getLoginError(Exception e, boolean isH2) {
        if (e instanceof JdbcSQLException &&
                ((JdbcSQLException) e).getErrorCode() == ErrorCode.CLASS_NOT_FOUND_1) {
            return "${text.login.driverNotFound}<br />" + getStackTrace(0, e, isH2);
        }
        return getStackTrace(0, e, isH2);
    }

    private String login() {
        String driver = attributes.getProperty("driver", "");
        String url = attributes.getProperty("url", "");
        String user = attributes.getProperty("user", "");
        String password = attributes.getProperty("password", "");
        session.put("autoCommit", "checked");
        session.put("autoComplete", "1");
        session.put("maxrows", "1000");
        boolean isH2 = url.startsWith("jdbc:h2:");
        try {
            Connection conn = server.getConnection(driver, url, user, password);
            session.setConnection(conn);
            session.put("url", url);
            session.put("user", user);
            session.remove("error");
            settingSave();
            return "frame.jsp";
        } catch (Exception e) {
            session.put("error", getLoginError(e, isH2));
            return "login.jsp";
        }
    }

    private String logout() {
        try {
            Connection conn = session.getConnection();
            session.setConnection(null);
            session.remove("conn");
            session.remove("result");
            session.remove("tables");
            session.remove("user");
            session.remove("tool");
            if (conn != null) {
                if (session.getShutdownServerOnDisconnect()) {
                    server.shutdown();
                } else {
                    conn.close();
                }
            }
        } catch (Exception e) {
            trace(e.toString());
        }
        return "index.do";
    }

    private String query() {
        String sql = attributes.getProperty("sql").trim();
        try {
            ScriptReader r = new ScriptReader(new StringReader(sql));
            final ArrayList<String> list = New.arrayList();
            while (true) {
                String s = r.readStatement();
                if (s == null) {
                    break;
                }
                list.add(s);
            }
            final Connection conn = session.getConnection();
            if (SysProperties.CONSOLE_STREAM && server.getAllowChunked()) {
                String page = new String(server.getFile("result.jsp"), StandardCharsets.UTF_8);
                int idx = page.indexOf("${result}");
                // the first element of the list is the header, the last the
                // footer
                list.add(0, page.substring(0, idx));
                list.add(page.substring(idx + "${result}".length()));
                session.put("chunks", new Iterator<String>() {
                    private int i;
                    @Override
                    public boolean hasNext() {
                        return i < list.size();
                    }
                    @Override
                    public String next() {
                        String s = list.get(i++);
                        if (i == 1 || i == list.size()) {
                            return s;
                        }
                        StringBuilder b = new StringBuilder();
                        query(conn, s, i - 1, list.size() - 2, b);
                        return b.toString();
                    }
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                });
                return "result.jsp";
            }
            String result;
            StringBuilder buff = new StringBuilder();
            for (int i = 0; i < list.size(); i++) {
                String s = list.get(i);
                query(conn, s, i, list.size(), buff);
            }
            result = buff.toString();
            session.put("result", result);
        } catch (Throwable e) {
            session.put("result", getStackTrace(0, e, session.getContents().isH2()));
        }
        return "result.jsp";
    }

    /**
     * Execute a query and append the result to the buffer.
     *
     * @param conn the connection
     * @param s the statement
     * @param i the index
     * @param size the number of statements
     * @param buff the target buffer
     */
    void query(Connection conn, String s, int i, int size, StringBuilder buff) {
        if (!(s.startsWith("@") && s.endsWith("."))) {
            buff.append(PageParser.escapeHtml(s + ";")).append("<br />");
        }
        boolean forceEdit = s.startsWith("@edit");
        buff.append(getResult(conn, i + 1, s, size == 1, forceEdit)).
            append("<br />");
    }

    private String editResult() {
        ResultSet rs = session.result;
        int row = Integer.parseInt(attributes.getProperty("row"));
        int op = Integer.parseInt(attributes.getProperty("op"));
        String result = "", error = "";
        try {
            if (op == 1) {
                boolean insert = row < 0;
                if (insert) {
                    rs.moveToInsertRow();
                } else {
                    rs.absolute(row);
                }
                for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                    String x = attributes.getProperty("r" + row + "c" + (i + 1));
                    unescapeData(x, rs, i + 1);
                }
                if (insert) {
                    rs.insertRow();
                } else {
                    rs.updateRow();
                }
            } else if (op == 2) {
                rs.absolute(row);
                rs.deleteRow();
            } else if (op == 3) {
                // cancel
            }
        } catch (Throwable e) {
            result = "<br />" + getStackTrace(0, e, session.getContents().isH2());
            error = formatAsError(e.getMessage());
        }
        String sql = "@edit " + (String) session.get("resultSetSQL");
        Connection conn = session.getConnection();
        result = error + getResult(conn, -1, sql, true, true) + result;
        session.put("result", result);
        return "result.jsp";
    }

    private ResultSet getMetaResultSet(Connection conn, String sql)
            throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        if (isBuiltIn(sql, "@best_row_identifier")) {
            String[] p = split(sql);
            int scale = p[4] == null ? 0 : Integer.parseInt(p[4]);
            boolean nullable = Boolean.parseBoolean(p[5]);
            return meta.getBestRowIdentifier(p[1], p[2], p[3], scale, nullable);
        } else if (isBuiltIn(sql, "@catalogs")) {
            return meta.getCatalogs();
        } else if (isBuiltIn(sql, "@columns")) {
            String[] p = split(sql);
            return meta.getColumns(p[1], p[2], p[3], p[4]);
        } else if (isBuiltIn(sql, "@column_privileges")) {
            String[] p = split(sql);
            return meta.getColumnPrivileges(p[1], p[2], p[3], p[4]);
        } else if (isBuiltIn(sql, "@cross_references")) {
            String[] p = split(sql);
            return meta.getCrossReference(p[1], p[2], p[3], p[4], p[5], p[6]);
        } else if (isBuiltIn(sql, "@exported_keys")) {
            String[] p = split(sql);
            return meta.getExportedKeys(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@imported_keys")) {
            String[] p = split(sql);
            return meta.getImportedKeys(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@index_info")) {
            String[] p = split(sql);
            boolean unique = Boolean.parseBoolean(p[4]);
            boolean approx = Boolean.parseBoolean(p[5]);
            return meta.getIndexInfo(p[1], p[2], p[3], unique, approx);
        } else if (isBuiltIn(sql, "@primary_keys")) {
            String[] p = split(sql);
            return meta.getPrimaryKeys(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@procedures")) {
            String[] p = split(sql);
            return meta.getProcedures(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@procedure_columns")) {
            String[] p = split(sql);
            return meta.getProcedureColumns(p[1], p[2], p[3], p[4]);
        } else if (isBuiltIn(sql, "@schemas")) {
            return meta.getSchemas();
        } else if (isBuiltIn(sql, "@tables")) {
            String[] p = split(sql);
            String[] types = p[4] == null ? null : StringUtils.arraySplit(p[4], ',', false);
            return meta.getTables(p[1], p[2], p[3], types);
        } else if (isBuiltIn(sql, "@table_privileges")) {
            String[] p = split(sql);
            return meta.getTablePrivileges(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@table_types")) {
            return meta.getTableTypes();
        } else if (isBuiltIn(sql, "@type_info")) {
            return meta.getTypeInfo();
        } else if (isBuiltIn(sql, "@udts")) {
            String[] p = split(sql);
            int[] types;
            if (p[4] == null) {
                types = null;
            } else {
                String[] t = StringUtils.arraySplit(p[4], ',', false);
                types = new int[t.length];
                for (int i = 0; i < t.length; i++) {
                    types[i] = Integer.parseInt(t[i]);
                }
            }
            return meta.getUDTs(p[1], p[2], p[3], types);
        } else if (isBuiltIn(sql, "@version_columns")) {
            String[] p = split(sql);
            return meta.getVersionColumns(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@memory")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("Type", Types.VARCHAR, 0, 0);
            rs.addColumn("KB", Types.VARCHAR, 0, 0);
            rs.addRow("Used Memory", "" + Utils.getMemoryUsed());
            rs.addRow("Free Memory", "" + Utils.getMemoryFree());
            return rs;
        } else if (isBuiltIn(sql, "@info")) {
            SimpleResultSet rs = new SimpleResultSet();
            rs.addColumn("KEY", Types.VARCHAR, 0, 0);
            rs.addColumn("VALUE", Types.VARCHAR, 0, 0);
            rs.addRow("conn.getCatalog", conn.getCatalog());
            rs.addRow("conn.getAutoCommit", "" + conn.getAutoCommit());
            rs.addRow("conn.getTransactionIsolation", "" + conn.getTransactionIsolation());
            rs.addRow("conn.getWarnings", "" + conn.getWarnings());
            String map;
            try {
                map = "" + conn.getTypeMap();
            } catch (SQLException e) {
                map = e.toString();
            }
            rs.addRow("conn.getTypeMap", "" + map);
            rs.addRow("conn.isReadOnly", "" + conn.isReadOnly());
            rs.addRow("conn.getHoldability", "" + conn.getHoldability());
            addDatabaseMetaData(rs, meta);
            return rs;
        } else if (isBuiltIn(sql, "@attributes")) {
            String[] p = split(sql);
            return meta.getAttributes(p[1], p[2], p[3], p[4]);
        } else if (isBuiltIn(sql, "@super_tables")) {
            String[] p = split(sql);
            return meta.getSuperTables(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@super_types")) {
            String[] p = split(sql);
            return meta.getSuperTypes(p[1], p[2], p[3]);
        } else if (isBuiltIn(sql, "@prof_stop")) {
            if (profiler != null) {
                profiler.stopCollecting();
                SimpleResultSet rs = new SimpleResultSet();
                rs.addColumn("Top Stack Trace(s)", Types.VARCHAR, 0, 0);
                rs.addRow(profiler.getTop(3));
                profiler = null;
                return rs;
            }
        }
        return null;
    }

    private static void addDatabaseMetaData(SimpleResultSet rs,
            DatabaseMetaData meta) {
        Method[] methods = DatabaseMetaData.class.getDeclaredMethods();
        Arrays.sort(methods, new Comparator<Method>() {
            @Override
            public int compare(Method o1, Method o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });
        for (Method m : methods) {
            if (m.getParameterTypes().length == 0) {
                try {
                    Object o = m.invoke(meta);
                    rs.addRow("meta." + m.getName(), "" + o);
                } catch (InvocationTargetException e) {
                    rs.addRow("meta." + m.getName(), e.getTargetException().toString());
                } catch (Exception e) {
                    rs.addRow("meta." + m.getName(), e.toString());
                }
            }
        }
    }

    private static String[] split(String s) {
        String[] list = new String[10];
        String[] t = StringUtils.arraySplit(s, ' ', true);
        System.arraycopy(t, 0, list, 0, t.length);
        for (int i = 0; i < list.length; i++) {
            if ("null".equals(list[i])) {
                list[i] = null;
            }
        }
        return list;
    }

    private int getMaxrows() {
        String r = (String) session.get("maxrows");
        return r == null ? 0 : Integer.parseInt(r);
    }

    private String getResult(Connection conn, int id, String sql,
            boolean allowEdit, boolean forceEdit) {
        try {
            sql = sql.trim();
            StringBuilder buff = new StringBuilder();
            String sqlUpper = StringUtils.toUpperEnglish(sql);
            if (sqlUpper.contains("CREATE") ||
                    sqlUpper.contains("DROP") ||
                    sqlUpper.contains("ALTER") ||
                    sqlUpper.contains("RUNSCRIPT")) {
                String sessionId = attributes.getProperty("jsessionid");
                buff.append("<script type=\"text/javascript\">parent['h2menu'].location='tables.do?jsessionid=")
                        .append(sessionId).append("';</script>");
            }
            Statement stat;
            DbContents contents = session.getContents();
            if (forceEdit || (allowEdit && contents.isH2())) {
                stat = conn.createStatement(
                        ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
            } else {
                stat = conn.createStatement();
            }
            ResultSet rs;
            long time = System.currentTimeMillis();
            boolean metadata = false;
            int generatedKeys = Statement.NO_GENERATED_KEYS;
            boolean edit = false;
            boolean list = false;
            if (isBuiltIn(sql, "@autocommit_true")) {
                conn.setAutoCommit(true);
                return "${text.result.autoCommitOn}";
            } else if (isBuiltIn(sql, "@autocommit_false")) {
                conn.setAutoCommit(false);
                return "${text.result.autoCommitOff}";
            } else if (isBuiltIn(sql, "@cancel")) {
                stat = session.executingStatement;
                if (stat != null) {
                    stat.cancel();
                    buff.append("${text.result.statementWasCanceled}");
                } else {
                    buff.append("${text.result.noRunningStatement}");
                }
                return buff.toString();
            } else if (isBuiltIn(sql, "@edit")) {
                edit = true;
                sql = sql.substring("@edit".length()).trim();
                session.put("resultSetSQL", sql);
            }
            if (isBuiltIn(sql, "@list")) {
                list = true;
                sql = sql.substring("@list".length()).trim();
            }
            if (isBuiltIn(sql, "@meta")) {
                metadata = true;
                sql = sql.substring("@meta".length()).trim();
            }
            if (isBuiltIn(sql, "@generated")) {
                generatedKeys = Statement.RETURN_GENERATED_KEYS;
                sql = sql.substring("@generated".length()).trim();
            } else if (isBuiltIn(sql, "@history")) {
                buff.append(getCommandHistoryString());
                return buff.toString();
            } else if (isBuiltIn(sql, "@loop")) {
                sql = sql.substring("@loop".length()).trim();
                int idx = sql.indexOf(' ');
                int count = Integer.decode(sql.substring(0, idx));
                sql = sql.substring(idx).trim();
                return executeLoop(conn, count, sql);
            } else if (isBuiltIn(sql, "@maxrows")) {
                int maxrows = (int) Double.parseDouble(
                        sql.substring("@maxrows".length()).trim());
                session.put("maxrows", "" + maxrows);
                return "${text.result.maxrowsSet}";
            } else if (isBuiltIn(sql, "@parameter_meta")) {
                sql = sql.substring("@parameter_meta".length()).trim();
                PreparedStatement prep = conn.prepareStatement(sql);
                buff.append(getParameterResultSet(prep.getParameterMetaData()));
                return buff.toString();
            } else if (isBuiltIn(sql, "@password_hash")) {
                sql = sql.substring("@password_hash".length()).trim();
                String[] p = split(sql);
                return StringUtils.convertBytesToHex(
                        SHA256.getKeyPasswordHash(p[0], p[1].toCharArray()));
            } else if (isBuiltIn(sql, "@prof_start")) {
                if (profiler != null) {
                    profiler.stopCollecting();
                }
                profiler = new Profiler();
                profiler.startCollecting();
                return "Ok";
            } else if (isBuiltIn(sql, "@sleep")) {
                String s = sql.substring("@sleep".length()).trim();
                int sleep = 1;
                if (s.length() > 0) {
                    sleep = Integer.parseInt(s);
                }
                Thread.sleep(sleep * 1000);
                return "Ok";
            } else if (isBuiltIn(sql, "@transaction_isolation")) {
                String s = sql.substring("@transaction_isolation".length()).trim();
                if (s.length() > 0) {
                    int level = Integer.parseInt(s);
                    conn.setTransactionIsolation(level);
                }
                buff.append("Transaction Isolation: ")
                        .append(conn.getTransactionIsolation())
                        .append("<br />");
                buff.append(Connection.TRANSACTION_READ_UNCOMMITTED)
                        .append(": read_uncommitted<br />");
                buff.append(Connection.TRANSACTION_READ_COMMITTED)
                        .append(": read_committed<br />");
                buff.append(Connection.TRANSACTION_REPEATABLE_READ)
                        .append(": repeatable_read<br />");
                buff.append(Connection.TRANSACTION_SERIALIZABLE)
                        .append(": serializable");
            }
            if (sql.startsWith("@")) {
                rs = getMetaResultSet(conn, sql);
                if (rs == null) {
                    buff.append("?: ").append(sql);
                    return buff.toString();
                }
            } else {
                int maxrows = getMaxrows();
                stat.setMaxRows(maxrows);
                session.executingStatement = stat;
                boolean isResultSet = stat.execute(sql, generatedKeys);
                session.addCommand(sql);
                if (generatedKeys == Statement.RETURN_GENERATED_KEYS) {
                    rs = null;
                    rs = stat.getGeneratedKeys();
                } else {
                    if (!isResultSet) {
                        buff.append("${text.result.updateCount}: ")
                                .append(stat.getUpdateCount());
                        time = System.currentTimeMillis() - time;
                        buff.append("<br />(").append(time).append(" ms)");
                        stat.close();
                        return buff.toString();
                    }
                    rs = stat.getResultSet();
                }
            }
            time = System.currentTimeMillis() - time;
            buff.append(getResultSet(sql, rs, metadata, list, edit, time, allowEdit));
            // SQLWarning warning = stat.getWarnings();
            // if (warning != null) {
            // buff.append("<br />Warning:<br />").
            // append(getStackTrace(id, warning));
            // }
            if (!edit) {
                stat.close();
            }
            return buff.toString();
        } catch (Throwable e) {
            // throwable: including OutOfMemoryError and so on
            return getStackTrace(id, e, session.getContents().isH2());
        } finally {
            session.executingStatement = null;
        }
    }

    private static boolean isBuiltIn(String sql, String builtIn) {
        return StringUtils.startsWithIgnoreCase(sql, builtIn);
    }

    private String executeLoop(Connection conn, int count, String sql)
            throws SQLException {
        ArrayList<Integer> params = New.arrayList();
        int idx = 0;
        while (!stop) {
            idx = sql.indexOf('?', idx);
            if (idx < 0) {
                break;
            }
            if (isBuiltIn(sql.substring(idx), "?/*rnd*/")) {
                params.add(1);
                sql = sql.substring(0, idx) + "?" + sql.substring(idx + "/*rnd*/".length() + 1);
            } else {
                params.add(0);
            }
            idx++;
        }
        boolean prepared;
        Random random = new Random(1);
        long time = System.currentTimeMillis();
        if (isBuiltIn(sql, "@statement")) {
            sql = sql.substring("@statement".length()).trim();
            prepared = false;
            Statement stat = conn.createStatement();
            for (int i = 0; !stop && i < count; i++) {
                String s = sql;
                for (Integer type : params) {
                    idx = s.indexOf('?');
                    if (type.intValue() == 1) {
                        s = s.substring(0, idx) + random.nextInt(count) + s.substring(idx + 1);
                    } else {
                        s = s.substring(0, idx) + i + s.substring(idx + 1);
                    }
                }
                if (stat.execute(s)) {
                    ResultSet rs = stat.getResultSet();
                    while (!stop && rs.next()) {
                        // maybe get the data as well
                    }
                    rs.close();
                }
            }
        } else {
            prepared = true;
            PreparedStatement prep = conn.prepareStatement(sql);
            for (int i = 0; !stop && i < count; i++) {
                for (int j = 0; j < params.size(); j++) {
                    Integer type = params.get(j);
                    if (type.intValue() == 1) {
                        prep.setInt(j + 1, random.nextInt(count));
                    } else {
                        prep.setInt(j + 1, i);
                    }
                }
                if (session.getContents().isSQLite()) {
                    // SQLite currently throws an exception on prep.execute()
                    prep.executeUpdate();
                } else {
                    if (prep.execute()) {
                        ResultSet rs = prep.getResultSet();
                        while (!stop && rs.next()) {
                            // maybe get the data as well
                        }
                        rs.close();
                    }
                }
            }
        }
        time = System.currentTimeMillis() - time;
        StatementBuilder buff = new StatementBuilder();
        buff.append(time).append(" ms: ").append(count).append(" * ");
        if (prepared) {
            buff.append("(Prepared) ");
        } else {
            buff.append("(Statement) ");
        }
        buff.append('(');
        for (int p : params) {
            buff.appendExceptFirst(", ");
            buff.append(p == 0 ? "i" : "rnd");
        }
        return buff.append(") ").append(sql).toString();
    }

    private String getCommandHistoryString() {
        StringBuilder buff = new StringBuilder();
        ArrayList<String> history = session.getCommandHistory();
        buff.append("<table cellspacing=0 cellpadding=0>" +
                "<tr><th></th><th>Command</th></tr>");
        for (int i = history.size() - 1; i >= 0; i--) {
            String sql = history.get(i);
            buff.append("<tr><td><a href=\"getHistory.do?id=").
                append(i).
                append("&jsessionid=${sessionId}\" target=\"h2query\" >").
                append("<img width=16 height=16 src=\"ico_write.gif\" " +
                        "onmouseover = \"this.className ='icon_hover'\" ").
                append("onmouseout = \"this.className ='icon'\" " +
                        "class=\"icon\" alt=\"${text.resultEdit.edit}\" ").
                append("title=\"${text.resultEdit.edit}\" border=\"1\"/></a>").
                append("</td><td>").
                append(PageParser.escapeHtml(sql)).
                append("</td></tr>");
        }
        buff.append("</table>");
        return buff.toString();
    }

    private static String getParameterResultSet(ParameterMetaData meta)
            throws SQLException {
        StringBuilder buff = new StringBuilder();
        if (meta == null) {
            return "No parameter meta data";
        }
        buff.append("<table cellspacing=0 cellpadding=0>").
            append("<tr><th>className</th><th>mode</th><th>type</th>").
            append("<th>typeName</th><th>precision</th><th>scale</th></tr>");
        for (int i = 0; i < meta.getParameterCount(); i++) {
            buff.append("</tr><td>").
                append(meta.getParameterClassName(i + 1)).
                append("</td><td>").
                append(meta.getParameterMode(i + 1)).
                append("</td><td>").
                append(meta.getParameterType(i + 1)).
                append("</td><td>").
                append(meta.getParameterTypeName(i + 1)).
                append("</td><td>").
                append(meta.getPrecision(i + 1)).
                append("</td><td>").
                append(meta.getScale(i + 1)).
                append("</td></tr>");
        }
        buff.append("</table>");
        return buff.toString();
    }

    private String getResultSet(String sql, ResultSet rs, boolean metadata,
            boolean list, boolean edit, long time, boolean allowEdit)
            throws SQLException {
        int maxrows = getMaxrows();
        time = System.currentTimeMillis() - time;
        StringBuilder buff = new StringBuilder();
        if (edit) {
            buff.append("<form id=\"editing\" name=\"editing\" method=\"post\" " +
                    "action=\"editResult.do?jsessionid=${sessionId}\" " +
                    "id=\"mainForm\" target=\"h2result\">" +
                    "<input type=\"hidden\" name=\"op\" value=\"1\" />" +
                    "<input type=\"hidden\" name=\"row\" value=\"\" />" +
                    "<table cellspacing=0 cellpadding=0 id=\"editTable\">");
        } else {
            buff.append("<table cellspacing=0 cellpadding=0>");
        }
        if (metadata) {
            SimpleResultSet r = new SimpleResultSet();
            r.addColumn("#", Types.INTEGER, 0, 0);
            r.addColumn("label", Types.VARCHAR, 0, 0);
            r.addColumn("catalog", Types.VARCHAR, 0, 0);
            r.addColumn("schema", Types.VARCHAR, 0, 0);
            r.addColumn("table", Types.VARCHAR, 0, 0);
            r.addColumn("column", Types.VARCHAR, 0, 0);
            r.addColumn("type", Types.INTEGER, 0, 0);
            r.addColumn("typeName", Types.VARCHAR, 0, 0);
            r.addColumn("class", Types.VARCHAR, 0, 0);
            r.addColumn("precision", Types.INTEGER, 0, 0);
            r.addColumn("scale", Types.INTEGER, 0, 0);
            r.addColumn("displaySize", Types.INTEGER, 0, 0);
            r.addColumn("autoIncrement", Types.BOOLEAN, 0, 0);
            r.addColumn("caseSensitive", Types.BOOLEAN, 0, 0);
            r.addColumn("currency", Types.BOOLEAN, 0, 0);
            r.addColumn("nullable", Types.INTEGER, 0, 0);
            r.addColumn("readOnly", Types.BOOLEAN, 0, 0);
            r.addColumn("searchable", Types.BOOLEAN, 0, 0);
            r.addColumn("signed", Types.BOOLEAN, 0, 0);
            r.addColumn("writable", Types.BOOLEAN, 0, 0);
            r.addColumn("definitelyWritable", Types.BOOLEAN, 0, 0);
            ResultSetMetaData m = rs.getMetaData();
            for (int i = 1; i <= m.getColumnCount(); i++) {
                r.addRow(i,
                        m.getColumnLabel(i),
                        m.getCatalogName(i),
                        m.getSchemaName(i),
                        m.getTableName(i),
                        m.getColumnName(i),
                        m.getColumnType(i),
                        m.getColumnTypeName(i),
                        m.getColumnClassName(i),
                        m.getPrecision(i),
                        m.getScale(i),
                        m.getColumnDisplaySize(i),
                        m.isAutoIncrement(i),
                        m.isCaseSensitive(i),
                        m.isCurrency(i),
                        m.isNullable(i),
                        m.isReadOnly(i),
                        m.isSearchable(i),
                        m.isSigned(i),
                        m.isWritable(i),
                        m.isDefinitelyWritable(i));
            }
            rs = r;
        }
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();
        int rows = 0;
        if (list) {
            buff.append("<tr><th>Column</th><th>Data</th></tr><tr>");
            while (rs.next()) {
                if (maxrows > 0 && rows >= maxrows) {
                    break;
                }
                rows++;
                buff.append("<tr><td>Row #</td><td>").
                    append(rows).append("</tr>");
                for (int i = 0; i < columns; i++) {
                    buff.append("<tr><td>").
                        append(PageParser.escapeHtml(meta.getColumnLabel(i + 1))).
                        append("</td><td>").
                        append(escapeData(rs, i + 1)).
                        append("</td></tr>");
                }
            }
        } else {
            buff.append("<tr>");
            if (edit) {
                buff.append("<th>${text.resultEdit.action}</th>");
            }
            for (int i = 0; i < columns; i++) {
                buff.append("<th>").
                    append(PageParser.escapeHtml(meta.getColumnLabel(i + 1))).
                    append("</th>");
            }
            buff.append("</tr>");
            while (rs.next()) {
                if (maxrows > 0 && rows >= maxrows) {
                    break;
                }
                rows++;
                buff.append("<tr>");
                if (edit) {
                    buff.append("<td>").
                        append("<img onclick=\"javascript:editRow(").
                        append(rs.getRow()).
                        append(",'${sessionId}', '${text.resultEdit.save}', " +
                                "'${text.resultEdit.cancel}'").
                        append(")\" width=16 height=16 src=\"ico_write.gif\" " +
                                "onmouseover = \"this.className ='icon_hover'\" " +
                                "onmouseout = \"this.className ='icon'\" " +
                                "class=\"icon\" alt=\"${text.resultEdit.edit}\" " +
                                "title=\"${text.resultEdit.edit}\" border=\"1\"/>").
                        append("<img onclick=\"javascript:deleteRow(").
                        append(rs.getRow()).
                        append(",'${sessionId}', '${text.resultEdit.delete}', " +
                                "'${text.resultEdit.cancel}'").
                        append(")\" width=16 height=16 src=\"ico_remove.gif\" " +
                                "onmouseover = \"this.className ='icon_hover'\" " +
                                "onmouseout = \"this.className ='icon'\" " +
                                "class=\"icon\" alt=\"${text.resultEdit.delete}\" " +
                                "title=\"${text.resultEdit.delete}\" border=\"1\" /></a>").
                        append("</td>");
                }
                for (int i = 0; i < columns; i++) {
                    buff.append("<td>").
                        append(escapeData(rs, i + 1)).
                        append("</td>");
                }
                buff.append("</tr>");
            }
        }
        boolean isUpdatable = false;
        try {
            if (!session.getContents().isDB2()) {
                isUpdatable = rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE
                    && rs.getType() != ResultSet.TYPE_FORWARD_ONLY;
            }
        } catch (NullPointerException e) {
            // ignore
            // workaround for a JDBC-ODBC bridge problem
        }
        if (edit) {
            ResultSet old = session.result;
            if (old != null) {
                old.close();
            }
            session.result = rs;
        } else {
            rs.close();
        }
        if (edit) {
            buff.append("<tr><td>").
                append("<img onclick=\"javascript:editRow(-1, " +
                        "'${sessionId}', '${text.resultEdit.save}', '${text.resultEdit.cancel}'").
                append(")\" width=16 height=16 src=\"ico_add.gif\" " +
                        "onmouseover = \"this.className ='icon_hover'\" " +
                        "onmouseout = \"this.className ='icon'\" " +
                        "class=\"icon\" alt=\"${text.resultEdit.add}\" " +
                        "title=\"${text.resultEdit.add}\" border=\"1\"/>").
                append("</td>");
            for (int i = 0; i < columns; i++) {
                buff.append("<td></td>");
            }
            buff.append("</tr>");
        }
        buff.append("</table>");
        if (edit) {
            buff.append("</form>");
        }
        if (rows == 0) {
            buff.append("(${text.result.noRows}");
        } else if (rows == 1) {
            buff.append("(${text.result.1row}");
        } else {
            buff.append('(').append(rows).append(" ${text.result.rows}");
        }
        buff.append(", ");
        time = System.currentTimeMillis() - time;
        buff.append(time).append(" ms)");
        if (!edit && isUpdatable && allowEdit) {
            buff.append("<br /><br />" +
                    "<form name=\"editResult\" method=\"post\" " +
                    "action=\"query.do?jsessionid=${sessionId}\" target=\"h2result\">" +
                    "<input type=\"submit\" class=\"button\" " +
                    "value=\"${text.resultEdit.editResult}\" />" +
                    "<input type=\"hidden\" name=\"sql\" value=\"@edit ").
            append(PageParser.escapeHtmlData(sql)).
            append("\" /></form>");
        }
        return buff.toString();
    }

    /**
     * Save the current connection settings to the properties file.
     *
     * @return the file to open afterwards
     */
    private String settingSave() {
        ConnectionInfo info = new ConnectionInfo();
        info.name = attributes.getProperty("name", "");
        info.driver = attributes.getProperty("driver", "");
        info.url = attributes.getProperty("url", "");
        info.user = attributes.getProperty("user", "");
        server.updateSetting(info);
        attributes.put("setting", info.name);
        server.saveProperties(null);
        return "index.do";
    }

    private static String escapeData(ResultSet rs, int columnIndex)
            throws SQLException {
        String d = rs.getString(columnIndex);
        if (d == null) {
            return "<i>null</i>";
        } else if (d.length() > 100_000) {
            String s;
            if (isBinary(rs.getMetaData().getColumnType(columnIndex))) {
                s = PageParser.escapeHtml(d.substring(0, 6)) +
                        "... (" + (d.length() / 2) + " ${text.result.bytes})";
            } else {
                s = PageParser.escapeHtml(d.substring(0, 100)) +
                        "... (" + d.length() + " ${text.result.characters})";
            }
            return "<div style='display: none'>=+</div>" + s;
        } else if (d.equals("null") || d.startsWith("= ") || d.startsWith("=+")) {
            return "<div style='display: none'>= </div>" + PageParser.escapeHtml(d);
        } else if (d.equals("")) {
            // PageParser.escapeHtml replaces "" with a non-breaking space
            return "";
        }
        return PageParser.escapeHtml(d);
    }

    private static boolean isBinary(int sqlType) {
        switch (sqlType) {
        case Types.BINARY:
        case Types.BLOB:
        case Types.JAVA_OBJECT:
        case Types.LONGVARBINARY:
        case Types.OTHER:
        case Types.VARBINARY:
            return true;
        }
        return false;
    }

    private void unescapeData(String x, ResultSet rs, int columnIndex)
            throws SQLException {
        if (x.equals("null")) {
            rs.updateNull(columnIndex);
            return;
        } else if (x.startsWith("=+")) {
            // don't update
            return;
        } else if (x.equals("=*")) {
            // set an appropriate default value
            int type = rs.getMetaData().getColumnType(columnIndex);
            switch (type) {
            case Types.TIME:
                rs.updateString(columnIndex, "12:00:00");
                break;
            case Types.TIMESTAMP:
            case Types.DATE:
                rs.updateString(columnIndex, "2001-01-01");
                break;
            default:
                rs.updateString(columnIndex, "1");
                break;
            }
            return;
        } else if (x.startsWith("= ")) {
            x = x.substring(2);
        }
        ResultSetMetaData meta = rs.getMetaData();
        int type = meta.getColumnType(columnIndex);
        if (session.getContents().isH2()) {
            rs.updateString(columnIndex, x);
            return;
        }
        switch (type) {
        case Types.BIGINT:
            rs.updateLong(columnIndex, Long.decode(x));
            break;
        case Types.DECIMAL:
            rs.updateBigDecimal(columnIndex, new BigDecimal(x));
            break;
        case Types.DOUBLE:
        case Types.FLOAT:
            rs.updateDouble(columnIndex, Double.parseDouble(x));
            break;
        case Types.REAL:
            rs.updateFloat(columnIndex, Float.parseFloat(x));
            break;
        case Types.INTEGER:
            rs.updateInt(columnIndex, Integer.decode(x));
            break;
        case Types.TINYINT:
            rs.updateShort(columnIndex, Short.decode(x));
            break;
        default:
            rs.updateString(columnIndex, x);
        }
    }

    private String settingRemove() {
        String setting = attributes.getProperty("name", "");
        server.removeSetting(setting);
        ArrayList<ConnectionInfo> settings = server.getSettings();
        if (!settings.isEmpty()) {
            attributes.put("setting", settings.get(0));
        }
        server.saveProperties(null);
        return "index.do";
    }

    /**
     * Get the current mime type.
     *
     * @return the mime type
     */
    String getMimeType() {
        return mimeType;
    }

    boolean getCache() {
        return cache;
    }

    WebSession getSession() {
        return session;
    }

    private void trace(String s) {
        server.trace(s);
    }

}
