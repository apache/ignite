/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.doc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.h2.bnf.Bnf;
import org.h2.engine.Constants;
import org.h2.server.web.PageParser;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

/**
 * This application generates sections of the documentation
 * by converting the built-in help section (INFORMATION_SCHEMA.HELP)
 * to cross linked html.
 */
public class GenerateDoc {

    private static final String IN_HELP = "src/docsrc/help/help.csv";
    private String inDir = "src/docsrc/html";
    private String outDir = "docs/html";
    private Connection conn;
    private final HashMap<String, Object> session =
            new HashMap<>();
    private Bnf bnf;

    /**
     * This method is called when executing this application from the command
     * line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new GenerateDoc().run(args);
    }

    private void run(String... args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-in")) {
                inDir = args[++i];
            } else if (args[i].equals("-out")) {
                outDir = args[++i];
            }
        }
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:mem:");
        new File(outDir).mkdirs();
        new RailroadImages().run(outDir + "/images");
        bnf = Bnf.getInstance(null);
        bnf.linkStatements();
        session.put("version", Constants.getVersion());
        session.put("versionDate", Constants.BUILD_DATE);
        session.put("stableVersion", Constants.getVersionStable());
        session.put("stableVersionDate", Constants.BUILD_DATE_STABLE);
        // String help = "SELECT * FROM INFORMATION_SCHEMA.HELP WHERE SECTION";
        String help = "SELECT ROWNUM ID, * FROM CSVREAD('" +
                IN_HELP + "', NULL, 'lineComment=#') WHERE SECTION ";
        map("commandsDML",
                help + "= 'Commands (DML)' ORDER BY ID", true, false);
        map("commandsDDL",
                help + "= 'Commands (DDL)' ORDER BY ID", true, false);
        map("commandsOther",
                help + "= 'Commands (Other)' ORDER BY ID", true, false);
        map("literals",
                help + "= 'Literals' ORDER BY ID", true, false);
        map("datetimeFields",
                help + "= 'Datetime fields' ORDER BY ID", true, false);
        map("otherGrammar",
                help + "= 'Other Grammar' ORDER BY ID", true, false);
        map("functionsAggregate",
                help + "= 'Functions (Aggregate)' ORDER BY ID", true, false);
        map("functionsNumeric",
                help + "= 'Functions (Numeric)' ORDER BY ID", true, false);
        map("functionsString",
                help + "= 'Functions (String)' ORDER BY ID", true, false);
        map("functionsTimeDate",
                help + "= 'Functions (Time and Date)' ORDER BY ID", true, false);
        map("functionsSystem",
                help + "= 'Functions (System)' ORDER BY ID", true, false);
        map("functionsWindow",
                help + "= 'Functions (Window)' ORDER BY ID", true, false);
        map("dataTypes",
                help + "LIKE 'Data Types%' ORDER BY SECTION, ID", true, true);
        map("intervalDataTypes",
                help + "LIKE 'Interval Data Types%' ORDER BY SECTION, ID", true, true);
        map("informationSchema", "SELECT TABLE_NAME TOPIC, " +
                "GROUP_CONCAT(COLUMN_NAME " +
                "ORDER BY ORDINAL_POSITION SEPARATOR ', ') SYNTAX " +
                "FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA='INFORMATION_SCHEMA' " +
                "GROUP BY TABLE_NAME ORDER BY TABLE_NAME", false, false);
        processAll("");
        conn.close();
    }

    private void processAll(String dir) throws Exception {
        if (dir.endsWith(".svn")) {
            return;
        }
        File[] list = new File(inDir + "/" + dir).listFiles();
        for (File file : list) {
            if (file.isDirectory()) {
                processAll(dir + file.getName());
            } else {
                process(dir, file.getName());
            }
        }
    }

    private void process(String dir, String fileName) throws Exception {
        String inFile = inDir + "/" + dir + "/" + fileName;
        String outFile = outDir + "/" + dir + "/" + fileName;
        new File(outFile).getParentFile().mkdirs();
        FileOutputStream out = new FileOutputStream(outFile);
        FileInputStream in = new FileInputStream(inFile);
        byte[] bytes = IOUtils.readBytesAndClose(in, 0);
        if (fileName.endsWith(".html")) {
            String page = new String(bytes);
            page = PageParser.parse(page, session);
            bytes = page.getBytes();
        }
        out.write(bytes);
        out.close();
    }

    private void map(String key, String sql, boolean railroads, boolean forDataTypes)
            throws Exception {
        ResultSet rs = null;
        Statement stat = null;
        try {
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            ArrayList<HashMap<String, String>> list =
                    new ArrayList<>();
            while (rs.next()) {
                HashMap<String, String> map = new HashMap<>();
                ResultSetMetaData meta = rs.getMetaData();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String k = StringUtils.toLowerEnglish(meta.getColumnLabel(i));
                    String value = rs.getString(i);
                    value = value.trim();
                    map.put(k, PageParser.escapeHtml(value));
                }
                String topic = rs.getString("TOPIC");
                // Convert "INT Type" to "INT" etc.
                if (forDataTypes && topic.endsWith(" Type")) {
                    map.put("topic", topic.substring(0, topic.length() - 5));
                }
                String syntax = rs.getString("SYNTAX").trim();
                if (railroads) {
                    BnfRailroad r = new BnfRailroad();
                    String railroad = r.getHtml(bnf, syntax);
                    map.put("railroad", railroad);
                }
                BnfSyntax visitor = new BnfSyntax();
                String syntaxHtml = visitor.getHtml(bnf, syntax);
                map.put("syntax", syntaxHtml);

                // remove newlines in the regular text
                String text = map.get("text");
                if (text != null) {
                    // text is enclosed in <p> .. </p> so this works.
                    text = StringUtils.replaceAll(text,
                            "<br /><br />", "</p><p>");
                    text = StringUtils.replaceAll(text,
                            "<br />", " ");
                    text = addCode(text);
                    text = addLinks(text);
                    map.put("text", text);
                }

                String link = topic.toLowerCase();
                link = link.replace(' ', '_');
                // link = StringUtils.replaceAll(link, "_", "");
                link = link.replace('@', '_');
                map.put("link", StringUtils.urlEncode(link));

                list.add(map);
            }
            session.put(key, list);
            int div = 3;
            int part = (list.size() + div - 1) / div;
            for (int i = 0, start = 0; i < div; i++, start += part) {
                List<HashMap<String, String>> listThird = list.subList(start,
                        Math.min(start + part, list.size()));
                session.put(key + "-" + i, listThird);
            }
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(stat);
        }
    }

    private static String addCode(String text) {
        text = StringUtils.replaceAll(text, "&quot;", "\"");
        StringBuilder buff = new StringBuilder(text.length());
        int len = text.length();
        boolean code = false, codeQuoted = false;
        for (int i = 0; i < len; i++) {
            char c = text.charAt(i);
            if (i < len - 1) {
                char next = text.charAt(i+1);
                if (!code && !codeQuoted) {
                    if (Character.isUpperCase(c) && Character.isUpperCase(next)) {
                        buff.append("<code>");
                        code = true;
                    } else if (c == '\"' && (i == 0 || text.charAt(i - 1) != '\\')) {
                        buff.append("<code>");
                        codeQuoted = true;
                        continue;
                    }
                }
            }
            if (code) {
                if (!Character.isLetterOrDigit(c) && "_.".indexOf(c) < 0) {
                    buff.append("</code>");
                    code = false;
                }
            } else if (codeQuoted && c == '\"' && (i == 0 || text.charAt(i - 1) != '\\')) {
                buff.append("</code>");
                codeQuoted = false;
                continue;
            }
            buff.append(c);
        }
        if (code) {
            buff.append("</code>");
        }
        String s = buff.toString();
        s = StringUtils.replaceAll(s, "</code>, <code>", ", ");
        s = StringUtils.replaceAll(s, ".</code>", "</code>.");
        s = StringUtils.replaceAll(s, ",</code>", "</code>.");
        s = StringUtils.replaceAll(s, " @<code>", " <code>@");
        s = StringUtils.replaceAll(s, "</code> <code>", " ");
        s = StringUtils.replaceAll(s, "<code>SQL</code>", "SQL");
        s = StringUtils.replaceAll(s, "<code>XML</code>", "XML");
        s = StringUtils.replaceAll(s, "<code>URL</code>", "URL");
        s = StringUtils.replaceAll(s, "<code>URLs</code>", "URLs");
        s = StringUtils.replaceAll(s, "<code>HTML</code>", "HTML");
        s = StringUtils.replaceAll(s, "<code>KB</code>", "KB");
        s = StringUtils.replaceAll(s, "<code>MB</code>", "MB");
        s = StringUtils.replaceAll(s, "<code>GB</code>", "GB");
        return s;
    }

    private static String addLinks(String text) {
        int start = nextLink(text, 0);
        if (start < 0) {
            return text;
        }
        StringBuilder buff = new StringBuilder(text.length());
        int len = text.length();
        int offset = 0;
        do {
            int end = start + 7;
            for (; end < len && !Character.isWhitespace(text.charAt(end)); end++) {
                // Nothing to do
            }
            buff.append(text, offset, start) //
                    .append("<a href=\"").append(text, start, end).append("\">") //
                    .append(text, start, end) //
                    .append("</a>");
            offset = end;
        } while ((start = nextLink(text, offset)) >= 0);
        return buff.append(text, offset, len).toString();
    }

    private static int nextLink(String text, int i) {
        int found = -1;
        found = findLink(text, i, "http://", found);
        found = findLink(text, i, "https://", found);
        return found;
    }

    private static int findLink(String text, int offset, String prefix, int found) {
        int idx = text.indexOf(prefix, offset);
        if (idx >= 0 && (found < 0 || idx < found)) {
            found = idx;
        }
        return found;
    }

}
