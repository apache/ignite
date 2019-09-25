/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;
import org.h2.util.Tool;

/**
 * Creates a SQL script file by extracting the schema and data of a database.
 * @h2.resource
 */
public class Script extends Tool {

    /**
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-url "&lt;url&gt;"]</td>
     * <td>The database URL (jdbc:...)</td></tr>
     * <tr><td>[-user &lt;user&gt;]</td>
     * <td>The user name (default: sa)</td></tr>
     * <tr><td>[-password &lt;pwd&gt;]</td>
     * <td>The password</td></tr>
     * <tr><td>[-script &lt;file&gt;]</td>
     * <td>The target script file name (default: backup.sql)</td></tr>
     * <tr><td>[-options ...]</td>
     * <td>A list of options (only for embedded H2, see SCRIPT)</td></tr>
     * <tr><td>[-quiet]</td>
     * <td>Do not print progress information</td></tr>
     * </table>
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new Script().runTool(args);
    }

    @Override
    public void runTool(String... args) throws SQLException {
        String url = null;
        String user = "";
        String password = "";
        String file = "backup.sql";
        String options1 = "";
        String options2 = "";
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-url")) {
                url = args[++i];
            } else if (arg.equals("-user")) {
                user = args[++i];
            } else if (arg.equals("-password")) {
                password = args[++i];
            } else if (arg.equals("-script")) {
                file = args[++i];
            } else if (arg.equals("-options")) {
                StringBuilder buff1 = new StringBuilder();
                StringBuilder buff2 = new StringBuilder();
                i++;
                for (; i < args.length; i++) {
                    String a = args[i];
                    String upper = StringUtils.toUpperEnglish(a);
                    if ("SIMPLE".equals(upper) || upper.startsWith("NO") || "DROP".equals(upper)) {
                        buff1.append(' ');
                        buff1.append(args[i]);
                    } else if ("BLOCKSIZE".equals(upper)) {
                        buff1.append(' ');
                        buff1.append(args[i]);
                        i++;
                        buff1.append(' ');
                        buff1.append(args[i]);
                    } else {
                        buff2.append(' ');
                        buff2.append(args[i]);
                    }
                }
                options1 = buff1.toString();
                options2 = buff2.toString();
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                showUsageAndThrowUnsupportedOption(arg);
            }
        }
        if (url == null) {
            showUsage();
            throw new SQLException("URL not set");
        }
        process(url, user, password, file, options1, options2);
    }

    /**
     * Backs up a database to a stream.
     *
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @param fileName the target file name
     * @param options1 the options before the file name (may be an empty string)
     * @param options2 the options after the file name (may be an empty string)
     */
    public static void process(String url, String user, String password,
            String fileName, String options1, String options2) throws SQLException {
        Connection conn = null;
        try {
            org.h2.Driver.load();
            conn = DriverManager.getConnection(url, user, password);
            process(conn, fileName, options1, options2);
        } finally {
            JdbcUtils.closeSilently(conn);
        }
    }

    /**
     * Backs up a database to a stream. The stream is not closed.
     * The connection is not closed.
     *
     * @param conn the connection
     * @param fileName the target file name
     * @param options1 the options before the file name
     * @param options2 the options after the file name
     */
    public static void process(Connection conn,
            String fileName, String options1, String options2) throws SQLException {

        try (Statement stat = conn.createStatement()) {
            String sql = "SCRIPT " + options1 + " TO '" + fileName + "' " + options2;
            stat.execute(sql);
        }
    }

}
