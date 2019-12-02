/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;
import org.h2.util.Tool;

/**
 * Runs a SQL script against a database.
 * @h2.resource
 */
public class RunScript extends Tool {

    private boolean showResults;
    private boolean checkResults;

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
     * <td>The script file to run (default: backup.sql)</td></tr>
     * <tr><td>[-driver &lt;class&gt;]</td>
     * <td>The JDBC driver class to use (not required in most cases)</td></tr>
     * <tr><td>[-showResults]</td>
     * <td>Show the statements and the results of queries</td></tr>
     * <tr><td>[-checkResults]</td>
     * <td>Check if the query results match the expected results</td></tr>
     * <tr><td>[-continueOnError]</td>
     * <td>Continue even if the script contains errors</td></tr>
     * <tr><td>[-options ...]</td>
     * <td>RUNSCRIPT options (embedded H2; -*Results not supported)</td></tr>
     * </table>
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new RunScript().runTool(args);
    }

    /**
     * Executes the contents of a SQL script file against a database.
     * This tool is usually used to create a database from script.
     * It can also be used to analyze performance problems by running
     * the tool using Java profiler settings such as:
     * <pre>
     * java -Xrunhprof:cpu=samples,depth=16 ...
     * </pre>
     * To include local files when using remote databases, use the special
     * syntax:
     * <pre>
     * &#064;INCLUDE fileName
     * </pre>
     * This syntax is only supported by this tool. Embedded RUNSCRIPT SQL
     * statements will be executed by the database.
     *
     * @param args the command line arguments
     */
    @Override
    public void runTool(String... args) throws SQLException {
        String url = null;
        String user = "";
        String password = "";
        String script = "backup.sql";
        String options = null;
        boolean continueOnError = false;
        boolean showTime = false;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-url")) {
                url = args[++i];
            } else if (arg.equals("-user")) {
                user = args[++i];
            } else if (arg.equals("-password")) {
                password = args[++i];
            } else if (arg.equals("-continueOnError")) {
                continueOnError = true;
            } else if (arg.equals("-checkResults")) {
                checkResults = true;
            } else if (arg.equals("-showResults")) {
                showResults = true;
            } else if (arg.equals("-script")) {
                script = args[++i];
            } else if (arg.equals("-time")) {
                showTime = true;
            } else if (arg.equals("-driver")) {
                String driver = args[++i];
                JdbcUtils.loadUserClass(driver);
            } else if (arg.equals("-options")) {
                StringBuilder buff = new StringBuilder();
                i++;
                for (; i < args.length; i++) {
                    buff.append(' ').append(args[i]);
                }
                options = buff.toString();
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
        long time = System.nanoTime();
        if (options != null) {
            processRunscript(url, user, password, script, options);
        } else {
            process(url, user, password, script, null, continueOnError);
        }
        if (showTime) {
            time = System.nanoTime() - time;
            out.println("Done in " + TimeUnit.NANOSECONDS.toMillis(time) + " ms");
        }
    }

    /**
     * Executes the SQL commands read from the reader against a database.
     *
     * @param conn the connection to a database
     * @param reader the reader
     * @return the last result set
     */
    public static ResultSet execute(Connection conn, Reader reader)
            throws SQLException {
        // can not close the statement because we return a result set from it
        Statement stat = conn.createStatement();
        ResultSet rs = null;
        ScriptReader r = new ScriptReader(reader);
        while (true) {
            String sql = r.readStatement();
            if (sql == null) {
                break;
            }
            if (sql.trim().length() == 0) {
                continue;
            }
            boolean resultSet = stat.execute(sql);
            if (resultSet) {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                rs = stat.getResultSet();
            }
        }
        return rs;
    }

    private void process(Connection conn, String fileName,
            boolean continueOnError, Charset charset) throws SQLException,
            IOException {
        InputStream in = FileUtils.newInputStream(fileName);
        String path = FileUtils.getParent(fileName);
        try {
            in = new BufferedInputStream(in, Constants.IO_BUFFER_SIZE);
            Reader reader = new InputStreamReader(in, charset);
            process(conn, continueOnError, path, reader, charset);
        } finally {
            IOUtils.closeSilently(in);
        }
    }

    private void process(Connection conn, boolean continueOnError, String path,
            Reader reader, Charset charset) throws SQLException, IOException {
        Statement stat = conn.createStatement();
        ScriptReader r = new ScriptReader(reader);
        while (true) {
            String sql = r.readStatement();
            if (sql == null) {
                break;
            }
            String trim = sql.trim();
            if (trim.length() == 0) {
                continue;
            }
            if (trim.startsWith("@") && StringUtils.toUpperEnglish(trim).
                    startsWith("@INCLUDE")) {
                sql = trim;
                sql = sql.substring("@INCLUDE".length()).trim();
                if (!FileUtils.isAbsolute(sql)) {
                    sql = path + SysProperties.FILE_SEPARATOR + sql;
                }
                process(conn, sql, continueOnError, charset);
            } else {
                try {
                    if (showResults && !trim.startsWith("-->")) {
                        out.print(sql + ";");
                    }
                    if (showResults || checkResults) {
                        boolean query = stat.execute(sql);
                        if (query) {
                            ResultSet rs = stat.getResultSet();
                            int columns = rs.getMetaData().getColumnCount();
                            StringBuilder buff = new StringBuilder();
                            while (rs.next()) {
                                buff.append("\n-->");
                                for (int i = 0; i < columns; i++) {
                                    String s = rs.getString(i + 1);
                                    if (s != null) {
                                        s = StringUtils.replaceAll(s, "\r\n", "\n");
                                        s = StringUtils.replaceAll(s, "\n", "\n-->    ");
                                        s = StringUtils.replaceAll(s, "\r", "\r-->    ");
                                    }
                                    buff.append(' ').append(s);
                                }
                            }
                            buff.append("\n;");
                            String result = buff.toString();
                            if (showResults) {
                                out.print(result);
                            }
                            if (checkResults) {
                                String expected = r.readStatement() + ";";
                                expected = StringUtils.replaceAll(expected, "\r\n", "\n");
                                expected = StringUtils.replaceAll(expected, "\r", "\n");
                                if (!expected.equals(result)) {
                                    expected = StringUtils.replaceAll(expected, " ", "+");
                                    result = StringUtils.replaceAll(result, " ", "+");
                                    throw new SQLException(
                                            "Unexpected output for:\n" + sql.trim() +
                                            "\nGot:\n" + result + "\nExpected:\n" + expected);
                                }
                            }

                        }
                    } else {
                        stat.execute(sql);
                    }
                } catch (Exception e) {
                    if (continueOnError) {
                        e.printStackTrace(out);
                    } else {
                        throw DbException.toSQLException(e);
                    }
                }
            }
        }
    }

    private static void processRunscript(String url, String user, String password,
            String fileName, String options) throws SQLException {
        Connection conn = null;
        Statement stat = null;
        try {
            org.h2.Driver.load();
            conn = DriverManager.getConnection(url, user, password);
            stat = conn.createStatement();
            String sql = "RUNSCRIPT FROM '" + fileName + "' " + options;
            stat.execute(sql);
        } finally {
            JdbcUtils.closeSilently(stat);
            JdbcUtils.closeSilently(conn);
        }
    }

    /**
     * Executes the SQL commands in a script file against a database.
     *
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @param fileName the script file
     * @param charset the character set or null for UTF-8
     * @param continueOnError if execution should be continued if an error
     *            occurs
     */
    public static void execute(String url, String user, String password,
            String fileName, Charset charset, boolean continueOnError)
            throws SQLException {
        new RunScript().process(url, user, password, fileName, charset,
                continueOnError);
    }

    /**
     * Executes the SQL commands in a script file against a database.
     *
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @param fileName the script file
     * @param charset the character set or null for UTF-8
     * @param continueOnError if execution should be continued if an error
     *            occurs
     */
    void process(String url, String user, String password,
            String fileName, Charset charset,
            boolean continueOnError) throws SQLException {
        try {
            org.h2.Driver.load();
            if (charset == null) {
                charset = StandardCharsets.UTF_8;
            }
            try (Connection conn = DriverManager.getConnection(url, user, password)) {
                process(conn, fileName, continueOnError, charset);
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

}
