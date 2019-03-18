/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.h2.jaqu.Db;
import org.h2.jaqu.DbInspector;
import org.h2.message.DbException;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

/**
 * Generates JaQu models.
 */
public class GenerateModels {

    private static final int todoReview = 0;

    /**
     * The output stream where this tool writes to.
     */
    protected final PrintStream out = System.out;

    public static void main(String... args) throws SQLException {
        new GenerateModels().runTool(args);
    }

    public void runTool(String... args) throws SQLException {
        String url = null;
        String user = "sa";
        String password = "";
        String schema = null;
        String table = null;
        String packageName = "";
        String folder = null;
        boolean annotateSchema = true;
        boolean trimStrings = false;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-url")) {
                url = args[++i];
            } else if (arg.equals("-user")) {
                user = args[++i];
            } else if (arg.equals("-password")) {
                password = args[++i];
            } else if (arg.equals("-schema")) {
                schema = args[++i];
            } else if (arg.equals("-table")) {
                table = args[++i];
            } else if (arg.equals("-package")) {
                packageName = args[++i];
            } else if (arg.equals("-folder")) {
                folder = args[++i];
            } else if (arg.equals("-annotateSchema")) {
                try {
                    annotateSchema = Boolean.parseBoolean(args[++i]);
                } catch (Throwable t) {
                    throw new SQLException(
                            "Can not parse -annotateSchema value");
                }
            } else if (arg.equals("-trimStrings")) {
                try {
                    trimStrings = Boolean.parseBoolean(args[++i]);
                } catch (Throwable t) {
                    throw new SQLException("Can not parse -trimStrings value");
                }
            } else {
                throwUnsupportedOption(arg);
            }
        }
        if (url == null) {
            throw new SQLException("URL not set");
        }
        execute(url, user, password, schema, table, packageName, folder,
                annotateSchema, trimStrings);
    }

    /**
     * Generates models from the database.
     *
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @param schema the schema to read from. null for all schemas.
     * @param table the table to model. null for all tables within schema.
     * @param packageName the package name of the model classes.
     * @param folder destination folder for model classes (package path not
     *            included)
     * @param annotateSchema includes the schema in the table model annotations
     * @param trimStrings automatically trim strings that exceed maxLength
     */
    public static void execute(String url, String user, String password,
            String schema, String table, String packageName, String folder,
            boolean annotateSchema, boolean trimStrings) throws SQLException {
        Connection conn = null;
        try {
            org.h2.Driver.load();
            conn = DriverManager.getConnection(url, user, password);
            Db db = Db.open(url, user, password.toCharArray());
            DbInspector inspector = new DbInspector(db);
            List<String> models = inspector.generateModel(schema, table,
                    packageName, annotateSchema, trimStrings);
            File parentFile;
            if (StringUtils.isNullOrEmpty(folder)) {
                parentFile = new File(System.getProperty("user.dir"));
            } else {
                parentFile = new File(folder);
            }
            parentFile.mkdirs();
            Pattern p = Pattern.compile("class ([a-zA-Z0-9]+)");
            for (String model : models) {
                Matcher m = p.matcher(model);
                if (m.find()) {
                    String className = m.group().substring("class".length())
                            .trim();
                    File classFile = new File(parentFile, className + ".java");
                    Writer o = new FileWriter(classFile, false);
                    PrintWriter writer = new PrintWriter(new BufferedWriter(o));
                    writer.write(model);
                    writer.close();
                    System.out.println("Generated "
                            + classFile.getAbsolutePath());
                }
            }
        } catch (IOException io) {
            throw DbException
                    .convertIOException(io, "could not generate model")
                    .getSQLException();
        } finally {
            JdbcUtils.closeSilently(conn);
        }
    }

    /**
     * Throw a SQLException saying this command line option is not supported.
     *
     * @param option the unsupported option
     * @return this method never returns normally
     */
    protected SQLException throwUnsupportedOption(String option)
            throws SQLException {
        showUsage();
        throw new SQLException("Unsupported option: " + option);
    }

    protected void showUsage() {
        out.println("GenerateModels");
        out.println("Usage: java " + getClass().getName());
        out.println();
        out.println("(*) -url jdbc:h2:~test");
        out.println("    -user <string>");
        out.println("    -password <string>");
        out.println("    -schema <string>");
        out.println("    -table <string>");
        out.println("    -package <string>");
        out.println("    -folder <string>");
        out.println("    -annotateSchema <boolean>");
        out.println("    -trimStrings <boolean>");
    }

}
