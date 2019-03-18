/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.tools.RunScript;

/**
 * In this example a database is initialized from compressed script in a jar
 * file.
 */
public class InitDatabaseFromJar {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        createScript();
        new InitDatabaseFromJar().initDb();
    }

    /**
     * Create a script from a new database.
     */
    private static void createScript() throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:test");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES('Hello World')");
        stat.execute("SCRIPT TO 'script.sql'");
        stat.close();
        conn.close();
    }

    /**
     * Initialize a database from a SQL script file.
     */
    void initDb() throws Exception {
        Class.forName("org.h2.Driver");
        InputStream in = getClass().getResourceAsStream("script.sql");
        if (in == null) {
            System.out.println("Please add the file script.sql to the classpath, package "
                    + getClass().getPackage().getName());
        } else {
            Connection conn = DriverManager.getConnection("jdbc:h2:mem:test");
            RunScript.execute(conn, new InputStreamReader(in));
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            rs.close();
            stat.close();
            conn.close();
        }
    }
}
