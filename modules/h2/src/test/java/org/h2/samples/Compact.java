/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.store.fs.FileUtils;
import org.h2.tools.Script;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.RunScript;

/**
 * This sample application shows how to compact the database files.
 * This is done by creating a SQL script, and then re-creating the database
 * using this script.
 */
public class Compact {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        DeleteDbFiles.execute("./data", "test", true);
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:./data/test", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World');");
        stat.close();
        conn.close();
        System.out.println("Compacting...");
        compact("./data", "test", "sa", "");
        System.out.println("Done.");
    }

    /**
     * Utility method to compact a database.
     *
     * @param dir the directory
     * @param dbName the database name
     * @param user the user name
     * @param password the password
     */
    public static void compact(String dir, String dbName,
            String user, String password) throws SQLException {
        String url = "jdbc:h2:" + dir + "/" + dbName;
        String file = "data/test.sql";
        Script.process(url, user, password, file, "", "");
        DeleteDbFiles.execute(dir, dbName, true);
        RunScript.execute(url, user, password, file, null, false);
        FileUtils.delete(file);
    }

}
