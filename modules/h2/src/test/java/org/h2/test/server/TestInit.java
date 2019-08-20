/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Tests INIT command within embedded/server mode.
 */
public class TestInit extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {

        String init1 = getBaseDir() + "/test-init-1.sql";
        String init2 = getBaseDir() + "/test-init-2.sql";

        // Create two scripts that we will run via "INIT"
        FileUtils.createDirectories(FileUtils.getParent(init1));

        Writer w = new OutputStreamWriter(FileUtils.newOutputStream(init1, false));

        PrintWriter writer = new PrintWriter(w);
        writer.println("create table test(id int identity, name varchar);");
        writer.println("insert into test(name) values('cat');");
        writer.close();

        w = new OutputStreamWriter(FileUtils.newOutputStream(init2, false));
        writer = new PrintWriter(w);
        writer.println("insert into test(name) values('dog');");
        writer.close();

        // Make the database connection, and run the two scripts
        deleteDb("initDb");
        Connection conn = getConnection("initDb;" +
                "INIT=" +
                "RUNSCRIPT FROM '" + init1 + "'\\;" +
                "RUNSCRIPT FROM '" + init2 + "'");

        Statement stat = conn.createStatement();

        // Confirm our scripts have run by loading the data they inserted
        ResultSet rs = stat.executeQuery("select name from test order by name");

        assertTrue(rs.next());
        assertEquals("cat", rs.getString(1));

        assertTrue(rs.next());
        assertEquals("dog", rs.getString(1));

        assertFalse(rs.next());

        conn.close();
        deleteDb("initDb");

        FileUtils.delete(init1);
        FileUtils.delete(init2);
    }

}
