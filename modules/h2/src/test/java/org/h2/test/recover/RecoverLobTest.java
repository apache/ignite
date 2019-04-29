/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.recover;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Recover;

/**
 * Tests BLOB/CLOB recovery.
 */
public class RecoverLobTest extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public boolean isEnabled() {
        if (config.memory) {
            return false;
        }
        return true;
    }

    @Override
    public void test() throws Exception {
        testRecoverClob();
    }

    private void testRecoverClob() throws Exception {
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        Connection conn = getConnection("recovery");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int, data clob)");
        stat.execute("insert into test values(1, space(10000))");
        stat.execute("insert into test values(2, space(20000))");
        stat.execute("insert into test values(3, space(30000))");
        stat.execute("insert into test values(4, space(40000))");
        stat.execute("insert into test values(5, space(50000))");
        stat.execute("insert into test values(6, space(60000))");
        stat.execute("insert into test values(7, space(70000))");
        stat.execute("insert into test values(8, space(80000))");

        conn.close();
        Recover.main("-dir", getBaseDir(), "-db", "recovery");
        DeleteDbFiles.execute(getBaseDir(), "recovery", true);
        conn = getConnection(
                "recovery;init=runscript from '" +
                getBaseDir() + "/recovery.h2.sql'");
        stat = conn.createStatement();

        ResultSet rs = stat.executeQuery("select * from test");
        while(rs.next()){

            int id = rs.getInt(1);
            String data = rs.getString(2);

            assertNotNull(data);
            assertTrue(data.length() == 10000 * id);

        }
        rs.close();
        conn.close();
    }



}
