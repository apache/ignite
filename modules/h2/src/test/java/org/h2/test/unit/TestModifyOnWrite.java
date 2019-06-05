/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.engine.SysProperties;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.IOUtils;
import org.h2.util.Utils;

/**
 * Test that the database file is only modified when writing to the database.
 */
public class TestModifyOnWrite extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        System.setProperty("h2.modifyOnWrite", "true");
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        if (!SysProperties.MODIFY_ON_WRITE) {
            return;
        }
        deleteDb("modifyOnWrite");
        String dbFile = getBaseDir() + "/modifyOnWrite.h2.db";
        assertFalse(FileUtils.exists(dbFile));
        Connection conn = getConnection("modifyOnWrite");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        conn.close();
        byte[] test = IOUtils.readBytesAndClose(FileUtils.newInputStream(dbFile), -1);

        conn = getConnection("modifyOnWrite");
        stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        assertFalse(rs.next());
        conn.close();
        assertTrue(FileUtils.exists(dbFile));
        byte[] test2 = IOUtils.readBytesAndClose(FileUtils.newInputStream(dbFile), -1);
        assertEquals(test, test2);

        conn = getConnection("modifyOnWrite");
        stat = conn.createStatement();
        stat.execute("insert into test values(1)");
        conn.close();

        conn = getConnection("modifyOnWrite");
        stat = conn.createStatement();
        rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        conn.close();

        test2 = IOUtils.readBytesAndClose(FileUtils.newInputStream(dbFile), -1);
        assertFalse(Utils.compareSecure(test, test2));
    }

}
