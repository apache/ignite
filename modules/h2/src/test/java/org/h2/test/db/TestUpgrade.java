/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.upgrade.DbUpgrade;
import org.h2.util.Utils;

/**
 * Automatic upgrade test cases.
 */
public class TestUpgrade extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        if (config.mvStore) {
            return;
        }
        if (!Utils.isClassPresent("org.h2.upgrade.v1_1.Driver")) {
            return;
        }
        testLobs();
        testErrorUpgrading();
        testNoDb();
        testNoUpgradeOldAndNew();
        testIfExists();
        testCipher();
    }

    private void testLobs() throws Exception {
        deleteDb("upgrade");
        Connection conn;
        conn = DriverManager.getConnection("jdbc:h2v1_1:" +
                getBaseDir() + "/upgrade;PAGE_STORE=FALSE", getUser(), getPassword());
        conn.createStatement().execute(
                "create table test(data clob) as select space(100000)");
        conn.close();
        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.data.db"));
        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.index.db"));
        DbUpgrade.setDeleteOldDb(true);
        DbUpgrade.setScriptInTempDir(true);
        conn = getConnection("upgrade");
        assertFalse(FileUtils.exists(getBaseDir() + "/upgrade.data.db"));
        assertFalse(FileUtils.exists(getBaseDir() + "/upgrade.index.db"));
        ResultSet rs = conn.createStatement().executeQuery("select * from test");
        rs.next();
        assertEquals(new String(new char[100000]).replace((char) 0, ' '),
                rs.getString(1));
        conn.close();
        DbUpgrade.setDeleteOldDb(false);
        DbUpgrade.setScriptInTempDir(false);
        deleteDb("upgrade");
    }

    private void testErrorUpgrading() throws Exception {
        deleteDb("upgrade");
        OutputStream out;
        out = FileUtils.newOutputStream(getBaseDir() + "/upgrade.data.db", false);
        out.write(new byte[10000]);
        out.close();
        out = FileUtils.newOutputStream(getBaseDir() + "/upgrade.index.db", false);
        out.write(new byte[10000]);
        out.close();
        assertThrows(ErrorCode.FILE_VERSION_ERROR_1, this).
                getConnection("upgrade");

        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.data.db"));
        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.index.db"));
        deleteDb("upgrade");
    }

    private void testNoDb() throws SQLException {
        deleteDb("upgrade");
        Connection conn = getConnection("upgrade");
        conn.close();
        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.h2.db"));
        deleteDb("upgrade");

        conn = getConnection("upgrade;NO_UPGRADE=TRUE");
        conn.close();
        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.h2.db"));
        deleteDb("upgrade");
    }

    private void testNoUpgradeOldAndNew() throws Exception {
        deleteDb("upgrade");
        deleteDb("upgradeOld");
        String additionalParameters = ";AUTO_SERVER=TRUE;OPEN_NEW=TRUE";

        // Create old db
        Utils.callStaticMethod("org.h2.upgrade.v1_1.Driver.load");
        Connection connOld = DriverManager.getConnection("jdbc:h2v1_1:" +
                getBaseDir() + "/upgradeOld;PAGE_STORE=FALSE" + additionalParameters);
        // Test auto server, too
        Connection connOld2 = DriverManager.getConnection("jdbc:h2v1_1:" +
                getBaseDir() + "/upgradeOld;PAGE_STORE=FALSE" + additionalParameters);
        Statement statOld = connOld.createStatement();
        statOld.execute("create table testOld(id int)");
        connOld.close();
        connOld2.close();
        assertTrue(FileUtils.exists(getBaseDir() + "/upgradeOld.data.db"));

        // Create new DB
        Connection connNew = DriverManager.getConnection("jdbc:h2:" +
                getBaseDir() + "/upgrade" + additionalParameters);
        Connection connNew2 = DriverManager.getConnection("jdbc:h2:" +
                getBaseDir() + "/upgrade" + additionalParameters);
        Statement statNew = connNew.createStatement();
        statNew.execute("create table test(id int)");

        // Link to old DB without upgrade
        statNew.executeUpdate("CREATE LOCAL TEMPORARY LINKED TABLE " +
                "linkedTestOld('org.h2.Driver', 'jdbc:h2v1_1:" +
                getBaseDir() + "/upgradeOld" + additionalParameters + "', '', '', 'TestOld')");
        statNew.executeQuery("select * from linkedTestOld");
        connNew.close();
        connNew2.close();
        assertTrue(FileUtils.exists(getBaseDir() + "/upgradeOld.data.db"));
        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.h2.db"));

        connNew = DriverManager.getConnection("jdbc:h2:" +
                getBaseDir() + "/upgrade" + additionalParameters);
        connNew2 = DriverManager.getConnection("jdbc:h2:" +
                getBaseDir() + "/upgrade" + additionalParameters);
        statNew = connNew.createStatement();
        // Link to old DB with upgrade
        statNew.executeUpdate("CREATE LOCAL TEMPORARY LINKED TABLE " +
                "linkedTestOld('org.h2.Driver', 'jdbc:h2:" +
                getBaseDir() + "/upgradeOld" + additionalParameters + "', '', '', 'TestOld')");
        statNew.executeQuery("select * from linkedTestOld");
        connNew.close();
        connNew2.close();
        assertTrue(FileUtils.exists(getBaseDir() + "/upgradeOld.h2.db"));
        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.h2.db"));

        deleteDb("upgrade");
        deleteDb("upgradeOld");
    }

    private void testIfExists() throws Exception {
        deleteDb("upgrade");

        // Create old
        Utils.callStaticMethod("org.h2.upgrade.v1_1.Driver.load");
        Connection connOld = DriverManager.getConnection(
                "jdbc:h2v1_1:" + getBaseDir() + "/upgrade;PAGE_STORE=FALSE");
        // Test auto server, too
        Connection connOld2 = DriverManager.getConnection(
                "jdbc:h2v1_1:" + getBaseDir() + "/upgrade;PAGE_STORE=FALSE");
        Statement statOld = connOld.createStatement();
        statOld.execute("create table test(id int)");
        connOld.close();
        connOld2.close();
        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.data.db"));

        // Upgrade
        Connection connOldViaNew = DriverManager.getConnection(
                "jdbc:h2:" + getBaseDir() + "/upgrade;ifexists=true");
        Statement statOldViaNew = connOldViaNew.createStatement();
        statOldViaNew.executeQuery("select * from test");
        connOldViaNew.close();
        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.h2.db"));

        deleteDb("upgrade");
    }

    private void testCipher() throws Exception {
        deleteDb("upgrade");

        // Create old db
        Utils.callStaticMethod("org.h2.upgrade.v1_1.Driver.load");
        Connection conn = DriverManager.getConnection("jdbc:h2v1_1:" +
                getBaseDir() + "/upgrade;PAGE_STORE=FALSE;" +
                "CIPHER=AES", "abc", "abc abc");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        conn.close();
        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.data.db"));

        // Connect to old DB with upgrade
        conn = DriverManager.getConnection("jdbc:h2:" +
                getBaseDir() + "/upgrade;CIPHER=AES", "abc", "abc abc");
        stat = conn.createStatement();
        stat.executeQuery("select * from test");
        conn.close();
        assertTrue(FileUtils.exists(getBaseDir() + "/upgrade.h2.db"));

        deleteDb("upgrade");
    }

    @Override
    public void deleteDb(String dbName) {
        super.deleteDb(dbName);
        try {
            Utils.callStaticMethod(
                    "org.h2.upgrade.v1_1.tools.DeleteDbFiles.execute",
                    getBaseDir(), dbName, true);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        FileUtils.delete(getBaseDir() + "/" +
                    dbName + ".data.db.backup");
        FileUtils.delete(getBaseDir() + "/" +
                    dbName + ".index.db.backup");
        FileUtils.deleteRecursive(getBaseDir() + "/" +
                    dbName + ".lobs.db.backup", false);
    }

}