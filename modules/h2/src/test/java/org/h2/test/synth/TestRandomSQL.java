/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.engine.SysProperties;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.MathUtils;

/**
 * This test executes random SQL statements generated using the BNF tool.
 */
public class TestRandomSQL extends TestBase {

    private int success, total;

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
        if (config.networked) {
            return;
        }
        config.memory = true;
        int len = getSize(2, 6);
        for (int a = 0; a < len; a++) {
            int s = MathUtils.randomInt(Integer.MAX_VALUE);
            testCase(s);
        }
    }

    private void testWithSeed(int seed) throws Exception {
        Connection conn = null;
        try {
            conn = getConnection(getDatabaseName(seed));
        } catch (SQLException e) {
            if (e.getSQLState().equals("HY000")) {
                TestBase.logError("new TestRandomSQL().init(test).testCase(" + seed + ");  " +
                        "// FAIL: " + e.toString() + " sql: " + "connect", e);
            }
            conn = getConnection(getDatabaseName(seed));
        }
        Statement stat = conn.createStatement();

        BnfRandom bnfRandom = new BnfRandom();
        bnfRandom.setSeed(seed);
        for (int i = 0; i < bnfRandom.getStatementCount(); i++) {
            String sql = bnfRandom.getRandomSQL();
            if (sql != null) {
                try {
                    Thread.yield();
                    total++;
                    if (total % 100 == 0) {
                        printTime("total: " + total + " success: " +
                                (100 * success / total) + "%");
                    }
                    stat.execute(sql);
                    success++;
                } catch (SQLException e) {
                    if (e.getSQLState().equals("HY000")) {
                        TestBase.logError(
                                "new TestRandomSQL().init(test).testCase(" +
                                        seed + ");  " + "// FAIL: " +
                                        e.toString() + " sql: " + sql, e);
                    }
                }
            }
        }
        try {
            conn.close();
            conn = getConnection(getDatabaseName(seed));
            conn.createStatement().execute("shutdown immediately");
            conn.close();
        } catch (SQLException e) {
            if (e.getSQLState().equals("HY000")) {
                TestBase.logError("new TestRandomSQL().init(test).testCase(" + seed + ");  " +
                        "// FAIL: " + e.toString() + " sql: " + "conn.close", e);
            }
        }
    }

    private void testCase(int seed) throws Exception {
        String old = SysProperties.getScriptDirectory();
        try {
            System.setProperty(SysProperties.H2_SCRIPT_DIRECTORY,
                    getBaseDir() + "/" + getTestName());
            printTime("seed: " + seed);
            deleteDb(seed);
            testWithSeed(seed);
        } finally {
            System.setProperty(SysProperties.H2_SCRIPT_DIRECTORY, old);
        }
        deleteDb(seed);
    }

    private String getDatabaseName(int seed) {
        return getTestName() + "/db" + seed;
    }

    private void deleteDb(int seed) {
        FileUtils.delete(getDatabaseName(seed));
    }

}
