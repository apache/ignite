/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;

/**
 * A b-tree index test.
 */
public class TestBtreeIndex extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.big = true;
        test.test();
        test.test();
        test.test();
    }

    @Override
    public void test() throws SQLException {
        Random random = new Random();
        for (int i = 0; i < getSize(1, 4); i++) {
            testAddDelete();
            int seed = random.nextInt();
            testCase(seed);
        }
    }

    private void testAddDelete() throws SQLException {
        deleteDb(getTestName());
        Connection conn = getConnection(getTestName());
        try {
            Statement stat = conn.createStatement();
            stat.execute("CREATE TABLE TEST(ID bigint primary key)");
            int count = 1000;
            stat.execute(
                    "insert into test select x from system_range(1, " +
                    count + ")");
            if (!config.memory) {
                conn.close();
                conn = getConnection(getTestName());
                stat = conn.createStatement();
            }
            for (int i = 1; i < count; i++) {
                ResultSet rs = stat.executeQuery("select * from test order by id");
                for (int j = i; rs.next(); j++) {
                    assertEquals(j, rs.getInt(1));
                }
                stat.execute("delete from test where id =" + i);
            }
            stat.execute("drop all objects delete files");
        } finally {
            conn.close();
        }
        deleteDb(getTestName());
    }

    private void testCase(int seed) throws SQLException {
        testOne(seed);
    }

    private void testOne(int seed) throws SQLException {
        org.h2.Driver.load();
        deleteDb(getTestName());
        printTime("testIndex " + seed);
        Random random = new Random(seed);
        int distinct, prefixLength;
        if (random.nextBoolean()) {
            distinct = random.nextInt(8000) + 1;
            prefixLength = random.nextInt(8000) + 1;
        } else if (random.nextBoolean()) {
            distinct = random.nextInt(16000) + 1;
            prefixLength = random.nextInt(100) + 1;
        } else {
            distinct = random.nextInt(10) + 1;
            prefixLength = random.nextInt(10) + 1;
        }
        boolean delete = random.nextBoolean();
        StringBuilder buff = new StringBuilder();
        for (int j = 0; j < prefixLength; j++) {
            buff.append("x");
            if (buff.length() % 10 == 0) {
                buff.append(buff.length());
            }
        }
        String prefix = buff.toString().substring(0, prefixLength);
        DeleteDbFiles.execute(getBaseDir() + "/" + getTestName(), null, true);
        try (Connection conn = getConnection(getTestName())) {
            Statement stat = conn.createStatement();
            stat.execute("CREATE TABLE a(text VARCHAR PRIMARY KEY)");
            PreparedStatement prepInsert = conn.prepareStatement(
                    "INSERT INTO a VALUES(?)");
            PreparedStatement prepDelete = conn.prepareStatement(
                    "DELETE FROM a WHERE text=?");
            PreparedStatement prepDeleteAllButOne = conn.prepareStatement(
                    "DELETE FROM a WHERE text <> ?");
            int count = 0;
            for (int i = 0; i < 1000; i++) {
                int y = random.nextInt(distinct);
                try {
                    prepInsert.setString(1, prefix + y);
                    prepInsert.executeUpdate();
                    count++;
                } catch (SQLException e) {
                    if (e.getSQLState().equals("23505")) {
                        // ignore
                    } else {
                        TestBase.logError("error", e);
                        break;
                    }
                }
                if (delete && random.nextInt(10) == 1) {
                    if (random.nextInt(4) == 1) {
                        try {
                            prepDeleteAllButOne.setString(1, prefix + y);
                            int deleted = prepDeleteAllButOne.executeUpdate();
                            if (deleted < count - 1) {
                                printError(seed, "deleted:" + deleted + " i:" + i);
                            }
                            count -= deleted;
                        } catch (SQLException e) {
                            TestBase.logError("error", e);
                            break;
                        }
                    } else {
                        try {
                            prepDelete.setString(1, prefix + y);
                            int deleted = prepDelete.executeUpdate();
                            if (deleted > 1) {
                                printError(seed, "deleted:" + deleted + " i:" + i);
                            }
                            count -= deleted;
                        } catch (SQLException e) {
                            TestBase.logError("error", e);
                            break;
                        }
                    }
                }
            }
            int testCount;
            testCount = 0;
            ResultSet rs = stat.executeQuery(
                    "SELECT text FROM a ORDER BY text");
            ResultSet rs2 = conn.createStatement().executeQuery(
                    "SELECT text FROM a ORDER BY 'x' || text");

//System.out.println("-----------");
//while(rs.next()) {
//    System.out.println(rs.getString(1));
//}
//System.out.println("-----------");
//while(rs2.next()) {
//    System.out.println(rs2.getString(1));
//}
//if (true) throw new AssertionError("stop");
//
            testCount = 0;
            while (rs.next() && rs2.next()) {
                if (!rs.getString(1).equals(rs2.getString(1))) {
                    assertEquals("" + testCount, rs.getString(1), rs2.getString(1));
                }
                testCount++;
            }
            assertFalse(rs.next());
            assertFalse(rs2.next());
            if (testCount != count) {
                printError(seed, "count:" + count + " testCount:" + testCount);
            }
            rs = stat.executeQuery("SELECT text, count(*) FROM a " +
                    "GROUP BY text HAVING COUNT(*)>1");
            if (rs.next()) {
                printError(seed, "testCount:" + testCount + " " + rs.getString(1));
            }
        }
        deleteDb(getTestName());
    }

    private void printError(int seed, String message) {
        TestBase.logError("new TestBtreeIndex().init(test).testCase(" +
                seed + "); // " + message, null);
        fail(message);
    }

}
