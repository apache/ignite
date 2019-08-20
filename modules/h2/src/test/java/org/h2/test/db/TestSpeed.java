/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.h2.test.TestBase;

/**
 * Various small performance tests.
 */
public class TestSpeed extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {

        deleteDb("speed");
        Connection conn;

        conn = getConnection("speed");

        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        int len = getSize(1, 10000);
        for (int i = 0; i < len; i++) {
            stat.execute("SELECT ID, NAME FROM TEST ORDER BY ID");
        }

        // drop table if exists test;
        // CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
        // @LOOP 100000 INSERT INTO TEST VALUES(?, 'Hello');
        // @LOOP 100000 SELECT * FROM TEST WHERE ID = ?;

        // stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME
        // VARCHAR(255))");
        // for(int i=0; i<1000; i++) {
        // stat.execute("INSERT INTO TEST VALUES("+i+", 'Hello')");
        // }
        // stat.execute("CREATE TABLE TEST_A(ID INT PRIMARY KEY, NAME
        // VARCHAR(255))");
        // stat.execute("INSERT INTO TEST_A VALUES(0, 'Hello')");
        long time = System.nanoTime();
        // for(int i=1; i<8000; i*=2) {
        // stat.execute("INSERT INTO TEST_A SELECT ID+"+i+", NAME FROM TEST_A");
        //
        // // stat.execute("INSERT INTO TEST_A VALUES("+i+", 'Hello')");
        // }
        // for(int i=0; i<4; i++) {
        // ResultSet rs = stat.executeQuery("SELECT * FROM TEST_A");
        // while(rs.next()) {
        // rs.getInt(1);
        // rs.getString(2);
        // }
        // }

        //
        // stat.execute("CREATE TABLE TEST_B(ID INT PRIMARY KEY, NAME
        // VARCHAR(255))");
        // for(int i=0; i<80000; i++) {
        // stat.execute("INSERT INTO TEST_B VALUES("+i+", 'Hello')");
        // }

        // conn.close();
        // System.exit(0);
        // int testParser;
        // java -Xrunhprof:cpu=samples,depth=8 -cp . org.h2.test.TestAll
        //
        // stat.execute("CREATE TABLE TEST(ID INT)");
        // stat.execute("INSERT INTO TEST VALUES(1)");
        // ResultSet rs = stat.executeQuery("SELECT ID OTHER_ID FROM TEST");
        // rs.next();
        // rs.getString("ID");
        // stat.execute("DROP TABLE TEST");

        // long time = System.nanoTime();

        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE CACHED TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");

        int max = getSize(1, 10000);
        for (int i = 0; i < max; i++) {
            prep.setInt(1, i);
            prep.setString(2,
                    "abchelloasdfaldsjflajdflajdslfoajlskdfkjasdf" +
                    "abcfasdfadsfadfsalksdjflasjflajsdlkfjaksdjflkskd" + i);
            prep.execute();
        }

        // System.exit(0);
        // System.out.println("END "+Value.cacheHit+" "+Value.cacheMiss);

        time = System.nanoTime() - time;
        trace(TimeUnit.NANOSECONDS.toMillis(time) + " insert");

        // if(true) return;

        // if(config.log) {
        // System.gc();
        // System.gc();
        // log("mem="+(Runtime.getRuntime().totalMemory() -
        //     Runtime.getRuntime().freeMemory())/1024);
        // }

        // conn.close();

        time = System.nanoTime();

        prep = conn.prepareStatement("UPDATE TEST " +
                "SET NAME='Another data row which is long' WHERE ID=?");
        for (int i = 0; i < max; i++) {
            prep.setInt(1, i);
            prep.execute();

            // System.out.println("updated "+i);
            // stat.execute("UPDATE TEST SET NAME='Another data row which is
            // long' WHERE ID="+i);
            // ResultSet rs = stat.executeQuery("SELECT * FROM TEST WHERE
            // ID="+i);
            // if(!rs.next()) {
            // throw new AssertionError("hey! i="+i);
            // }
            // if(rs.next()) {
            // throw new AssertionError("hey! i="+i);
            // }
        }
        // for(int i=0; i<max; i++) {
        // stat.execute("DELETE FROM TEST WHERE ID="+i);
        // ResultSet rs = stat.executeQuery("SELECT * FROM TEST WHERE ID="+i);
        // if(rs.next()) {
        // throw new AssertionError("hey!");
        // }
        // }

        time = System.nanoTime() - time;
        trace(TimeUnit.NANOSECONDS.toMillis(time) + " update");

        time = System.nanoTime();
        conn.close();
        time = System.nanoTime() - time;
        trace(TimeUnit.NANOSECONDS.toMillis(time) + " close");
        deleteDb("speed");
    }

}
