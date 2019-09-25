/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.todo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.h2.store.fs.FilePathRec;
import org.h2.test.unit.TestReopen;
import org.h2.tools.DeleteDbFiles;

/**
 * Test crashing a database by creating a lot of temporary tables.
 */
public class TestTempTableCrash {

    /**
     * Run just this test.
     *
     * @param args ignored
     */
    public static void main(String[] args) throws Exception {
        TestTempTableCrash.test();
    }

    private static void test() throws Exception {
        Connection conn;
        Statement stat;

        System.setProperty("h2.delayWrongPasswordMin", "0");
        System.setProperty("h2.check2", "false");
        FilePathRec.register();
        System.setProperty("reopenShift", "4");
        TestReopen reopen = new TestReopen();
        FilePathRec.setRecorder(reopen);

        String url = "jdbc:h2:rec:memFS:data;PAGE_SIZE=64;ANALYZE_AUTO=100";
        // String url = "jdbc:h2:" + RecordingFileSystem.PREFIX +
        //      "data/test;PAGE_SIZE=64";

        Class.forName("org.h2.Driver");
        DeleteDbFiles.execute("data", "test", true);
        conn = DriverManager.getConnection(url, "sa", "sa");
        stat = conn.createStatement();

        Random random = new Random(1);
        long start = System.nanoTime();
        for (int i = 0; i < 10000; i++) {
            long now = System.nanoTime();
            if (now > start + TimeUnit.SECONDS.toNanos(1)) {
                System.out.println("i: " + i);
                start = now;
            }
            int x;
            x = random.nextInt(100);
            stat.execute("drop table if exists test" + x);
            String type = random.nextBoolean() ? "temp" : "";
            // String type = "";
            stat.execute("create " + type + " table test" + x +
                    "(id int primary key, name varchar)");
            if (random.nextBoolean()) {
                stat.execute("create index idx_" + x + " on test" + x + "(name, id)");
            }
            if (random.nextBoolean()) {
                stat.execute("insert into test" + x + " select x, x " +
                        "from system_range(1, " + random.nextInt(100) + ")");
            }
            if (random.nextInt(10) == 1) {
                conn.close();
                conn = DriverManager.getConnection(url, "sa", "sa");
                stat = conn.createStatement();
            }
        }
        conn.close();
    }

}
