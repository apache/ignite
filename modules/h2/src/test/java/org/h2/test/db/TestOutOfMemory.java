/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import org.h2.api.ErrorCode;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathMem;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Tests out of memory situations. The database must not get corrupted, and
 * transactions must stay atomic.
 */
public class TestOutOfMemory extends TestBase {

    private static final String DB_NAME = "outOfMemory";

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
        if (config.vmlens) {
            // running out of memory will cause the vmlens agent to stop working
            return;
        }
        try {
            if (!config.travis) {
                System.gc();
                testMVStoreUsingInMemoryFileSystem();
                System.gc();
                testDatabaseUsingInMemoryFileSystem();
            }
            System.gc();
            testUpdateWhenNearlyOutOfMemory();
        } finally {
            System.gc();
        }
    }

    private void testMVStoreUsingInMemoryFileSystem() {
        FilePath.register(new FilePathMem());
        String fileName = "memFS:" + getTestName();
        final AtomicReference<Throwable> exRef = new AtomicReference<>();
        MVStore store = new MVStore.Builder()
                .fileName(fileName)
                .backgroundExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        exRef.compareAndSet(null, e);
                    }
                })
                .open();
        try {
            Map<Integer, byte[]> map = store.openMap("test");
            Random r = new Random(1);
            try {
                for (int i = 0; i < 100; i++) {
                    byte[] data = new byte[10 * 1024 * 1024];
                    r.nextBytes(data);
                    map.put(i, data);
                }
                Throwable throwable = exRef.get();
                if(throwable instanceof OutOfMemoryError) throw (OutOfMemoryError)throwable;
                if(throwable instanceof IllegalStateException) throw (IllegalStateException)throwable;
                fail();
            } catch (OutOfMemoryError | IllegalStateException e) {
                // expected
            }
            try {
                store.close();
            } catch (IllegalStateException e) {
                // expected
            }
            store.closeImmediately();
            store = MVStore.open(fileName);
            store.openMap("test");
            store.close();
        } finally {
            // just in case, otherwise if this test suffers a spurious failure,
            // succeeding tests will too, because they will OOM
            store.closeImmediately();
            FileUtils.delete(fileName);
        }
    }

    private void testDatabaseUsingInMemoryFileSystem() throws SQLException, InterruptedException {
        String filename = "memFS:" + getTestName();
        String url = "jdbc:h2:" + filename + "/test";
        try {
            Connection conn = DriverManager.getConnection(url);
            Statement stat = conn.createStatement();
            try {
                stat.execute("create table test(id int, name varchar) as " +
                        "select x, space(10000000+x) from system_range(1, 1000)");
                fail();
            } catch (SQLException e) {
                assertTrue("Unexpected error code: " + e.getErrorCode(),
                        ErrorCode.OUT_OF_MEMORY == e.getErrorCode() ||
                        ErrorCode.FILE_CORRUPTED_1 == e.getErrorCode() ||
                        ErrorCode.DATABASE_IS_CLOSED == e.getErrorCode() ||
                        ErrorCode.GENERAL_ERROR_1 == e.getErrorCode());
            }
            recoverAfterOOM();
            try {
                conn.close();
                fail();
            } catch (SQLException e) {
                assertTrue("Unexpected error code: " + e.getErrorCode(),
                        ErrorCode.OUT_OF_MEMORY == e.getErrorCode() ||
                        ErrorCode.FILE_CORRUPTED_1 == e.getErrorCode() ||
                        ErrorCode.DATABASE_IS_CLOSED == e.getErrorCode() ||
                        ErrorCode.GENERAL_ERROR_1 == e.getErrorCode());
            }
            recoverAfterOOM();
            conn = DriverManager.getConnection(url);
            stat = conn.createStatement();
            stat.execute("SELECT 1");
            conn.close();
        } finally {
            // release the static data this test generates
            FileUtils.deleteRecursive(filename, true);
        }
    }

    private static void recoverAfterOOM() throws InterruptedException {
        for (int i = 0; i < 5; i++) {
            System.gc();
            Thread.sleep(20);
        }
    }

    private void testUpdateWhenNearlyOutOfMemory() throws Exception {
        if (config.memory) {
            return;
        }
        deleteDb(DB_NAME);

        ProcessBuilder processBuilder = buildChild(
                DB_NAME + ";MAX_OPERATION_MEMORY=1000000",
                MyChild.class,
                "-XX:+UseParallelGC",
//                "-XX:+UseG1GC",
                "-Xmx128m");
//*
        processBuilder.start().waitFor();
/*/
        List<String> args = processBuilder.command();
        for (Iterator<String> iter = args.iterator(); iter.hasNext(); ) {
            String arg = iter.next();
            if(arg.equals(MyChild.class.getName())) {
                iter.remove();
                break;
            }
            iter.remove();
        }
        MyChild.main(args.toArray(new String[0]));
//*/
        try (Connection conn = getConnection(DB_NAME)) {
            Statement stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT count(*) FROM stuff");
            assertTrue(rs.next());
            assertEquals(3000, rs.getInt(1));

            rs = stat.executeQuery("SELECT * FROM stuff WHERE id = 3000");
            assertTrue(rs.next());
            String text = rs.getString(2);
            assertFalse(rs.wasNull());
            assertEquals(1004, text.length());

            // TODO: there are intermittent failures here
            // where number is about 1000 short of expected value.
            // This indicates a real problem - durability failure
            // and need to be looked at.
            rs = stat.executeQuery("SELECT sum(length(text)) FROM stuff");
            assertTrue(rs.next());
            int totalSize = rs.getInt(1);
            if (3010893 > totalSize) {
                TestBase.logErrorMessage("Durability failure - expected: 3010893, actual: " + totalSize);
            }
        } finally {
            deleteDb(DB_NAME);
        }
    }

    public static final class MyChild extends TestBase.Child
    {

        /**
         * Run just this test.
         *
         * @param args the arguments
         */
        public static void main(String... args) throws Exception {
            new MyChild(args).init().test();
        }

        private MyChild(String... args) {
            super(args);
        }

        @Override
        public void test() {
            try (Connection conn = getConnection()) {
                Statement stat = conn.createStatement();
                stat.execute("DROP ALL OBJECTS");
                stat.execute("CREATE TABLE stuff (id INT, text VARCHAR)");
                stat.execute("INSERT INTO stuff(id) SELECT x FROM system_range(1, 3000)");
                PreparedStatement prep = conn.prepareStatement(
                        "UPDATE stuff SET text = IFNULL(text,'') || space(1000) || id");
                prep.execute();
                stat.execute("CHECKPOINT");

                ResultSet rs = stat.executeQuery("SELECT sum(length(text)) FROM stuff");
                assertTrue(rs.next());
                assertEquals(3010893, rs.getInt(1));

                eatMemory(80);
                prep.execute();
                fail();
            } catch (SQLException ignore) {
            } finally {
                freeMemory();
            }
        }
    }
}
