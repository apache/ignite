/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * Tests the sample apps.
 */
public class TestSampleApps extends TestBase {

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
        if (!getBaseDir().startsWith(TestBase.BASE_TEST_DIR)) {
            return;
        }
        deleteDb(getTestName());
        InputStream in = getClass().getClassLoader().getResourceAsStream(
                "org/h2/samples/optimizations.sql");
        new File(getBaseDir()).mkdirs();
        FileOutputStream out = new FileOutputStream(getBaseDir() +
                "/optimizations.sql");
        IOUtils.copyAndClose(in, out);
        String url = "jdbc:h2:" + getBaseDir() + "/" + getTestName();
        testApp("", org.h2.tools.RunScript.class, "-url", url, "-user", "sa",
                "-password", "sa", "-script", getBaseDir() +
                        "/optimizations.sql", "-checkResults");
        deleteDb(getTestName());
        testApp("Compacting...\nDone.", org.h2.samples.Compact.class);
        testApp("NAME: Bob Meier\n" +
                "EMAIL: bob.meier@abcde.abc\n" +
                "PHONE: +41123456789\n\n" +
                "NAME: John Jones\n" +
                "EMAIL: john.jones@abcde.abc\n" +
                "PHONE: +41976543210\n",
                org.h2.samples.CsvSample.class);
        testApp("",
                org.h2.samples.CachedPreparedStatements.class);
        testApp("2 is prime\n" +
                "3 is prime\n" +
                "5 is prime\n" +
                "7 is prime\n" +
                "11 is prime\n" +
                "13 is prime\n" +
                "17 is prime\n" +
                "19 is prime\n" +
                "30\n" +
                "20\n" +
                "0/0\n" +
                "0/1\n" +
                "1/0\n" +
                "1/1\n" +
                "10",
                org.h2.samples.Function.class);
        // Not compatible with PostgreSQL JDBC driver (throws a
        // NullPointerException):
        // testApp(org.h2.samples.SecurePassword.class, null, "Joe");
        // TODO test ShowProgress (percent numbers are hardware specific)
        // TODO test ShutdownServer (server needs to be started in a separate
        // process)
        testApp("The sum is 20.00", org.h2.samples.TriggerSample.class);
        testApp("Hello: 1\nWorld: 2", org.h2.samples.TriggerPassData.class);
        testApp("table test:\n" +
                "1 Hallo\n\n" +
                "test_view:\n" +
                "1 Hallo",
                org.h2.samples.UpdatableView.class);
        testApp(
                "adding test data...\n" +
                "defrag to reduce random access...\n" +
                "create the zip file...\n" +
                "open the database from the zip file...",
                org.h2.samples.ReadOnlyDatabaseInZip.class);
        testApp(
                "a: 1/Hello!\n" +
                "b: 1/Hallo!\n" +
                "1/A/Hello!\n" +
                "1/B/Hallo!",
                org.h2.samples.RowAccessRights.class);

        // tools
        testApp("Allows changing the database file encryption password or algorithm*",
                org.h2.tools.ChangeFileEncryption.class, "-help");
        testApp("Allows changing the database file encryption password or algorithm*",
                org.h2.tools.ChangeFileEncryption.class);
        testApp("Deletes all files belonging to a database.*",
                org.h2.tools.DeleteDbFiles.class, "-help");
        FileUtils.delete(getBaseDir() + "/optimizations.sql");
    }

    private void testApp(String expected, Class<?> clazz, String... args)
            throws Exception {
        DeleteDbFiles.execute("data", "test", true);
        Method m = clazz.getMethod("main", String[].class);
        PrintStream oldOut = System.out, oldErr = System.err;
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(buff, false, "UTF-8");
        System.setOut(out);
        System.setErr(out);
        try {
            m.invoke(null, new Object[] { args });
        } catch (InvocationTargetException e) {
            TestBase.logError("error", e.getTargetException());
        } catch (Throwable e) {
            TestBase.logError("error", e);
        }
        out.flush();
        System.setOut(oldOut);
        System.setErr(oldErr);
        String s = new String(buff.toByteArray(), StandardCharsets.UTF_8);
        s = StringUtils.replaceAll(s, "\r\n", "\n");
        s = s.trim();
        expected = expected.trim();
        if (expected.endsWith("*")) {
            expected = expected.substring(0, expected.length() - 1);
            if (!s.startsWith(expected)) {
                assertEquals(expected.trim(), s.trim());
            }
        } else {
            assertEquals(expected.trim(), s.trim());
        }
    }
}
