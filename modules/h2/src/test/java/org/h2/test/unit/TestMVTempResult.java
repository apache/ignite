/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.lang.ProcessBuilder.Redirect;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;

import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;

/**
 * Tests that MVTempResult implementations do not produce OOME.
 */
public class TestMVTempResult extends TestBase {

    private static final int MEMORY = 64;

    private static final int ROWS = 1_000_000;

    /**
     * May be used to run only this test and may be launched by this test in a
     * subprocess.
     *
     * @param a
     *            if empty run this test, if not empty run the subprocess
     */
    public static void main(String... a) throws Exception {
        TestMVTempResult test = (TestMVTempResult) TestBase.createCaller().init();
        if (a.length == 0) {
            test.test();
        } else {
            test.runTest();
        }
    }

    @Override
    public void test() throws Exception {
        ProcessBuilder pb = new ProcessBuilder().redirectError(Redirect.INHERIT);
        pb.command(getJVM(), "-Xmx" + MEMORY + "M", "-cp", getClassPath(), "-ea", getClass().getName(), "dummy");
        assertEquals(0, pb.start().waitFor());
    }

    private void runTest() throws SQLException {
        String dir = getBaseDir();
        String name = "testResultExternal";
        DeleteDbFiles.execute(dir, name, true);
        try (Connection c = DriverManager.getConnection("jdbc:h2:" + dir + '/' + name)) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE TEST(I BIGINT, E ENUM('a', 'b'))" //
                    + " AS SELECT X, 'a' FROM SYSTEM_RANGE(1, " + ROWS + ')');
            try (ResultSet rs = s.executeQuery("SELECT I, E FROM TEST ORDER BY I DESC")) {
                for (int i = ROWS; i > 0; i--) {
                    assertTrue(rs.next());
                    assertEquals(i, rs.getLong(1));
                    assertEquals("a", rs.getString(2));
                }
                assertFalse(rs.next());
            }
            BitSet set = new BitSet(ROWS);
            try (ResultSet rs = s.executeQuery("SELECT I, E FROM TEST")) {
                for (int i = 1; i <= ROWS; i++) {
                    assertTrue(rs.next());
                    set.set((int) rs.getLong(1));
                    assertEquals("a", rs.getString(2));
                }
                assertFalse(rs.next());
                assertEquals(ROWS, set.cardinality());
            }
        }
        DeleteDbFiles.execute(dir, name, true);
    }

}
