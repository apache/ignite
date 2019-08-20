/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.h2.test.TestBase;
import org.h2.tools.MultiDimension;

/**
 * Tests the multi-dimension index tool.
 */
public class TestMultiDimension extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.traceTest = true;
        test.test();
    }

    @Override
    public void test() throws SQLException {
        testHelperMethods();
        testPerformance2d();
        testPerformance3d();
    }

    private void testHelperMethods() {
        MultiDimension m = MultiDimension.getInstance();
        assertEquals(Integer.MAX_VALUE, m.getMaxValue(2));
        assertEquals(0, m.normalize(2, 0, 0, 100));
        assertEquals(Integer.MAX_VALUE / 2, m.normalize(2, 50, 0, 100));
        assertEquals(Integer.MAX_VALUE, m.normalize(2, 100, 0, 100));
        assertEquals(Integer.MAX_VALUE / 10, m.normalize(2, 0.1, 0, 1));
        assertEquals(0, m.normalize(2, 1, 1, 1));
        assertEquals(0, m.normalize(2, 0, 0, 0));
        assertEquals(3, m.interleave(1, 1));
        assertEquals(3, m.interleave(new int[]{1, 1}));
        assertEquals(5, m.interleave(3, 0));
        assertEquals(5, m.interleave(new int[]{3, 0}));
        assertEquals(10, m.interleave(0, 3));
        assertEquals(10, m.interleave(new int[] { 0, 3 }));
        long v = Integer.MAX_VALUE | ((long) Integer.MAX_VALUE << 31L);
        assertEquals(v, m.interleave(Integer.MAX_VALUE, Integer.MAX_VALUE));
        assertEquals(v, m.interleave(new int[] {
                Integer.MAX_VALUE, Integer.MAX_VALUE }));
        Random random = new Random(1);
        for (int i = 0; i < 1000; i++) {
            int x = random.nextInt(Integer.MAX_VALUE), y =
                    random.nextInt(Integer.MAX_VALUE);
            v = m.interleave(new int[] {x, y});
            long v2 = m.interleave(x, y);
            assertEquals(v, v2);
            int x1 = m.deinterleave(2, v, 0);
            int y1 = m.deinterleave(2, v, 1);
            assertEquals(x, x1);
            assertEquals(y, y1);
        }
        for (int i = 0; i < 1000; i++) {
            int x = random.nextInt(1000), y = random.nextInt(1000),
                    z = random.nextInt(1000);
            MultiDimension tool = MultiDimension.getInstance();
            long xyz = tool.interleave(new int[] { x, y, z });
            assertEquals(x, tool.deinterleave(3, xyz, 0));
            assertEquals(y, tool.deinterleave(3, xyz, 1));
            assertEquals(z, tool.deinterleave(3, xyz, 2));
        }
        createClassProxy(MultiDimension.class);
        assertThrows(IllegalArgumentException.class, m).getMaxValue(1);
        assertThrows(IllegalArgumentException.class, m).getMaxValue(33);
        assertThrows(IllegalArgumentException.class, m).normalize(2, 10, 11, 12);
        assertThrows(IllegalArgumentException.class, m).normalize(2, 5, 10, 0);
        assertThrows(IllegalArgumentException.class, m).normalize(2, 10, 0, 9);
        assertThrows(IllegalArgumentException.class, m).interleave(-1, 5);
        assertThrows(IllegalArgumentException.class, m).interleave(5, -1);
        assertThrows(IllegalArgumentException.class, m).
                interleave(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    private void testPerformance2d() throws SQLException {
        deleteDb("multiDimension");
        Connection conn;
        conn = getConnection("multiDimension");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS MAP FOR \"" +
        getClass().getName() + ".interleave\"");
        stat.execute("CREATE TABLE TEST(X INT NOT NULL, Y INT NOT NULL, " +
                "XY BIGINT AS MAP(X, Y), DATA VARCHAR)");
        stat.execute("CREATE INDEX IDX_X ON TEST(X, Y)");
        stat.execute("CREATE INDEX IDX_XY ON TEST(XY)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST(X, Y, DATA) VALUES(?, ?, ?)");
        // the MultiDimension tool is faster for 4225 (65^2) points
        // the more the bigger the difference
        int max = getSize(30, 65);
        long time = System.nanoTime();
        for (int x = 0; x < max; x++) {
            for (int y = 0; y < max; y++) {
                long t2 = System.nanoTime();
                if (t2 - time > TimeUnit.SECONDS.toNanos(1)) {
                    int percent = (int) (100.0 * ((double) x * max + y) /
                            ((double) max * max));
                    trace(percent + "%");
                    time = t2;
                }
                prep.setInt(1, x);
                prep.setInt(2, y);
                prep.setString(3, "Test data");
                prep.execute();
            }
        }
        stat.execute("ANALYZE SAMPLE_SIZE 10000");
        PreparedStatement prepRegular = conn.prepareStatement(
                "SELECT * FROM TEST WHERE X BETWEEN ? AND ? " +
                "AND Y BETWEEN ? AND ? ORDER BY X, Y");
        MultiDimension multi = MultiDimension.getInstance();
        String sql = multi.generatePreparedQuery("TEST", "XY",
                new String[] { "X", "Y" });
        sql += " ORDER BY X, Y";
        PreparedStatement prepMulti = conn.prepareStatement(sql);
        long timeMulti = 0, timeRegular = 0;
        int timeMax = getSize(500, 2000);
        Random rand = new Random(1);
        while (timeMulti < timeMax) {
            int size = rand.nextInt(max / 10);
            int minX = rand.nextInt(max - size);
            int minY = rand.nextInt(max - size);
            int maxX = minX + size, maxY = minY + size;
            time = System.nanoTime();
            ResultSet rs1 = multi.getResult(prepMulti,
                    new int[] { minX, minY }, new int[] { maxX, maxY });
            timeMulti += System.nanoTime() - time;
            time = System.nanoTime();
            prepRegular.setInt(1, minX);
            prepRegular.setInt(2, maxX);
            prepRegular.setInt(3, minY);
            prepRegular.setInt(4, maxY);
            ResultSet rs2 = prepRegular.executeQuery();
            timeRegular += System.nanoTime() - time;
            while (rs1.next()) {
                assertTrue(rs2.next());
                assertEquals(rs1.getInt(1), rs2.getInt(1));
                assertEquals(rs1.getInt(2), rs2.getInt(2));
            }
            assertFalse(rs2.next());
        }
        conn.close();
        deleteDb("multiDimension");
        trace("2d: regular: " + TimeUnit.NANOSECONDS.toMillis(timeRegular) +
                " MultiDimension: " + TimeUnit.NANOSECONDS.toMillis(timeMulti));
    }

    private void testPerformance3d() throws SQLException {
        deleteDb("multiDimension");
        Connection conn;
        conn = getConnection("multiDimension");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS MAP FOR \"" +
                getClass().getName() + ".interleave\"");
        stat.execute("CREATE TABLE TEST(X INT NOT NULL, " +
                "Y INT NOT NULL, Z INT NOT NULL, "
                + "XYZ BIGINT AS MAP(X, Y, Z), DATA VARCHAR)");
        stat.execute("CREATE INDEX IDX_X ON TEST(X, Y, Z)");
        stat.execute("CREATE INDEX IDX_XYZ ON TEST(XYZ)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST(X, Y, Z, DATA) VALUES(?, ?, ?, ?)");
        // the MultiDimension tool is faster for 8000 (20^3) points
        // the more the bigger the difference
        int max = getSize(10, 20);
        long time = System.nanoTime();
        for (int x = 0; x < max; x++) {
            for (int y = 0; y < max; y++) {
                for (int z = 0; z < max; z++) {
                    long t2 = System.nanoTime();
                    if (t2 - time > TimeUnit.SECONDS.toNanos(1)) {
                        int percent = (int) (100.0 * ((double) x * max + y) /
                                ((double) max * max));
                        trace(percent + "%");
                        time = t2;
                    }
                    prep.setInt(1, x);
                    prep.setInt(2, y);
                    prep.setInt(3, z);
                    prep.setString(4, "Test data");
                    prep.execute();
                }
            }
        }
        stat.execute("ANALYZE SAMPLE_SIZE 10000");
        PreparedStatement prepRegular = conn.prepareStatement(
                "SELECT * FROM TEST WHERE X BETWEEN ? AND ? " +
                "AND Y BETWEEN ? AND ? AND Z BETWEEN ? AND ? ORDER BY X, Y, Z");
        MultiDimension multi = MultiDimension.getInstance();
        String sql = multi.generatePreparedQuery("TEST", "XYZ", new String[] {
                "X", "Y", "Z" });
        sql += " ORDER BY X, Y, Z";
        PreparedStatement prepMulti = conn.prepareStatement(sql);
        long timeMulti = 0, timeRegular = 0;
        int timeMax = getSize(500, 2000);
        Random rand = new Random(1);
        while (timeMulti < timeMax) {
            int size = rand.nextInt(max / 10);
            int minX = rand.nextInt(max - size);
            int minY = rand.nextInt(max - size);
            int minZ = rand.nextInt(max - size);
            int maxX = minX + size, maxY = minY + size, maxZ = minZ + size;
            time = System.nanoTime();
            ResultSet rs1 = multi.getResult(prepMulti, new int[] { minX, minY,
                    minZ }, new int[] { maxX, maxY, maxZ });
            timeMulti += System.nanoTime() - time;
            time = System.nanoTime();
            prepRegular.setInt(1, minX);
            prepRegular.setInt(2, maxX);
            prepRegular.setInt(3, minY);
            prepRegular.setInt(4, maxY);
            prepRegular.setInt(5, minZ);
            prepRegular.setInt(6, maxZ);
            ResultSet rs2 = prepRegular.executeQuery();
            timeRegular += System.nanoTime() - time;
            while (rs1.next()) {
                assertTrue(rs2.next());
                assertEquals(rs1.getInt(1), rs2.getInt(1));
                assertEquals(rs1.getInt(2), rs2.getInt(2));
            }
            assertFalse(rs2.next());
        }
        conn.close();
        deleteDb("multiDimension");
        trace("3d: regular: " + TimeUnit.NANOSECONDS.toMillis(timeRegular) +
                " MultiDimension: " + TimeUnit.NANOSECONDS.toMillis(timeMulti));
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param x the x value
     * @param y the y value
     * @return the bit-interleaved value
     */
    public static long interleave(int x, int y) {
        return MultiDimension.getInstance().interleave(x, y);
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param x the x value
     * @param y the y value
     * @param z the z value
     * @return the bit-interleaved value
     */
    public static long interleave(int x, int y, int z) {
        return MultiDimension.getInstance().interleave(new int[] { x, y, z });
    }

}
