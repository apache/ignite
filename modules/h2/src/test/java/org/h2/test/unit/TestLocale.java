/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import org.h2.test.TestBase;

/**
 * Tests that change the default locale.
 */
public class TestLocale extends TestBase {

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
        testSpecialLocale();
        testDatesInJapanLocale();
    }

    private void testSpecialLocale() throws SQLException {
        deleteDb(getTestName());
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        Locale old = Locale.getDefault();
        try {
            // when using Turkish as the default locale, "i".toUpperCase() is
            // not "I"
            Locale.setDefault(new Locale("tr"));
            stat.execute("create table test(I1 int, i2 int, b int, c int, d int) " +
                    "as select 1, 1, 1, 1, 1");
            ResultSet rs = stat.executeQuery("select * from test");
            rs.next();
            rs.getString("I1");
            rs.getString("i1");
            rs.getString("I2");
            rs.getString("i2");
            stat.execute("drop table test");
        } finally {
            Locale.setDefault(old);
        }
        conn.close();
    }

    private void testDatesInJapanLocale() throws SQLException {
        deleteDb(getTestName());
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        Locale old = Locale.getDefault();
        try {
            // when using Japanese as the default locale, the default calendar is
            // the imperial japanese calendar
            Locale.setDefault(new Locale("ja", "JP", "JP"));
            stat.execute("CREATE TABLE test(d TIMESTAMP, dz TIMESTAMP WITH TIME ZONE) " +
                    "as select '2017-12-03T00:00:00Z', '2017-12-03T00:00:00Z'");
            ResultSet rs = stat.executeQuery("select YEAR(d) y, YEAR(dz) yz from test");
            rs.next();
            assertEquals(2017, rs.getInt("y"));
            assertEquals(2017, rs.getInt("yz"));
            stat.execute("drop table test");

            rs = stat.executeQuery(
                    "CALL FORMATDATETIME(TIMESTAMP '2001-02-03 04:05:06', 'yyyy-MM-dd HH:mm:ss', 'en')");
            rs.next();
            assertEquals("2001-02-03 04:05:06", rs.getString(1));

        } finally {
            Locale.setDefault(old);
        }
        conn.close();
    }

}
