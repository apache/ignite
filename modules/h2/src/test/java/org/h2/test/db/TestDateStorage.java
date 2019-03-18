/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import org.h2.test.TestBase;
import org.h2.test.unit.TestDate;
import org.h2.util.DateTimeUtils;
import org.h2.value.ValueTimestamp;

/**
 * Tests the date transfer and storage.
 */
public class TestDateStorage extends TestBase {

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
        deleteDb(getTestName());
        testDateTimeTimestampWithCalendar();
        testMoveDatabaseToAnotherTimezone();
        testAllTimeZones();
        testCurrentTimeZone();
    }

    private void testDateTimeTimestampWithCalendar() throws SQLException {
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table ts(x timestamp primary key)");
        stat.execute("create table t(x time primary key)");
        stat.execute("create table d(x date)");
        Calendar utcCalendar = new GregorianCalendar(new SimpleTimeZone(0, "Z"));
        TimeZone old = TimeZone.getDefault();
        DateTimeUtils.resetCalendar();
        TimeZone.setDefault(TimeZone.getTimeZone("PST"));
        try {
            Timestamp ts1 = Timestamp.valueOf("2010-03-13 18:15:00");
            Time t1 = new Time(ts1.getTime());
            Date d1 = new Date(ts1.getTime());
            // when converted to UTC, this is 03:15, which doesn't actually
            // exist because of summer time change at that day
            Timestamp ts2 = Timestamp.valueOf("2010-03-13 19:15:00");
            Time t2 = new Time(ts2.getTime());
            Date d2 = new Date(ts2.getTime());
            PreparedStatement prep;
            ResultSet rs;
            prep = conn.prepareStatement("insert into ts values(?)");
            prep.setTimestamp(1, ts1, utcCalendar);
            prep.execute();
            prep.setTimestamp(1, ts2, utcCalendar);
            prep.execute();
            prep = conn.prepareStatement("insert into t values(?)");
            prep.setTime(1, t1, utcCalendar);
            prep.execute();
            prep.setTime(1, t2, utcCalendar);
            prep.execute();
            prep = conn.prepareStatement("insert into d values(?)");
            prep.setDate(1, d1, utcCalendar);
            prep.execute();
            prep.setDate(1, d2, utcCalendar);
            prep.execute();
            rs = stat.executeQuery("select * from ts order by x");
            rs.next();
            assertEquals("2010-03-14 02:15:00",
                    rs.getString(1));
            assertEquals("2010-03-13 18:15:00.0",
                    rs.getTimestamp(1, utcCalendar).toString());
            assertEquals("2010-03-14 03:15:00.0",
                    rs.getTimestamp(1).toString());
            assertEquals("2010-03-14 02:15:00",
                    rs.getString("x"));
            assertEquals("2010-03-13 18:15:00.0",
                    rs.getTimestamp("x", utcCalendar).toString());
            assertEquals("2010-03-14 03:15:00.0",
                    rs.getTimestamp("x").toString());
            rs.next();
            assertEquals("2010-03-14 03:15:00",
                    rs.getString(1));
            assertEquals("2010-03-13 19:15:00.0",
                    rs.getTimestamp(1, utcCalendar).toString());
            assertEquals("2010-03-14 03:15:00.0",
                    rs.getTimestamp(1).toString());
            assertEquals("2010-03-14 03:15:00",
                    rs.getString("x"));
            assertEquals("2010-03-13 19:15:00.0",
                    rs.getTimestamp("x", utcCalendar).toString());
            assertEquals("2010-03-14 03:15:00.0",
                    rs.getTimestamp("x").toString());
            rs = stat.executeQuery("select * from t order by x");
            rs.next();
            assertEquals("02:15:00", rs.getString(1));
            assertEquals("18:15:00", rs.getTime(1, utcCalendar).toString());
            assertEquals("02:15:00", rs.getTime(1).toString());
            assertEquals("02:15:00", rs.getString("x"));
            assertEquals("18:15:00", rs.getTime("x", utcCalendar).toString());
            assertEquals("02:15:00", rs.getTime("x").toString());
            rs.next();
            assertEquals("03:15:00", rs.getString(1));
            assertEquals("19:15:00", rs.getTime(1, utcCalendar).toString());
            assertEquals("03:15:00", rs.getTime(1).toString());
            assertEquals("03:15:00", rs.getString("x"));
            assertEquals("19:15:00", rs.getTime("x", utcCalendar).toString());
            assertEquals("03:15:00", rs.getTime("x").toString());
            rs = stat.executeQuery("select * from d order by x");
            rs.next();
            assertEquals("2010-03-14", rs.getString(1));
            assertEquals("2010-03-13", rs.getDate(1, utcCalendar).toString());
            assertEquals("2010-03-14", rs.getDate(1).toString());
            assertEquals("2010-03-14", rs.getString("x"));
            assertEquals("2010-03-13", rs.getDate("x", utcCalendar).toString());
            assertEquals("2010-03-14", rs.getDate("x").toString());
            rs.next();
            assertEquals("2010-03-14", rs.getString(1));
            assertEquals("2010-03-13", rs.getDate(1, utcCalendar).toString());
            assertEquals("2010-03-14", rs.getDate(1).toString());
            assertEquals("2010-03-14", rs.getString("x"));
            assertEquals("2010-03-13", rs.getDate("x", utcCalendar).toString());
            assertEquals("2010-03-14", rs.getDate("x").toString());
        } finally {
            TimeZone.setDefault(old);
            DateTimeUtils.resetCalendar();
        }
        stat.execute("drop table ts");
        stat.execute("drop table t");
        stat.execute("drop table d");
        conn.close();
    }

    private void testMoveDatabaseToAnotherTimezone() throws SQLException {
        if (config.memory) {
            return;
        }
        if (config.mvStore) {
            return;
        }
        String db = getTestName() + ";LOG=0;FILE_LOCK=NO";
        Connection conn = getConnection(db);
        Statement stat;
        stat = conn.createStatement();
        stat.execute("create table date_list(tz varchar, t varchar, ts timestamp)");
        conn.close();
        TimeZone defaultTimeZone = TimeZone.getDefault();
        ArrayList<TimeZone> distinct = TestDate.getDistinctTimeZones();
        try {
            for (TimeZone tz : distinct) {
                // println("insert using " + tz.getID());
                TimeZone.setDefault(tz);
                DateTimeUtils.resetCalendar();
                conn = getConnection(db);
                PreparedStatement prep = conn.prepareStatement(
                        "insert into date_list values(?, ?, ?)");
                prep.setString(1, tz.getID());
                for (int m = 1; m < 10; m++) {
                    String s = "2000-0" + m + "-01 15:00:00";
                    prep.setString(2, s);
                    prep.setTimestamp(3, Timestamp.valueOf(s));
                    prep.execute();
                }
                conn.close();
            }
            // printTime("inserted");
            for (TimeZone target : distinct) {
                // println("select from " + target.getID());
                if ("Pacific/Kiritimati".equals(target.getID())) {
                    // there is a problem with this time zone, but it seems
                    // unrelated to this database (possibly wrong timezone
                    // information?)
                    continue;
                }
                TimeZone.setDefault(target);
                DateTimeUtils.resetCalendar();
                conn = getConnection(db);
                stat = conn.createStatement();
                ResultSet rs = stat.executeQuery("select * from date_list order by t");
                while (rs.next()) {
                    String source = rs.getString(1);
                    String a = rs.getString(2);
                    String b = rs.getString(3);
                    b = b.substring(0, a.length());
                    if (!a.equals(b)) {
                        assertEquals(source + ">" + target, a, b);
                    }
                }
                conn.close();
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
            DateTimeUtils.resetCalendar();
        }
        // printTime("done");
        conn = getConnection(db);
        stat = conn.createStatement();
        stat.execute("drop table date_list");
        conn.close();
    }

    private static void testCurrentTimeZone() {
        for (int year = 1890; year < 2050; year += 3) {
            for (int month = 1; month <= 12; month++) {
                for (int day = 1; day < 29; day++) {
                    for (int hour = 0; hour < 24; hour++) {
                        test(year, month, day, hour);
                    }
                }
            }
        }
    }

    private static void test(int year, int month, int day, int hour) {
        ValueTimestamp.parse(year + "-" + month + "-" + day + " " + hour + ":00:00");
    }

    private void testAllTimeZones() throws SQLException {
        Connection conn = getConnection(getTestName());
        TimeZone defaultTimeZone = TimeZone.getDefault();
        PreparedStatement prep = conn.prepareStatement("CALL CAST(? AS DATE)");
        try {
            ArrayList<TimeZone> distinct = TestDate.getDistinctTimeZones();
            for (TimeZone tz : distinct) {
                // println(tz.getID());
                TimeZone.setDefault(tz);
                DateTimeUtils.resetCalendar();
                for (int d = 101; d < 129; d++) {
                    test(prep, d);
                }
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
            DateTimeUtils.resetCalendar();
        }
        conn.close();
        deleteDb(getTestName());
    }

    private void test(PreparedStatement prep, int d) throws SQLException {
        String s = "2040-10-" + ("" + d).substring(1);
        // some dates don't work in some versions of Java
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6772689
        java.sql.Date date = java.sql.Date.valueOf(s);
        long time = date.getTime();
        while (true) {
            date = new java.sql.Date(time);
            String x = date.toString();
            if (x.equals(s)) {
                break;
            }
            time += 1000;
        }
        if (!date.toString().equals(s)) {
            println(TimeZone.getDefault().getID() + " " + s + " <> " + date.toString());
            return;
        }
        prep.setString(1, s);
        ResultSet rs = prep.executeQuery();
        rs.next();
        String t = rs.getString(1);
        if (!s.equals(t)) {
            assertEquals(TimeZone.getDefault().getID(), s, t);
        }
    }

}
