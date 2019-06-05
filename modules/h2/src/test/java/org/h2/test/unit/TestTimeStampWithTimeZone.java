/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;

import org.h2.api.TimestampWithTimeZone;
import org.h2.test.TestBase;
import org.h2.util.DateTimeUtils;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 */
public class TestTimeStampWithTimeZone extends TestBase {

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
        test1();
        test2();
        test3();
        test4();
        test5();
        testOrder();
        testConversions();
        deleteDb(getTestName());
    }

    private void test1() throws SQLException {
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity, t1 timestamp(9) with time zone)");
        stat.execute("insert into test(t1) values('1970-01-01 12:00:00.00+00:15')");
        // verify NanosSinceMidnight is in local time and not UTC
        stat.execute("insert into test(t1) values('2016-09-24 00:00:00.000000001+00:01')");
        stat.execute("insert into test(t1) values('2016-09-24 00:00:00.000000001-00:01')");
        // verify year month day is in local time and not UTC
        stat.execute("insert into test(t1) values('2016-01-01 05:00:00.00+10:00')");
        stat.execute("insert into test(t1) values('2015-12-31 19:00:00.00-10:00')");
        ResultSet rs = stat.executeQuery("select t1 from test");
        rs.next();
        assertEquals("1970-01-01 12:00:00+00:15", rs.getString(1));
        TimestampWithTimeZone ts = (TimestampWithTimeZone) rs.getObject(1);
        assertEquals(1970, ts.getYear());
        assertEquals(1, ts.getMonth());
        assertEquals(1, ts.getDay());
        assertEquals(15, ts.getTimeZoneOffsetMins());
        assertEquals(new TimestampWithTimeZone(1008673L, 43200000000000L, (short) 15), ts);
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("1970-01-01T12:00+00:15", rs.getObject(1,
                            LocalDateTimeUtils.OFFSET_DATE_TIME).toString());
        }
        rs.next();
        ts = (TimestampWithTimeZone) rs.getObject(1);
        assertEquals(2016, ts.getYear());
        assertEquals(9, ts.getMonth());
        assertEquals(24, ts.getDay());
        assertEquals(1, ts.getTimeZoneOffsetMins());
        assertEquals(1L, ts.getNanosSinceMidnight());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2016-09-24T00:00:00.000000001+00:01", rs.getObject(1,
                            LocalDateTimeUtils.OFFSET_DATE_TIME).toString());
        }
        rs.next();
        ts = (TimestampWithTimeZone) rs.getObject(1);
        assertEquals(2016, ts.getYear());
        assertEquals(9, ts.getMonth());
        assertEquals(24, ts.getDay());
        assertEquals(-1, ts.getTimeZoneOffsetMins());
        assertEquals(1L, ts.getNanosSinceMidnight());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2016-09-24T00:00:00.000000001-00:01", rs.getObject(1,
                            LocalDateTimeUtils.OFFSET_DATE_TIME).toString());
        }
        rs.next();
        ts = (TimestampWithTimeZone) rs.getObject(1);
        assertEquals(2016, ts.getYear());
        assertEquals(1, ts.getMonth());
        assertEquals(1, ts.getDay());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2016-01-01T05:00+10:00", rs.getObject(1,
                            LocalDateTimeUtils.OFFSET_DATE_TIME).toString());
        }
        rs.next();
        ts = (TimestampWithTimeZone) rs.getObject(1);
        assertEquals(2015, ts.getYear());
        assertEquals(12, ts.getMonth());
        assertEquals(31, ts.getDay());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2015-12-31T19:00-10:00", rs.getObject(1,
                            LocalDateTimeUtils.OFFSET_DATE_TIME).toString());
        }

        ResultSetMetaData metaData = rs.getMetaData();
        int columnType = metaData.getColumnType(1);
        // 2014 is the value of Types.TIMESTAMP_WITH_TIMEZONE
        // use the value instead of the reference because the code has to
        // compile (on Java 1.7). Can be replaced with
        // Types.TIMESTAMP_WITH_TIMEZONE
        // once Java 1.8 is required.
        assertEquals(2014, columnType);

        rs.close();
        stat.close();
        conn.close();
    }

    private void test2() {
        ValueTimestampTimeZone a = ValueTimestampTimeZone.parse("1970-01-01 12:00:00.00+00:15");
        ValueTimestampTimeZone b = ValueTimestampTimeZone.parse("1970-01-01 12:00:01.00+01:15");
        int c = a.compareTo(b, null);
        assertEquals(1, c);
        c = b.compareTo(a, null);
        assertEquals(-1, c);
    }

    private void test3() {
        ValueTimestampTimeZone a = ValueTimestampTimeZone.parse("1970-01-02 00:00:02.00+01:15");
        ValueTimestampTimeZone b = ValueTimestampTimeZone.parse("1970-01-01 23:00:01.00+00:15");
        int c = a.compareTo(b, null);
        assertEquals(1, c);
        c = b.compareTo(a, null);
        assertEquals(-1, c);
    }

    private void test4() {
        ValueTimestampTimeZone a = ValueTimestampTimeZone.parse("1970-01-02 00:00:01.00+01:15");
        ValueTimestampTimeZone b = ValueTimestampTimeZone.parse("1970-01-01 23:00:01.00+00:15");
        int c = a.compareTo(b, null);
        assertEquals(0, c);
        c = b.compareTo(a, null);
        assertEquals(0, c);
    }

    private void test5() throws SQLException {
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table test5(id identity, t1 timestamp with time zone)");
        stat.execute("insert into test5(t1) values('2016-09-24 00:00:00.000000001+00:01')");
        stat.execute("insert into test5(t1) values('2017-04-20 00:00:00.000000001+00:01')");

        PreparedStatement preparedStatement = conn.prepareStatement("select id"
                        + " from test5"
                        + " where (t1 < ?)");
        Value value = ValueTimestampTimeZone.parse("2016-12-24 00:00:00.000000001+00:01");
        preparedStatement.setObject(1, value.getObject());

        ResultSet rs = preparedStatement.executeQuery();

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        rs.close();
        preparedStatement.close();
        stat.close();
        conn.close();
    }

    private void testOrder() throws SQLException {
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table test_order(id identity, t1 timestamp with time zone)");
        stat.execute("insert into test_order(t1) values('1970-01-01 12:00:00.00+00:15')");
        stat.execute("insert into test_order(t1) values('1970-01-01 12:00:01.00+01:15')");
        ResultSet rs = stat.executeQuery("select t1 from test_order order by t1");
        rs.next();
        assertEquals("1970-01-01 12:00:01+01:15", rs.getString(1));
        conn.close();
    }

    private void testConversionsImpl(String timeStr, boolean testReverse) {
        ValueTimestamp ts = ValueTimestamp.parse(timeStr);
        ValueDate d = (ValueDate) ts.convertTo(Value.DATE);
        ValueTime t = (ValueTime) ts.convertTo(Value.TIME);
        ValueTimestampTimeZone tstz = ValueTimestampTimeZone.parse(timeStr);
        assertEquals(ts, tstz.convertTo(Value.TIMESTAMP));
        assertEquals(d, tstz.convertTo(Value.DATE));
        assertEquals(t, tstz.convertTo(Value.TIME));
        assertEquals(ts.getTimestamp(), tstz.getTimestamp());
        if (testReverse) {
            assertEquals(0, tstz.compareTo(ts.convertTo(Value.TIMESTAMP_TZ), null));
            assertEquals(d.convertTo(Value.TIMESTAMP).convertTo(Value.TIMESTAMP_TZ),
                    d.convertTo(Value.TIMESTAMP_TZ));
            assertEquals(t.convertTo(Value.TIMESTAMP).convertTo(Value.TIMESTAMP_TZ),
                    t.convertTo(Value.TIMESTAMP_TZ));
        }
    }

    private void testConversions() {
        TimeZone current = TimeZone.getDefault();
        try {
            for (String id : TimeZone.getAvailableIDs()) {
                TimeZone.setDefault(TimeZone.getTimeZone(id));
                DateTimeUtils.resetCalendar();
                testConversionsImpl("2017-12-05 23:59:30.987654321-12:00", true);
                testConversionsImpl("2000-01-02 10:20:30.123456789+07:30", true);
                boolean testReverse = !"Africa/Monrovia".equals(id);
                testConversionsImpl("1960-04-06 12:13:14.777666555+12:00", testReverse);
            }
        } finally {
            TimeZone.setDefault(current);
            DateTimeUtils.resetCalendar();
        }
    }

}
