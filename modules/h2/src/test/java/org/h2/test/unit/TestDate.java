/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.test.TestBase;
import org.h2.test.utils.AssertThrows;
import org.h2.util.DateTimeUtils;
import org.h2.util.New;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInt;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * Tests the date parsing. The problem is that some dates are not allowed
 * because of the summer time change. Most countries change at 2 o'clock in the
 * morning to 3 o'clock, but some (for example Chile) change at midnight.
 * Non-lenient parsing would not work in this case.
 */
public class TestDate extends TestBase {

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
        testValueDate();
        testValueTime();
        testValueTimestamp();
        testValueTimestampWithTimezone();
        testValidDate();
        testAbsoluteDay();
        testCalculateLocalMillis();
        testDateTimeUtils();
    }

    private void testValueDate() {
        assertEquals("2000-01-01",
                ValueDate.get(Date.valueOf("2000-01-01")).getString());
        assertEquals("0-00-00",
                ValueDate.fromDateValue(0).getString());
        assertEquals("9999-12-31",
                ValueDate.parse("9999-12-31").getString());
        assertEquals("-9999-12-31",
                ValueDate.parse("-9999-12-31").getString());
        assertEquals(Integer.MAX_VALUE + "-12-31",
                ValueDate.parse(Integer.MAX_VALUE + "-12-31").getString());
        assertEquals(Integer.MIN_VALUE + "-12-31",
                ValueDate.parse(Integer.MIN_VALUE + "-12-31").getString());
        ValueDate d1 = ValueDate.parse("2001-01-01");
        assertEquals("2001-01-01", d1.getDate().toString());
        assertEquals("DATE '2001-01-01'", d1.getSQL());
        assertEquals("DATE '2001-01-01'", d1.toString());
        assertEquals(Value.DATE, d1.getType());
        long dv = d1.getDateValue();
        assertEquals((int) ((dv >>> 32) ^ dv), d1.hashCode());
        assertEquals(d1.getString().length(), d1.getDisplaySize());
        assertEquals(ValueDate.PRECISION, d1.getPrecision());
        assertEquals("java.sql.Date", d1.getObject().getClass().getName());
        ValueDate d1b = ValueDate.parse("2001-01-01");
        assertTrue(d1 == d1b);
        Value.clearCache();
        d1b = ValueDate.parse("2001-01-01");
        assertFalse(d1 == d1b);
        assertTrue(d1.equals(d1));
        assertTrue(d1.equals(d1b));
        assertTrue(d1b.equals(d1));
        assertEquals(0, d1.compareTo(d1b, null));
        assertEquals(0, d1b.compareTo(d1, null));
        ValueDate d2 = ValueDate.parse("2002-02-02");
        assertFalse(d1.equals(d2));
        assertFalse(d2.equals(d1));
        assertEquals(-1, d1.compareTo(d2, null));
        assertEquals(1, d2.compareTo(d1, null));

        // can't convert using java.util.Date
        assertEquals(
                Integer.MAX_VALUE + "-12-31 00:00:00",
                ValueDate.parse(Integer.MAX_VALUE + "-12-31").
                convertTo(Value.TIMESTAMP).getString());
        assertEquals(
                Integer.MIN_VALUE + "-12-31 00:00:00",
                ValueDate.parse(Integer.MIN_VALUE + "-12-31").
                convertTo(Value.TIMESTAMP).getString());
        assertEquals(
                "00:00:00",
                ValueDate.parse(Integer.MAX_VALUE + "-12-31").
                convertTo(Value.TIME).getString());
        assertEquals(
                "00:00:00",
                ValueDate.parse(Integer.MIN_VALUE + "-12-31").
                convertTo(Value.TIME).getString());
    }

    private void testValueTime() {
        assertEquals("10:20:30", ValueTime.get(Time.valueOf("10:20:30")).getString());
        assertEquals("00:00:00", ValueTime.fromNanos(0).getString());
        assertEquals("23:59:59", ValueTime.parse("23:59:59").getString());
        assertEquals("11:22:33.444555666", ValueTime.parse("11:22:33.444555666").getString());
        if (SysProperties.UNLIMITED_TIME_RANGE) {
            assertEquals("99:59:59", ValueTime.parse("99:59:59").getString());
            assertEquals("-00:10:10", ValueTime.parse("-00:10:10").getString());
            assertEquals("-99:02:03.001002003",
                    ValueTime.parse("-99:02:03.001002003").getString());
            assertEquals("-99:02:03.001002",
                    ValueTime.parse("-99:02:03.001002000").getString());
            assertEquals("-99:02:03",
                    ValueTime.parse("-99:02:03.0000000000001").getString());
            assertEquals("1999999:59:59.999999999",
                    ValueTime.parse("1999999:59:59.999999999").getString());
            assertEquals("-1999999:59:59.999999999",
                    ValueTime.parse("-1999999:59:59.999999999").getString());
            assertEquals("2562047:47:16.854775807",
                    ValueTime.fromNanos(Long.MAX_VALUE).getString());
            assertEquals("-2562047:47:16.854775808",
                    ValueTime.fromNanos(Long.MIN_VALUE).getString());
        } else {
            try {
                ValueTime.parse("-00:00:00.000000001");
                fail();
            } catch (DbException ex) {
                assertEquals(ErrorCode.INVALID_DATETIME_CONSTANT_2, ex.getErrorCode());
            }
            try {
                ValueTime.parse("24:00:00");
                fail();
            } catch (DbException ex) {
                assertEquals(ErrorCode.INVALID_DATETIME_CONSTANT_2, ex.getErrorCode());
            }
        }
        ValueTime t1 = ValueTime.parse("11:11:11");
        assertEquals("11:11:11", t1.getTime().toString());
        assertEquals("1970-01-01", t1.getDate().toString());
        assertEquals("TIME '11:11:11'", t1.getSQL());
        assertEquals("TIME '11:11:11'", t1.toString());
        assertEquals(1, t1.getSignum());
        assertEquals(0, t1.multiply(ValueInt.get(0)).getSignum());
        assertEquals(0, t1.subtract(t1).getSignum());
        assertEquals("05:35:35.5", t1.multiply(ValueDouble.get(0.5)).getString());
        assertEquals("22:22:22", t1.divide(ValueDouble.get(0.5)).getString());
        assertEquals(Value.TIME, t1.getType());
        long nanos = t1.getNanos();
        assertEquals((int) ((nanos >>> 32) ^ nanos), t1.hashCode());
        // Literals return maximum precision
        assertEquals(ValueTime.MAXIMUM_PRECISION, t1.getDisplaySize());
        assertEquals(ValueTime.MAXIMUM_PRECISION, t1.getPrecision());
        assertEquals("java.sql.Time", t1.getObject().getClass().getName());
        ValueTime t1b = ValueTime.parse("11:11:11");
        assertTrue(t1 == t1b);
        Value.clearCache();
        t1b = ValueTime.parse("11:11:11");
        assertFalse(t1 == t1b);
        assertTrue(t1.equals(t1));
        assertTrue(t1.equals(t1b));
        assertTrue(t1b.equals(t1));
        assertEquals(0, t1.compareTo(t1b, null));
        assertEquals(0, t1b.compareTo(t1, null));
        ValueTime t2 = ValueTime.parse("22:22:22");
        assertFalse(t1.equals(t2));
        assertFalse(t2.equals(t1));
        assertEquals(-1, t1.compareTo(t2, null));
        assertEquals(1, t2.compareTo(t1, null));

        if (SysProperties.UNLIMITED_TIME_RANGE) {
            assertEquals(-1, t1.negate().getSignum());
            assertEquals("-11:11:11", t1.negate().getString());
            assertEquals("11:11:11", t1.negate().negate().getString());
            assertEquals("33:33:33", t1.add(t2).getString());
            assertEquals("33:33:33", t1.multiply(ValueInt.get(4)).subtract(t1).getString());

            // can't convert using java.util.Date
            assertEquals(
                    "1969-12-31 23:00:00.0",
                    ValueTime.parse("-1:00:00").
                    convertTo(Value.TIMESTAMP).getString());
            assertEquals(
                    "1970-01-01",
                    ValueTime.parse("-1:00:00").
                    convertTo(Value.DATE).getString());
        }
    }

    private void testValueTimestampWithTimezone() {
        for (int m = 1; m <= 12; m++) {
            for (int d = 1; d <= 28; d++) {
                for (int h = 0; h <= 23; h++) {
                    String s = "2011-" + (m < 10 ? "0" : "") + m +
                            "-" + (d < 10 ? "0" : "") + d + " " +
                            (h < 10 ? "0" : "") + h + ":00:00";
                    ValueTimestamp ts = ValueTimestamp.parse(s + "Z");
                    String s2 = ts.getString();
                    ValueTimestamp ts2 = ValueTimestamp.parse(s2);
                    assertEquals(ts.getString(), ts2.getString());
                }
            }
        }
    }

    @SuppressWarnings("unlikely-arg-type")
    private void testValueTimestamp() {
        assertEquals(
                "2001-02-03 04:05:06", ValueTimestamp.get(
                Timestamp.valueOf(
                "2001-02-03 04:05:06")).getString());
        assertEquals(
                "2001-02-03 04:05:06.001002003", ValueTimestamp.get(
                Timestamp.valueOf(
                "2001-02-03 04:05:06.001002003")).getString());
        assertEquals(
                "0-00-00 00:00:00", ValueTimestamp.fromDateValueAndNanos(0, 0).getString());
        assertEquals(
                "9999-12-31 23:59:59",
                ValueTimestamp.parse(
                "9999-12-31 23:59:59").getString());

        assertEquals(
                Integer.MAX_VALUE +
                "-12-31 01:02:03.04050607",
                ValueTimestamp.parse(Integer.MAX_VALUE +
                "-12-31 01:02:03.0405060708").getString());
        assertEquals(
                Integer.MIN_VALUE +
                "-12-31 01:02:03.04050607",
                ValueTimestamp.parse(Integer.MIN_VALUE +
                "-12-31 01:02:03.0405060708").getString());

        ValueTimestamp t1 = ValueTimestamp.parse("2001-01-01 01:01:01.111");
        assertEquals("2001-01-01 01:01:01.111", t1.getTimestamp().toString());
        assertEquals("2001-01-01", t1.getDate().toString());
        assertEquals("01:01:01", t1.getTime().toString());
        assertEquals("TIMESTAMP '2001-01-01 01:01:01.111'", t1.getSQL());
        assertEquals("TIMESTAMP '2001-01-01 01:01:01.111'", t1.toString());
        assertEquals(Value.TIMESTAMP, t1.getType());
        long dateValue = t1.getDateValue();
        long nanos = t1.getTimeNanos();
        assertEquals((int) ((dateValue >>> 32) ^ dateValue ^
                (nanos >>> 32) ^ nanos),
                t1.hashCode());
        // Literals return maximum precision
        assertEquals(ValueTimestamp.MAXIMUM_PRECISION, t1.getDisplaySize());
        assertEquals(ValueTimestamp.MAXIMUM_PRECISION, t1.getPrecision());
        assertEquals(9, t1.getScale());
        assertEquals("java.sql.Timestamp", t1.getObject().getClass().getName());
        ValueTimestamp t1b = ValueTimestamp.parse("2001-01-01 01:01:01.111");
        assertTrue(t1 == t1b);
        Value.clearCache();
        t1b = ValueTimestamp.parse("2001-01-01 01:01:01.111");
        assertFalse(t1 == t1b);
        assertTrue(t1.equals(t1));
        assertTrue(t1.equals(t1b));
        assertTrue(t1b.equals(t1));
        assertEquals(0, t1.compareTo(t1b, null));
        assertEquals(0, t1b.compareTo(t1, null));
        ValueTimestamp t2 = ValueTimestamp.parse("2002-02-02 02:02:02.222");
        assertFalse(t1.equals(t2));
        assertFalse(t2.equals(t1));
        assertEquals(-1, t1.compareTo(t2, null));
        assertEquals(1, t2.compareTo(t1, null));
        t1 = ValueTimestamp.parse("2001-01-01 01:01:01.123456789");
        assertEquals("2001-01-01 01:01:01.123456789",
                t1.getString());
        assertEquals("2001-01-01 01:01:01.123456789",
                t1.convertScale(true, 10).getString());
        assertEquals("2001-01-01 01:01:01.123456789",
                t1.convertScale(true, 9).getString());
        assertEquals("2001-01-01 01:01:01.12345679",
                t1.convertScale(true, 8).getString());
        assertEquals("2001-01-01 01:01:01.1234568",
                t1.convertScale(true, 7).getString());
        assertEquals("2001-01-01 01:01:01.123457",
                t1.convertScale(true, 6).getString());
        assertEquals("2001-01-01 01:01:01.12346",
                t1.convertScale(true, 5).getString());
        assertEquals("2001-01-01 01:01:01.1235",
                t1.convertScale(true, 4).getString());
        assertEquals("2001-01-01 01:01:01.123",
                t1.convertScale(true, 3).getString());
        assertEquals("2001-01-01 01:01:01.12",
                t1.convertScale(true, 2).getString());
        assertEquals("2001-01-01 01:01:01.1",
                t1.convertScale(true, 1).getString());
        assertEquals("2001-01-01 01:01:01",
                t1.convertScale(true, 0).getString());
        t1 = ValueTimestamp.parse("-2001-01-01 01:01:01.123456789");
        assertEquals("-2001-01-01 01:01:01.123457",
                t1.convertScale(true, 6).getString());
        // classes do not match
        assertFalse(ValueTimestamp.parse("2001-01-01").
                equals(ValueDate.parse("2001-01-01")));

        assertEquals("2001-01-01 01:01:01",
                ValueTimestamp.parse("2001-01-01").add(
                ValueTime.parse("01:01:01")).getString());
        assertEquals("1010-10-10 00:00:00",
                ValueTimestamp.parse("1010-10-10 10:10:10").subtract(
                ValueTime.parse("10:10:10")).getString());
        assertEquals("-2001-01-01 01:01:01",
                ValueTimestamp.parse("-2001-01-01").add(
                ValueTime.parse("01:01:01")).getString());
        assertEquals("-1010-10-10 00:00:00",
                ValueTimestamp.parse("-1010-10-10 10:10:10").subtract(
                ValueTime.parse("10:10:10")).getString());

        if (SysProperties.UNLIMITED_TIME_RANGE) {
            assertEquals("2001-01-02 01:01:01",
                    ValueTimestamp.parse("2001-01-01").add(
                    ValueTime.parse("25:01:01")).getString());
            assertEquals("1010-10-10 10:00:00",
                    ValueTimestamp.parse("1010-10-11 10:10:10").subtract(
                    ValueTime.parse("24:10:10")).getString());
        }

        assertEquals(0, DateTimeUtils.absoluteDayFromDateValue(
                ValueTimestamp.parse("1970-01-01").getDateValue()));
        assertEquals(0, ValueTimestamp.parse(
                "1970-01-01").getTimeNanos());
        assertEquals(0, ValueTimestamp.parse(
                "1970-01-01 00:00:00.000 UTC").getTimestamp().getTime());
        assertEquals(0, ValueTimestamp.parse(
                "+1970-01-01T00:00:00.000Z").getTimestamp().getTime());
        assertEquals(0, ValueTimestamp.parse(
                "1970-01-01T00:00:00.000+00:00").getTimestamp().getTime());
        assertEquals(0, ValueTimestamp.parse(
                "1970-01-01T00:00:00.000-00:00").getTimestamp().getTime());
        new AssertThrows(ErrorCode.INVALID_DATETIME_CONSTANT_2) {
            @Override
            public void test() {
                ValueTimestamp.parse("1970-01-01 00:00:00.000 ABC");
            }
        };
        new AssertThrows(ErrorCode.INVALID_DATETIME_CONSTANT_2) {
            @Override
            public void test() {
                ValueTimestamp.parse("1970-01-01T00:00:00.000+ABC");
            }
        };
    }

    private void testAbsoluteDay() {
        long next = Long.MIN_VALUE;
        for (int y = -2000; y < 3000; y++) {
            for (int m = -3; m <= 14; m++) {
                for (int d = -2; d <= 35; d++) {
                    if (!DateTimeUtils.isValidDate(y, m, d)) {
                        continue;
                    }
                    long date = DateTimeUtils.dateValue(y, m, d);
                    long abs = DateTimeUtils.absoluteDayFromDateValue(date);
                    if (abs != next && next != Long.MIN_VALUE) {
                        assertEquals(abs, next);
                    }
                    if (m == 1 && d == 1) {
                        assertEquals(abs, DateTimeUtils.absoluteDayFromYear(y));
                    }
                    next = abs + 1;
                    long d2 = DateTimeUtils.dateValueFromAbsoluteDay(abs);
                    assertEquals(date, d2);
                    assertEquals(y, DateTimeUtils.yearFromDateValue(date));
                    assertEquals(m, DateTimeUtils.monthFromDateValue(date));
                    assertEquals(d, DateTimeUtils.dayFromDateValue(date));
                    long nextDateValue = DateTimeUtils.dateValueFromAbsoluteDay(next);
                    assertEquals(nextDateValue, DateTimeUtils.incrementDateValue(date));
                    assertEquals(date, DateTimeUtils.decrementDateValue(nextDateValue));
                }
            }
        }
    }

    private void testValidDate() {
        Calendar c = DateTimeUtils.createGregorianCalendar(DateTimeUtils.UTC);
        c.setLenient(false);
        for (int y = -2000; y < 3000; y++) {
            for (int m = -3; m <= 14; m++) {
                for (int d = -2; d <= 35; d++) {
                    boolean valid = DateTimeUtils.isValidDate(y, m, d);
                    if (m < 1 || m > 12) {
                        assertFalse(valid);
                    } else if (d < 1 || d > 31) {
                        assertFalse(valid);
                    } else if (y != 1582 && d >= 1 && d <= 27) {
                        assertTrue(valid);
                    } else {
                        if (y <= 0) {
                            c.set(Calendar.ERA, GregorianCalendar.BC);
                            c.set(Calendar.YEAR, 1 - y);
                        } else {
                            c.set(Calendar.ERA, GregorianCalendar.AD);
                            c.set(Calendar.YEAR, y);
                        }
                        c.set(Calendar.MONTH, m - 1);
                        c.set(Calendar.DAY_OF_MONTH, d);
                        boolean expected = true;
                        try {
                            c.getTimeInMillis();
                        } catch (Exception e) {
                            expected = false;
                        }
                        if (expected != valid) {
                            fail(y + "-" + m + "-" + d +
                                    " expected: " + expected + " got: " + valid);
                        }
                    }
                }
            }
        }
    }

    private static void testCalculateLocalMillis() {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            for (TimeZone tz : TestDate.getDistinctTimeZones()) {
                TimeZone.setDefault(tz);
                for (int y = 1900; y < 2039; y += 10) {
                    if (y == 1993) {
                        // timezone change in Kwajalein
                    } else if (y == 1995) {
                        // timezone change in Enderbury and Kiritimati
                    }
                    for (int m = 1; m <= 12; m++) {
                        if (m != 3 && m != 4 && m != 10 && m != 11) {
                            // only test daylight saving time transitions
                            continue;
                        }
                        for (int day = 1; day < 29; day++) {
                            testDate(y, m, day);
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    private static void testDate(int y, int m, int day) {
        long millis = DateTimeUtils.getMillis(
                TimeZone.getDefault(), y, m, day, 0, 0, 0, 0);
        String st = new java.sql.Date(millis).toString();
        int y2 = Integer.parseInt(st.substring(0, 4));
        int m2 = Integer.parseInt(st.substring(5, 7));
        int d2 = Integer.parseInt(st.substring(8, 10));
        if (y != y2 || m != m2 || day != d2) {
            String s = y + "-" + (m < 10 ? "0" + m : m) +
                    "-" + (day < 10 ? "0" + day : day);
            System.out.println(s + "<>" + st + " " + TimeZone.getDefault().getID());
        }
    }

    /**
     * Get the list of timezones with distinct rules.
     *
     * @return the list
     */
    public static ArrayList<TimeZone> getDistinctTimeZones() {
        ArrayList<TimeZone> distinct = New.arrayList();
        for (String id : TimeZone.getAvailableIDs()) {
            TimeZone t = TimeZone.getTimeZone(id);
            for (TimeZone d : distinct) {
                if (t.hasSameRules(d)) {
                    t = null;
                    break;
                }
            }
            if (t != null) {
                distinct.add(t);
            }
        }
        return distinct;
    }

    private void testDateTimeUtils() {
        ValueTimestamp ts1 = ValueTimestamp.parse("-999-08-07 13:14:15.16");
        ValueTimestamp ts2 = ValueTimestamp.parse("19999-08-07 13:14:15.16");
        ValueTime t1 = (ValueTime) ts1.convertTo(Value.TIME);
        ValueTime t2 = (ValueTime) ts2.convertTo(Value.TIME);
        ValueDate d1 = (ValueDate) ts1.convertTo(Value.DATE);
        ValueDate d2 = (ValueDate) ts2.convertTo(Value.DATE);
        assertEquals("-999-08-07 13:14:15.16", ts1.getString());
        assertEquals("-999-08-07", d1.getString());
        assertEquals("13:14:15.16", t1.getString());
        assertEquals("19999-08-07 13:14:15.16", ts2.getString());
        assertEquals("19999-08-07", d2.getString());
        assertEquals("13:14:15.16", t2.getString());
        ValueTimestamp ts1a = DateTimeUtils.convertTimestamp(
                ts1.getTimestamp(), DateTimeUtils.createGregorianCalendar());
        ValueTimestamp ts2a = DateTimeUtils.convertTimestamp(
                ts2.getTimestamp(), DateTimeUtils.createGregorianCalendar());
        assertEquals("-999-08-07 13:14:15.16", ts1a.getString());
        assertEquals("19999-08-07 13:14:15.16", ts2a.getString());

        // test for bug on Java 1.8.0_60 in "Europe/Moscow" timezone.
        // Doesn't affect most other timezones
        long millis = 1407437460000L;
        long result1 = DateTimeUtils.nanosFromDate(DateTimeUtils.getTimeUTCWithoutDst(millis));
        long result2 = DateTimeUtils.nanosFromDate(DateTimeUtils.getTimeUTCWithoutDst(millis));
        assertEquals(result1, result2);
    }

}
