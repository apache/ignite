/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import static org.h2.util.DateTimeUtils.dateValue;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.h2.test.TestBase;
import org.h2.util.DateTimeUtils;
import org.h2.value.ValueTimestamp;

/**
 * Unit tests for the DateTimeUtils class
 */
public class TestDateTimeUtils extends TestBase {

    /**
     * Run just this test.
     *
     * @param a
     *            if {@code "testUtc2Value"} only {@link #testUTC2Value(boolean)}
     *            will be executed with all time zones (slow). Otherwise all tests
     *            in this test unit will be executed with local time zone.
     */
    public static void main(String... a) throws Exception {
        if (a.length == 1) {
            if ("testUtc2Value".equals(a[0])) {
                new TestDateTimeUtils().testUTC2Value(true);
                return;
            }
        }
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testParseTimeNanosDB2Format();
        testDayOfWeek();
        testWeekOfYear();
        testDateValueFromDenormalizedDate();
        testUTC2Value(false);
        testConvertScale();
    }

    private void testParseTimeNanosDB2Format() {
        assertEquals(3723004000000L, DateTimeUtils.parseTimeNanos("01:02:03.004", 0, 12, true));
        assertEquals(3723004000000L, DateTimeUtils.parseTimeNanos("01.02.03.004", 0, 12, true));

        assertEquals(3723000000000L, DateTimeUtils.parseTimeNanos("01:02:03", 0, 8, true));
        assertEquals(3723000000000L, DateTimeUtils.parseTimeNanos("01.02.03", 0, 8, true));
    }

    /**
     * Test for {@link DateTimeUtils#getSundayDayOfWeek(long)} and
     * {@link DateTimeUtils#getIsoDayOfWeek(long)}.
     */
    private void testDayOfWeek() {
        GregorianCalendar gc = DateTimeUtils.createGregorianCalendar(DateTimeUtils.UTC);
        for (int i = -1_000_000; i <= 1_000_000; i++) {
            gc.clear();
            gc.setTimeInMillis(i * 86400000L);
            int year = gc.get(Calendar.YEAR);
            if (gc.get(Calendar.ERA) == GregorianCalendar.BC) {
                year = 1 - year;
            }
            long expectedDateValue = dateValue(year, gc.get(Calendar.MONTH) + 1,
                    gc.get(Calendar.DAY_OF_MONTH));
            long dateValue = DateTimeUtils.dateValueFromAbsoluteDay(i);
            assertEquals(expectedDateValue, dateValue);
            assertEquals(i, DateTimeUtils.absoluteDayFromDateValue(dateValue));
            int dow = gc.get(Calendar.DAY_OF_WEEK);
            assertEquals(dow, DateTimeUtils.getSundayDayOfWeek(dateValue));
            int isoDow = (dow + 5) % 7 + 1;
            assertEquals(isoDow, DateTimeUtils.getIsoDayOfWeek(dateValue));
            assertEquals(gc.get(Calendar.WEEK_OF_YEAR),
                    DateTimeUtils.getWeekOfYear(dateValue, gc.getFirstDayOfWeek() - 1,
                    gc.getMinimalDaysInFirstWeek()));
        }
    }

    /**
     * Test for {@link DateTimeUtils#getDayOfYear(long)},
     * {@link DateTimeUtils#getWeekOfYear(long, int, int)} and
     * {@link DateTimeUtils#getWeekYear(long, int, int)}.
     */
    private void testWeekOfYear() {
        GregorianCalendar gc = new GregorianCalendar(DateTimeUtils.UTC);
        for (int firstDay = 1; firstDay <= 7; firstDay++) {
            gc.setFirstDayOfWeek(firstDay);
            for (int minimalDays = 1; minimalDays <= 7; minimalDays++) {
                gc.setMinimalDaysInFirstWeek(minimalDays);
                for (int i = 0; i < 150_000; i++) {
                    long dateValue = DateTimeUtils.dateValueFromAbsoluteDay(i);
                    gc.clear();
                    gc.setTimeInMillis(i * 86400000L);
                    assertEquals(gc.get(Calendar.DAY_OF_YEAR), DateTimeUtils.getDayOfYear(dateValue));
                    assertEquals(gc.get(Calendar.WEEK_OF_YEAR),
                            DateTimeUtils.getWeekOfYear(dateValue, firstDay - 1, minimalDays));
                    assertEquals(gc.getWeekYear(), DateTimeUtils.getWeekYear(dateValue, firstDay - 1, minimalDays));
                }
            }
        }
    }

    /**
     * Test for {@link DateTimeUtils#dateValueFromDenormalizedDate(long, long, int)}.
     */
    private void testDateValueFromDenormalizedDate() {
        assertEquals(dateValue(2017, 1, 1), DateTimeUtils.dateValueFromDenormalizedDate(2018, -11, 0));
        assertEquals(dateValue(2001, 2, 28), DateTimeUtils.dateValueFromDenormalizedDate(2000, 14, 29));
        assertEquals(dateValue(1999, 8, 1), DateTimeUtils.dateValueFromDenormalizedDate(2000, -4, -100));
        assertEquals(dateValue(2100, 12, 31), DateTimeUtils.dateValueFromDenormalizedDate(2100, 12, 2000));
        assertEquals(dateValue(-100, 2, 29), DateTimeUtils.dateValueFromDenormalizedDate(-100, 2, 30));
    }

    private void testUTC2Value(boolean allTimeZones) {
        TimeZone def = TimeZone.getDefault();
        GregorianCalendar gc = new GregorianCalendar();
        if (allTimeZones) {
            try {
                for (String id : TimeZone.getAvailableIDs()) {
                    System.out.println(id);
                    TimeZone tz = TimeZone.getTimeZone(id);
                    TimeZone.setDefault(tz);
                    DateTimeUtils.resetCalendar();
                    testUTC2ValueImpl(tz, gc);
                }
            } finally {
                TimeZone.setDefault(def);
                DateTimeUtils.resetCalendar();
            }
        } else {
            testUTC2ValueImpl(def, gc);
        }
    }

    private void testUTC2ValueImpl(TimeZone tz, GregorianCalendar gc) {
        gc.setTimeZone(tz);
        gc.set(Calendar.MILLISECOND, 0);
        long absoluteStart = DateTimeUtils.absoluteDayFromDateValue(DateTimeUtils.dateValue(1950, 01, 01));
        long absoluteEnd = DateTimeUtils.absoluteDayFromDateValue(DateTimeUtils.dateValue(2050, 01, 01));
        for (long i = absoluteStart; i < absoluteEnd; i++) {
            long dateValue = DateTimeUtils.dateValueFromAbsoluteDay(i);
            int year = DateTimeUtils.yearFromDateValue(dateValue);
            int month = DateTimeUtils.monthFromDateValue(dateValue);
            int day = DateTimeUtils.dayFromDateValue(dateValue);
            for (int j = 0; j < 48; j++) {
                gc.set(year, month - 1, day, j / 2, (j & 1) * 30, 0);
                long timeMillis = gc.getTimeInMillis();
                ValueTimestamp ts = DateTimeUtils.convertTimestamp(new Timestamp(timeMillis), gc);
                assertEquals(ts.getDateValue(), DateTimeUtils.dateValueFromDate(timeMillis));
                assertEquals(ts.getTimeNanos(), DateTimeUtils.nanosFromDate(timeMillis));
            }
        }
    }

    private void testConvertScale() {
        assertEquals(555_555_555_555L, DateTimeUtils.convertScale(555_555_555_555L, 9));
        assertEquals(555_555_555_550L, DateTimeUtils.convertScale(555_555_555_554L, 8));
        assertEquals(555_555_555_500L, DateTimeUtils.convertScale(555_555_555_549L, 7));
        assertEquals(555_555_555_000L, DateTimeUtils.convertScale(555_555_555_499L, 6));
        assertEquals(555_555_550_000L, DateTimeUtils.convertScale(555_555_554_999L, 5));
        assertEquals(555_555_500_000L, DateTimeUtils.convertScale(555_555_549_999L, 4));
        assertEquals(555_555_000_000L, DateTimeUtils.convertScale(555_555_499_999L, 3));
        assertEquals(555_550_000_000L, DateTimeUtils.convertScale(555_554_999_999L, 2));
        assertEquals(555_500_000_000L, DateTimeUtils.convertScale(555_549_999_999L, 1));
        assertEquals(555_000_000_000L, DateTimeUtils.convertScale(555_499_999_999L, 0));
        assertEquals(555_555_555_555L, DateTimeUtils.convertScale(555_555_555_555L, 9));
        assertEquals(555_555_555_560L, DateTimeUtils.convertScale(555_555_555_555L, 8));
        assertEquals(555_555_555_600L, DateTimeUtils.convertScale(555_555_555_550L, 7));
        assertEquals(555_555_556_000L, DateTimeUtils.convertScale(555_555_555_500L, 6));
        assertEquals(555_555_560_000L, DateTimeUtils.convertScale(555_555_555_000L, 5));
        assertEquals(555_555_600_000L, DateTimeUtils.convertScale(555_555_550_000L, 4));
        assertEquals(555_556_000_000L, DateTimeUtils.convertScale(555_555_500_000L, 3));
        assertEquals(555_560_000_000L, DateTimeUtils.convertScale(555_555_000_000L, 2));
        assertEquals(555_600_000_000L, DateTimeUtils.convertScale(555_550_000_000L, 1));
        assertEquals(556_000_000_000L, DateTimeUtils.convertScale(555_500_000_000L, 0));
        assertEquals(100_999_999_999L, DateTimeUtils.convertScale(100_999_999_999L, 9));
        assertEquals(101_000_000_000L, DateTimeUtils.convertScale(100_999_999_999L, 8));
        assertEquals(101_000_000_000L, DateTimeUtils.convertScale(100_999_999_999L, 7));
        assertEquals(101_000_000_000L, DateTimeUtils.convertScale(100_999_999_999L, 6));
        assertEquals(101_000_000_000L, DateTimeUtils.convertScale(100_999_999_999L, 5));
        assertEquals(101_000_000_000L, DateTimeUtils.convertScale(100_999_999_999L, 4));
        assertEquals(101_000_000_000L, DateTimeUtils.convertScale(100_999_999_999L, 3));
        assertEquals(101_000_000_000L, DateTimeUtils.convertScale(100_999_999_999L, 2));
        assertEquals(101_000_000_000L, DateTimeUtils.convertScale(100_999_999_999L, 1));
        assertEquals(101_000_000_000L, DateTimeUtils.convertScale(100_999_999_999L, 0));
    }

}
