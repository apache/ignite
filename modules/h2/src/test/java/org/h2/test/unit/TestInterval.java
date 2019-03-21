/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import static org.h2.util.DateTimeUtils.NANOS_PER_SECOND;

import org.h2.api.Interval;
import org.h2.test.TestBase;
import org.h2.util.StringUtils;

/**
 * Test cases for Interval.
 */
public class TestInterval extends TestBase {

    private static final long MAX = 999_999_999_999_999_999L;

    private static final long MIN = -999_999_999_999_999_999L;

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testOfYears();
        testOfMonths();
        testOfDays();
        testOfHours();
        testOfMinutes();
        testOfSeconds();
        testOfSeconds2();
        testOfNanos();
        testOfYearsMonths();
        testOfDaysHours();
        testOfDaysHoursMinutes();
        testOfDaysHoursMinutesSeconds();
        testOfHoursMinutes();
        testOfHoursMinutesSeconds();
        testOfMinutesSeconds();
    }

    private void testOfYears() {
        testOfYearsGood(0);
        testOfYearsGood(100);
        testOfYearsGood(-100);
        testOfYearsGood(MAX);
        testOfYearsGood(MIN);
        testOfYearsBad(MAX + 1);
        testOfYearsBad(MIN - 1);
        testOfYearsBad(Long.MAX_VALUE);
        testOfYearsBad(Long.MIN_VALUE);
    }

    private void testOfYearsGood(long years) {
        Interval i = Interval.ofYears(years);
        assertEquals(years, i.getYears());
        assertEquals("INTERVAL '" + years + "' YEAR", i.toString());
    }

    private void testOfYearsBad(long years) {
        try {
            Interval.ofYears(years);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfMonths() {
        testOfMonthsGood(0);
        testOfMonthsGood(100);
        testOfMonthsGood(-100);
        testOfMonthsGood(MAX);
        testOfMonthsGood(MIN);
        testOfMonthsBad(MAX + 1);
        testOfMonthsBad(MIN - 1);
        testOfMonthsBad(Long.MAX_VALUE);
        testOfMonthsBad(Long.MIN_VALUE);
    }

    private void testOfMonthsGood(long months) {
        Interval i = Interval.ofMonths(months);
        assertEquals(months, i.getMonths());
        assertEquals("INTERVAL '" + months + "' MONTH", i.toString());
    }

    private void testOfMonthsBad(long months) {
        try {
            Interval.ofMonths(months);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfDays() {
        testOfDaysGood(0);
        testOfDaysGood(100);
        testOfDaysGood(-100);
        testOfDaysGood(MAX);
        testOfDaysGood(MIN);
        testOfDaysBad(MAX + 1);
        testOfDaysBad(MIN - 1);
        testOfDaysBad(Long.MAX_VALUE);
        testOfDaysBad(Long.MIN_VALUE);
    }

    private void testOfDaysGood(long days) {
        Interval i = Interval.ofDays(days);
        assertEquals(days, i.getDays());
        assertEquals("INTERVAL '" + days + "' DAY", i.toString());
    }

    private void testOfDaysBad(long days) {
        try {
            Interval.ofDays(days);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfHours() {
        testOfHoursGood(0);
        testOfHoursGood(100);
        testOfHoursGood(-100);
        testOfHoursGood(MAX);
        testOfHoursGood(MIN);
        testOfHoursBad(MAX + 1);
        testOfHoursBad(MIN - 1);
        testOfHoursBad(Long.MAX_VALUE);
        testOfHoursBad(Long.MIN_VALUE);
    }

    private void testOfHoursGood(long hours) {
        Interval i = Interval.ofHours(hours);
        assertEquals(hours, i.getHours());
        assertEquals("INTERVAL '" + hours + "' HOUR", i.toString());
    }

    private void testOfHoursBad(long hours) {
        try {
            Interval.ofHours(hours);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfMinutes() {
        testOfMinutesGood(0);
        testOfMinutesGood(100);
        testOfMinutesGood(-100);
        testOfMinutesGood(MAX);
        testOfMinutesGood(MIN);
        testOfMinutesBad(MAX + 1);
        testOfMinutesBad(MIN - 1);
        testOfMinutesBad(Long.MAX_VALUE);
        testOfMinutesBad(Long.MIN_VALUE);
    }

    private void testOfMinutesGood(long minutes) {
        Interval i = Interval.ofMinutes(minutes);
        assertEquals(minutes, i.getMinutes());
        assertEquals("INTERVAL '" + minutes + "' MINUTE", i.toString());
    }

    private void testOfMinutesBad(long minutes) {
        try {
            Interval.ofMinutes(minutes);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfSeconds() {
        testOfSecondsGood(0);
        testOfSecondsGood(100);
        testOfSecondsGood(-100);
        testOfSecondsGood(MAX);
        testOfSecondsGood(MIN);
        testOfSecondsBad(MAX + 1);
        testOfSecondsBad(MIN - 1);
        testOfSecondsBad(Long.MAX_VALUE);
        testOfSecondsBad(Long.MIN_VALUE);
    }

    private void testOfSecondsGood(long seconds) {
        Interval i = Interval.ofSeconds(seconds);
        assertEquals(seconds, i.getSeconds());
        assertEquals("INTERVAL '" + seconds + "' SECOND", i.toString());
    }

    private void testOfSecondsBad(long seconds) {
        try {
            Interval.ofSeconds(seconds);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfSeconds2() {
        testOfSeconds2Good(0, 0);
        testOfSeconds2Good(0, -2);
        testOfSeconds2Good(100, 5);
        testOfSeconds2Good(-100, -1);
        testOfSeconds2Good(MAX, 999_999_999);
        testOfSeconds2Good(MIN, -999_999_999);
        testOfSeconds2Bad(0, 1_000_000_000);
        testOfSeconds2Bad(0, -1_000_000_000);
        testOfSeconds2Bad(MAX + 1, 0);
        testOfSeconds2Bad(MIN - 1, 0);
        testOfSeconds2Bad(Long.MAX_VALUE, 0);
        testOfSeconds2Bad(Long.MIN_VALUE, 0);
        testOfSeconds2Bad(0, Integer.MAX_VALUE);
        testOfSeconds2Bad(0, Integer.MIN_VALUE);
    }

    private void testOfSeconds2Good(long seconds, int nanos) {
        Interval i = Interval.ofSeconds(seconds, nanos);
        assertEquals(seconds, i.getSeconds());
        assertEquals(nanos, i.getNanosOfSecond());
        if (Math.abs(seconds) < 9_000_000_000L) {
            assertEquals(seconds * NANOS_PER_SECOND + nanos, i.getSecondsAndNanos());
        }
        StringBuilder b = new StringBuilder("INTERVAL '");
        if (seconds < 0 || nanos < 0) {
            b.append('-');
        }
        b.append(Math.abs(seconds));
        if (nanos != 0) {
            b.append('.');
            StringUtils.appendZeroPadded(b, 9, Math.abs(nanos));
            stripTrailingZeroes(b);
        }
        b.append("' SECOND");
        assertEquals(b.toString(), i.toString());
    }

    private void testOfSeconds2Bad(long seconds, int nanos) {
        try {
            Interval.ofSeconds(seconds, nanos);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfNanos() {
        testOfNanosGood(0);
        testOfNanosGood(100);
        testOfNanosGood(-100);
        testOfNanosGood(Long.MAX_VALUE);
        testOfNanosGood(Long.MIN_VALUE);
    }

    private void testOfNanosGood(long nanos) {
        Interval i = Interval.ofNanos(nanos);
        long seconds = nanos / NANOS_PER_SECOND;
        long nanosOfSecond = nanos % NANOS_PER_SECOND;
        assertEquals(seconds, i.getSeconds());
        assertEquals(nanosOfSecond, i.getNanosOfSecond());
        assertEquals(nanos, i.getSecondsAndNanos());
        StringBuilder b = new StringBuilder("INTERVAL '");
        if (nanos < 0) {
            b.append('-');
        }
        b.append(Math.abs(seconds));
        if (nanosOfSecond != 0) {
            b.append('.');
            StringUtils.appendZeroPadded(b, 9, Math.abs(nanosOfSecond));
            stripTrailingZeroes(b);
        }
        b.append("' SECOND");
        assertEquals(b.toString(), i.toString());
    }

    private void testOfYearsMonths() {
        testOfYearsMonthsGood(0, 0);
        testOfYearsMonthsGood(0, -2);
        testOfYearsMonthsGood(100, 5);
        testOfYearsMonthsGood(-100, -1);
        testOfYearsMonthsGood(MAX, 11);
        testOfYearsMonthsGood(MIN, -11);
        testOfYearsMonthsBad(0, 12);
        testOfYearsMonthsBad(0, -12);
        testOfYearsMonthsBad(MAX + 1, 0);
        testOfYearsMonthsBad(MIN - 1, 0);
        testOfYearsMonthsBad(Long.MAX_VALUE, 0);
        testOfYearsMonthsBad(Long.MIN_VALUE, 0);
        testOfYearsMonthsBad(0, Integer.MAX_VALUE);
        testOfYearsMonthsBad(0, Integer.MIN_VALUE);
    }

    private void testOfYearsMonthsGood(long years, int months) {
        Interval i = Interval.ofYearsMonths(years, months);
        assertEquals(years, i.getYears());
        assertEquals(months, i.getMonths());
        StringBuilder b = new StringBuilder("INTERVAL '");
        if (years < 0 || months < 0) {
            b.append('-');
        }
        b.append(Math.abs(years)).append('-').append(Math.abs(months)).append("' YEAR TO MONTH");
        assertEquals(b.toString(), i.toString());
    }

    private void testOfYearsMonthsBad(long years, int months) {
        try {
            Interval.ofYearsMonths(years, months);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfDaysHours() {
        testOfDaysHoursGood(0, 0);
        testOfDaysHoursGood(0, -2);
        testOfDaysHoursGood(100, 5);
        testOfDaysHoursGood(-100, -1);
        testOfDaysHoursGood(MAX, 23);
        testOfDaysHoursGood(MIN, -23);
        testOfDaysHoursBad(0, 24);
        testOfDaysHoursBad(0, -24);
        testOfDaysHoursBad(MAX + 1, 0);
        testOfDaysHoursBad(MIN - 1, 0);
        testOfDaysHoursBad(Long.MAX_VALUE, 0);
        testOfDaysHoursBad(Long.MIN_VALUE, 0);
        testOfDaysHoursBad(0, Integer.MAX_VALUE);
        testOfDaysHoursBad(0, Integer.MIN_VALUE);
    }

    private void testOfDaysHoursGood(long days, int hours) {
        Interval i = Interval.ofDaysHours(days, hours);
        assertEquals(days, i.getDays());
        assertEquals(hours, i.getHours());
        StringBuilder b = new StringBuilder("INTERVAL '");
        if (days < 0 || hours < 0) {
            b.append('-');
        }
        b.append(Math.abs(days)).append(' ');
        StringUtils.appendZeroPadded(b, 2, Math.abs(hours));
        b.append("' DAY TO HOUR");
        assertEquals(b.toString(), i.toString());
    }

    private void testOfDaysHoursBad(long days, int hours) {
        try {
            Interval.ofDaysHours(days, hours);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfDaysHoursMinutes() {
        testOfDaysHoursMinutesGood(0, 0, 0);
        testOfDaysHoursMinutesGood(0, -2, 0);
        testOfDaysHoursMinutesGood(0, 0, -2);
        testOfDaysHoursMinutesGood(100, 5, 3);
        testOfDaysHoursMinutesGood(-100, -1, -3);
        testOfDaysHoursMinutesGood(MAX, 23, 59);
        testOfDaysHoursMinutesGood(MIN, -23, -59);
        testOfDaysHoursMinutesBad(0, 24, 0);
        testOfDaysHoursMinutesBad(0, -24, 0);
        testOfDaysHoursMinutesBad(0, 0, 60);
        testOfDaysHoursMinutesBad(0, 0, -60);
        testOfDaysHoursMinutesBad(MAX + 1, 0, 0);
        testOfDaysHoursMinutesBad(MIN - 1, 0, 0);
        testOfDaysHoursMinutesBad(Long.MAX_VALUE, 0, 0);
        testOfDaysHoursMinutesBad(Long.MIN_VALUE, 0, 0);
        testOfDaysHoursMinutesBad(0, Integer.MAX_VALUE, 0);
        testOfDaysHoursMinutesBad(0, Integer.MIN_VALUE, 0);
        testOfDaysHoursMinutesBad(0, 0, Integer.MAX_VALUE);
        testOfDaysHoursMinutesBad(0, 0, Integer.MIN_VALUE);
    }

    private void testOfDaysHoursMinutesGood(long days, int hours, int minutes) {
        Interval i = Interval.ofDaysHoursMinutes(days, hours, minutes);
        assertEquals(days, i.getDays());
        assertEquals(hours, i.getHours());
        assertEquals(minutes, i.getMinutes());
        StringBuilder b = new StringBuilder("INTERVAL '");
        if (days < 0 || hours < 0 || minutes < 0) {
            b.append('-');
        }
        b.append(Math.abs(days)).append(' ');
        StringUtils.appendZeroPadded(b, 2, Math.abs(hours));
        b.append(':');
        StringUtils.appendZeroPadded(b, 2, Math.abs(minutes));
        b.append("' DAY TO MINUTE");
        assertEquals(b.toString(), i.toString());
    }

    private void testOfDaysHoursMinutesBad(long days, int hours, int minutes) {
        try {
            Interval.ofDaysHoursMinutes(days, hours, minutes);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfDaysHoursMinutesSeconds() {
        testOfDaysHoursMinutesSecondsGood(0, 0, 0, 0);
        testOfDaysHoursMinutesSecondsGood(0, -2, 0, 0);
        testOfDaysHoursMinutesSecondsGood(0, 0, -2, 0);
        testOfDaysHoursMinutesSecondsGood(0, 0, 0, -2);
        testOfDaysHoursMinutesSecondsGood(100, 5, 3, 4);
        testOfDaysHoursMinutesSecondsGood(-100, -1, -3, -4);
        testOfDaysHoursMinutesSecondsGood(MAX, 23, 59, 59);
        testOfDaysHoursMinutesSecondsGood(MIN, -23, -59, -59);
        testOfDaysHoursMinutesSecondsBad(0, 24, 0, 0);
        testOfDaysHoursMinutesSecondsBad(0, -24, 0, 0);
        testOfDaysHoursMinutesSecondsBad(0, 0, 60, 0);
        testOfDaysHoursMinutesSecondsBad(0, 0, -60, 0);
        testOfDaysHoursMinutesSecondsBad(0, 0, 0, 60);
        testOfDaysHoursMinutesSecondsBad(0, 0, 0, -60);
        testOfDaysHoursMinutesSecondsBad(MAX + 1, 0, 0, 0);
        testOfDaysHoursMinutesSecondsBad(MIN - 1, 0, 0, 0);
        testOfDaysHoursMinutesSecondsBad(Long.MAX_VALUE, 0, 0, 0);
        testOfDaysHoursMinutesSecondsBad(Long.MIN_VALUE, 0, 0, 0);
        testOfDaysHoursMinutesSecondsBad(0, Integer.MAX_VALUE, 0, 0);
        testOfDaysHoursMinutesSecondsBad(0, Integer.MIN_VALUE, 0, 0);
        testOfDaysHoursMinutesSecondsBad(0, 0, Integer.MAX_VALUE, 0);
        testOfDaysHoursMinutesSecondsBad(0, 0, Integer.MIN_VALUE, 0);
        testOfDaysHoursMinutesSecondsBad(0, 0, 0, Integer.MAX_VALUE);
        testOfDaysHoursMinutesSecondsBad(0, 0, 0, Integer.MIN_VALUE);
    }

    private void testOfDaysHoursMinutesSecondsGood(long days, int hours, int minutes, int seconds) {
        Interval i = Interval.ofDaysHoursMinutesSeconds(days, hours, minutes, seconds);
        assertEquals(days, i.getDays());
        assertEquals(hours, i.getHours());
        assertEquals(minutes, i.getMinutes());
        assertEquals(seconds, i.getSeconds());
        assertEquals(0, i.getNanosOfSecond());
        assertEquals(seconds * NANOS_PER_SECOND, i.getSecondsAndNanos());
        StringBuilder b = new StringBuilder("INTERVAL '");
        if (days < 0 || hours < 0 || minutes < 0 || seconds < 0) {
            b.append('-');
        }
        b.append(Math.abs(days)).append(' ');
        StringUtils.appendZeroPadded(b, 2, Math.abs(hours));
        b.append(':');
        StringUtils.appendZeroPadded(b, 2, Math.abs(minutes));
        b.append(':');
        StringUtils.appendZeroPadded(b, 2, Math.abs(seconds));
        b.append("' DAY TO SECOND");
        assertEquals(b.toString(), i.toString());
    }

    private void testOfDaysHoursMinutesSecondsBad(long days, int hours, int minutes, int seconds) {
        try {
            Interval.ofDaysHoursMinutesSeconds(days, hours, minutes, seconds);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfHoursMinutes() {
        testOfHoursMinutesGood(0, 0);
        testOfHoursMinutesGood(0, -2);
        testOfHoursMinutesGood(100, 5);
        testOfHoursMinutesGood(-100, -1);
        testOfHoursMinutesGood(MAX, 59);
        testOfHoursMinutesGood(MIN, -59);
        testOfHoursMinutesBad(0, 60);
        testOfHoursMinutesBad(0, -60);
        testOfHoursMinutesBad(MAX + 1, 0);
        testOfHoursMinutesBad(MIN - 1, 0);
        testOfHoursMinutesBad(Long.MAX_VALUE, 0);
        testOfHoursMinutesBad(Long.MIN_VALUE, 0);
        testOfHoursMinutesBad(0, Integer.MAX_VALUE);
        testOfHoursMinutesBad(0, Integer.MIN_VALUE);
    }

    private void testOfHoursMinutesGood(long hours, int minutes) {
        Interval i = Interval.ofHoursMinutes(hours, minutes);
        assertEquals(hours, i.getHours());
        assertEquals(minutes, i.getMinutes());
        StringBuilder b = new StringBuilder("INTERVAL '");
        if (hours < 0 || minutes < 0) {
            b.append('-');
        }
        b.append(Math.abs(hours)).append(':');
        StringUtils.appendZeroPadded(b, 2, Math.abs(minutes));
        b.append("' HOUR TO MINUTE");
        assertEquals(b.toString(), i.toString());
    }

    private void testOfHoursMinutesBad(long hours, int minutes) {
        try {
            Interval.ofHoursMinutes(hours, minutes);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfHoursMinutesSeconds() {
        testOfHoursMinutesSecondsGood(0, 0, 0);
        testOfHoursMinutesSecondsGood(0, -2, 0);
        testOfHoursMinutesSecondsGood(0, 0, -2);
        testOfHoursMinutesSecondsGood(100, 5, 3);
        testOfHoursMinutesSecondsGood(-100, -1, -3);
        testOfHoursMinutesSecondsGood(MAX, 59, 59);
        testOfHoursMinutesSecondsGood(MIN, -59, -59);
        testOfHoursMinutesSecondsBad(0, 60, 0);
        testOfHoursMinutesSecondsBad(0, -60, 0);
        testOfHoursMinutesSecondsBad(0, 0, 60);
        testOfHoursMinutesSecondsBad(0, 0, -60);
        testOfHoursMinutesSecondsBad(MAX + 1, 0, 0);
        testOfHoursMinutesSecondsBad(MIN - 1, 0, 0);
        testOfHoursMinutesSecondsBad(Long.MAX_VALUE, 0, 0);
        testOfHoursMinutesSecondsBad(Long.MIN_VALUE, 0, 0);
        testOfHoursMinutesSecondsBad(0, Integer.MAX_VALUE, 0);
        testOfHoursMinutesSecondsBad(0, Integer.MIN_VALUE, 0);
        testOfHoursMinutesSecondsBad(0, 0, Integer.MAX_VALUE);
        testOfHoursMinutesSecondsBad(0, 0, Integer.MIN_VALUE);
    }

    private void testOfHoursMinutesSecondsGood(long hours, int minutes, int seconds) {
        Interval i = Interval.ofHoursMinutesSeconds(hours, minutes, seconds);
        assertEquals(hours, i.getHours());
        assertEquals(minutes, i.getMinutes());
        assertEquals(seconds, i.getSeconds());
        assertEquals(0, i.getNanosOfSecond());
        assertEquals(seconds * NANOS_PER_SECOND, i.getSecondsAndNanos());
        StringBuilder b = new StringBuilder("INTERVAL '");
        if (hours < 0 || minutes < 0 || seconds < 0) {
            b.append('-');
        }
        b.append(Math.abs(hours)).append(':');
        StringUtils.appendZeroPadded(b, 2, Math.abs(minutes));
        b.append(':');
        StringUtils.appendZeroPadded(b, 2, Math.abs(seconds));
        b.append("' HOUR TO SECOND");
        assertEquals(b.toString(), i.toString());
    }

    private void testOfHoursMinutesSecondsBad(long hours, int minutes, int seconds) {
        try {
            Interval.ofHoursMinutesSeconds(hours, minutes, seconds);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void testOfMinutesSeconds() {
        testOfMinutesSecondsGood(0, 0);
        testOfMinutesSecondsGood(0, -2);
        testOfMinutesSecondsGood(100, 5);
        testOfMinutesSecondsGood(-100, -1);
        testOfMinutesSecondsGood(MAX, 59);
        testOfMinutesSecondsGood(MIN, -59);
        testOfMinutesSecondsBad(0, 60);
        testOfMinutesSecondsBad(0, -60);
        testOfMinutesSecondsBad(MAX + 1, 0);
        testOfMinutesSecondsBad(MIN - 1, 0);
        testOfMinutesSecondsBad(Long.MAX_VALUE, 0);
        testOfMinutesSecondsBad(Long.MIN_VALUE, 0);
        testOfMinutesSecondsBad(0, Integer.MAX_VALUE);
        testOfMinutesSecondsBad(0, Integer.MIN_VALUE);
    }

    private void testOfMinutesSecondsGood(long minutes, int seconds) {
        Interval i = Interval.ofMinutesSeconds(minutes, seconds);
        assertEquals(minutes, i.getMinutes());
        assertEquals(seconds, i.getSeconds());
        assertEquals(0, i.getNanosOfSecond());
        assertEquals(seconds * NANOS_PER_SECOND, i.getSecondsAndNanos());
        StringBuilder b = new StringBuilder("INTERVAL '");
        if (minutes < 0 || seconds < 0) {
            b.append('-');
        }
        b.append(Math.abs(minutes)).append(':');
        StringUtils.appendZeroPadded(b, 2, Math.abs(seconds));
        b.append("' MINUTE TO SECOND");
        assertEquals(b.toString(), i.toString());
    }

    private void testOfMinutesSecondsBad(long minutes, int seconds) {
        try {
            Interval.ofMinutesSeconds(minutes, seconds);
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private static void stripTrailingZeroes(StringBuilder b) {
        int i = b.length() - 1;
        if (b.charAt(i) == '0') {
            while (b.charAt(--i) == '0') {
                // do nothing
            }
            b.setLength(i + 1);
        }
    }

}
