/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group Iso8601: Initial Developer: Robert Rathsack (firstName dot lastName at
 * gmx dot de)
 */

package org.apache.ignite.internal.cache.query.index.sorted.inline.keys;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * This utility class contains time conversion functions.
 * <p>
 * Date value: a bit field with bits for the year, month, and day. Absolute day:
 * the day number (0 means 1970-01-01).
 */
public class DateTimeUtils {
    /**
     * The number of milliseconds per day.
     */
    public static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;

    /**
     * The number of seconds per day.
     */
    public static final long SECONDS_PER_DAY = 24 * 60 * 60;

    /**
     * UTC time zone.
     */
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /**
     * The number of nanoseconds per day.
     */
    public static final long NANOS_PER_DAY = MILLIS_PER_DAY * 1_000_000;

    public static final int SHIFT_YEAR = 9;

    public static final int SHIFT_MONTH = 5;

    public static final long MIN_DATE_VALUE = (-999_999_999L << SHIFT_YEAR) + (1 << SHIFT_MONTH) + 1;

    public static final long MAX_DATE_VALUE = (999_999_999L << SHIFT_YEAR) + (12 << SHIFT_MONTH) + 31;

    /**
     * Date value for 1970-01-01.
     */
    public static final int EPOCH_DATE_VALUE = (1970 << SHIFT_YEAR) + (1 << SHIFT_MONTH) + 1;

    private static final int[] NORMAL_DAYS_PER_MONTH = { 0, 31, 28, 31, 30, 31,
        30, 31, 31, 30, 31, 30, 31 };

    /**
     * Offsets of month within a year, starting with March, April,...
     */
    private static final int[] DAYS_OFFSET = { 0, 31, 61, 92, 122, 153, 184,
        214, 245, 275, 306, 337, 366 };

    /**
     * The thread local. Can not override initialValue because this would result
     * in an inner class, which would not be garbage collected in a web
     * container, and prevent the class loader of H2 from being garbage
     * collected. Using a ThreadLocal on a system class like Calendar does not
     * have that problem, and while it is still a small memory leak, it is not a
     * class loader memory leak.
     */
    private static final ThreadLocal<GregorianCalendar> CACHED_CALENDAR = new ThreadLocal<>();

    /**
     * A cached instance of Calendar used when a timezone is specified.
     */
    private static final ThreadLocal<GregorianCalendar> CACHED_CALENDAR_NON_DEFAULT_TIMEZONE =
        new ThreadLocal<>();

    /**
     * Cached local time zone.
     */
    private static volatile TimeZone timeZone;

    /**
     * Observed JVM behaviour is that if the timezone of the host computer is
     * changed while the JVM is running, the zone offset does not change but
     * keeps the initial value. So it is correct to measure this once and use
     * this value throughout the JVM's lifecycle. In any case, it is safer to
     * use a fixed value throughout the duration of the JVM's life, rather than
     * have this offset change, possibly midway through a long-running query.
     */
    private static int zoneOffsetMillis = createGregorianCalendar().get(Calendar.ZONE_OFFSET);

    private DateTimeUtils() {
        // utility class
    }

    /**
     * Returns local time zone.
     *
     * @return local time zone
     */
    public static TimeZone getTimeZone() {
        TimeZone tz = timeZone;
        if (tz == null) {
            timeZone = tz = TimeZone.getDefault();
        }
        return tz;
    }

    /**
     * Reset the cached calendar for default timezone, for example after
     * changing the default timezone.
     */
    public static void resetCalendar() {
        CACHED_CALENDAR.remove();
        timeZone = null;
        zoneOffsetMillis = createGregorianCalendar().get(Calendar.ZONE_OFFSET);
    }

    /**
     * Get a calendar for the default timezone.
     *
     * @return a calendar instance. A cached instance is returned where possible
     */
    public static GregorianCalendar getCalendar() {
        GregorianCalendar c = CACHED_CALENDAR.get();
        if (c == null) {
            c = createGregorianCalendar();
            CACHED_CALENDAR.set(c);
        }
        c.clear();
        return c;
    }

    /**
     * Get a calendar for the given timezone.
     *
     * @param tz timezone for the calendar, is never null
     * @return a calendar instance. A cached instance is returned where possible
     */
    private static GregorianCalendar getCalendar(TimeZone tz) {
        GregorianCalendar c = CACHED_CALENDAR_NON_DEFAULT_TIMEZONE.get();
        if (c == null || !c.getTimeZone().equals(tz)) {
            c = createGregorianCalendar(tz);
            CACHED_CALENDAR_NON_DEFAULT_TIMEZONE.set(c);
        }
        c.clear();
        return c;
    }

    /**
     * Creates a Gregorian calendar for the default timezone using the default
     * locale. Dates in H2 are represented in a Gregorian calendar. So this
     * method should be used instead of Calendar.getInstance() to ensure that
     * the Gregorian calendar is used for all date processing instead of a
     * default locale calendar that can be non-Gregorian in some locales.
     *
     * @return a new calendar instance.
     */
    public static GregorianCalendar createGregorianCalendar() {
        return new GregorianCalendar();
    }

    /**
     * Creates a Gregorian calendar for the given timezone using the default
     * locale. Dates in H2 are represented in a Gregorian calendar. So this
     * method should be used instead of Calendar.getInstance() to ensure that
     * the Gregorian calendar is used for all date processing instead of a
     * default locale calendar that can be non-Gregorian in some locales.
     *
     * @param tz timezone for the calendar, is never null
     * @return a new calendar instance.
     */
    public static GregorianCalendar createGregorianCalendar(TimeZone tz) {
        return new GregorianCalendar(tz);
    }

    /**
     * Calculates the time zone offset in minutes for the specified time zone, date
     * value, and nanoseconds since midnight.
     *
     * @param tz
     *            time zone, or {@code null} for default
     * @param dateValue
     *            date value
     * @param timeNanos
     *            nanoseconds since midnight
     * @return time zone offset in milliseconds
     */
    public static int getTimeZoneOffsetMillis(TimeZone tz, long dateValue, long timeNanos) {
        long msec = timeNanos / 1_000_000;
        long utc = convertDateTimeValueToMillis(tz, dateValue, msec);
        long local = absoluteDayFromDateValue(dateValue) * MILLIS_PER_DAY + msec;
        return (int) (local - utc);
    }

    /**
     * Calculates the milliseconds since epoch for the specified date value,
     * nanoseconds since midnight, and time zone offset.
     * @param dateValue
     *            date value
     * @param timeNanos
     *            nanoseconds since midnight
     * @param offsetMins
     *            time zone offset in minutes
     * @return milliseconds since epoch in UTC
     */
    public static long getMillis(long dateValue, long timeNanos, short offsetMins) {
        return absoluteDayFromDateValue(dateValue) * MILLIS_PER_DAY
            + timeNanos / 1_000_000 - offsetMins * 60_000;
    }

    /**
     * Calculate the milliseconds since 1970-01-01 (UTC) for the given date and
     * time (in the specified timezone).
     *
     * @param tz the timezone of the parameters, or null for the default
     *            timezone
     * @param year the absolute year (positive or negative)
     * @param month the month (1-12)
     * @param day the day (1-31)
     * @param hour the hour (0-23)
     * @param minute the minutes (0-59)
     * @param second the number of seconds (0-59)
     * @param millis the number of milliseconds
     * @return the number of milliseconds (UTC)
     */
    public static long getMillis(TimeZone tz, int year, int month, int day,
        int hour, int minute, int second, int millis) {
        GregorianCalendar c;
        if (tz == null) {
            c = getCalendar();
        } else {
            c = getCalendar(tz);
        }
        c.setLenient(false);
        try {
            return convertToMillis(c, year, month, day, hour, minute, second, millis);
        } catch (IllegalArgumentException e) {
            // special case: if the time simply doesn't exist because of
            // daylight saving time changes, use the lenient version
            String message = e.toString();
            if (message.indexOf("HOUR_OF_DAY") > 0) {
                if (hour < 0 || hour > 23) {
                    throw e;
                }
            } else if (message.indexOf("DAY_OF_MONTH") > 0) {
                int maxDay;
                if (month == 2) {
                    maxDay = c.isLeapYear(year) ? 29 : 28;
                } else {
                    maxDay = NORMAL_DAYS_PER_MONTH[month];
                }
                if (day < 1 || day > maxDay) {
                    throw e;
                }
                // DAY_OF_MONTH is thrown for years > 2037
                // using the timezone Brasilia and others,
                // for example for 2042-10-12 00:00:00.
                hour += 6;
            }
            c.setLenient(true);
            return convertToMillis(c, year, month, day, hour, minute, second, millis);
        }
    }

    private static long convertToMillis(Calendar cal, int year, int month, int day,
        int hour, int minute, int second, int millis) {
        if (year <= 0) {
            cal.set(Calendar.ERA, GregorianCalendar.BC);
            cal.set(Calendar.YEAR, 1 - year);
        } else {
            cal.set(Calendar.ERA, GregorianCalendar.AD);
            cal.set(Calendar.YEAR, year);
        }
        // january is 0
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minute);
        cal.set(Calendar.SECOND, second);
        cal.set(Calendar.MILLISECOND, millis);
        return cal.getTimeInMillis();
    }

    /**
     * Returns number of days in month.
     *
     * @param year the year
     * @param month the month
     * @return number of days in the specified month
     */
    public static int getDaysInMonth(int year, int month) {
        if (month != 2) {
            return NORMAL_DAYS_PER_MONTH[month];
        }
        // All leap years divisible by 4
        return (year & 3) == 0
            // All such years before 1582 are Julian and leap
            && (year < 1582
            // Otherwise check Gregorian conditions
            || year % 100 != 0 || year % 400 == 0)
            ? 29 : 28;
    }

    /**
     * Convert an encoded date value to a java.util.Date, using the default
     * timezone.
     *
     * @param dateValue the date value
     * @return the date
     */
    public static Date convertDateValueToDate(long dateValue) {
        long millis = getMillis(null, yearFromDateValue(dateValue),
            monthFromDateValue(dateValue), dayFromDateValue(dateValue), 0,
            0, 0, 0);
        return new Date(millis);
    }

    /**
     * Convert an encoded date-time value to millis, using the supplied timezone.
     *
     * @param tz the timezone
     * @param dateValue the date value
     * @param ms milliseconds of day
     * @return the date
     */
    public static long convertDateTimeValueToMillis(TimeZone tz, long dateValue, long ms) {
        long second = ms / 1000;
        ms -= second * 1000;
        int minute = (int) (second / 60);
        second -= minute * 60;
        int hour = minute / 60;
        minute -= hour * 60;
        return getMillis(tz, yearFromDateValue(dateValue), monthFromDateValue(dateValue), dayFromDateValue(dateValue),
            hour, minute, (int) second, (int) ms);
    }

    /**
     * Convert an encoded date value / time value to a timestamp, using the
     * default timezone.
     *
     * @param dateValue the date value
     * @param timeNanos the nanoseconds since midnight
     * @return the timestamp
     */
    public static Timestamp convertDateValueToTimestamp(long dateValue,
        long timeNanos) {
        Timestamp ts = new Timestamp(convertDateTimeValueToMillis(null, dateValue, timeNanos / 1_000_000));
        // This method expects the complete nanoseconds value including milliseconds
        ts.setNanos((int) (timeNanos % 1_000_000_000));
        return ts;
    }

    /**
     * Convert an encoded date value / time value to a timestamp using the specified
     * time zone offset.
     *
     * @param dateValue the date value
     * @param timeNanos the nanoseconds since midnight
     * @param offsetMins time zone offset in minutes
     * @return the timestamp
     */
    public static Timestamp convertTimestampTimeZoneToTimestamp(long dateValue, long timeNanos, short offsetMins) {
        Timestamp ts = new Timestamp(getMillis(dateValue, timeNanos, offsetMins));
        ts.setNanos((int) (timeNanos % 1_000_000_000));
        return ts;
    }

    /**
     * Convert a time value to a time, using the default timezone.
     *
     * @param nanosSinceMidnight the nanoseconds since midnight
     * @return the time
     */
    public static Time convertNanoToTime(long nanosSinceMidnight) {
        long millis = nanosSinceMidnight / 1_000_000;
        long s = millis / 1_000;
        millis -= s * 1_000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        long ms = getMillis(null, 1970, 1, 1, (int) (h % 24), (int) m, (int) s,
            (int) millis);
        return new Time(ms);
    }

    /**
     * Get the year from a date value.
     *
     * @param x the date value
     * @return the year
     */
    public static int yearFromDateValue(long x) {
        return (int) (x >>> SHIFT_YEAR);
    }

    /**
     * Get the month from a date value.
     *
     * @param x the date value
     * @return the month (1..12)
     */
    public static int monthFromDateValue(long x) {
        return (int) (x >>> SHIFT_MONTH) & 15;
    }

    /**
     * Get the day of month from a date value.
     *
     * @param x the date value
     * @return the day (1..31)
     */
    public static int dayFromDateValue(long x) {
        return (int) (x & 31);
    }

    /**
     * Get the date value from a given date.
     *
     * @param year the year
     * @param month the month (1..12)
     * @param day the day (1..31)
     * @return the date value
     */
    public static long dateValue(long year, int month, int day) {
        return (year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day;
    }

    /**
     * Convert a UTC datetime in millis to an encoded date in the default
     * timezone.
     *
     * @param ms the milliseconds
     * @return the date value
     */
    public static long dateValueFromDate(long ms) {
        ms += getTimeZone().getOffset(ms);
        long absoluteDay = ms / MILLIS_PER_DAY;
        // Round toward negative infinity
        if (ms < 0 && (absoluteDay * MILLIS_PER_DAY != ms)) {
            absoluteDay--;
        }
        return dateValueFromAbsoluteDay(absoluteDay);
    }

    /**
     * Calculate the encoded date value from a given calendar.
     *
     * @param cal the calendar
     * @return the date value
     */
    private static long dateValueFromCalendar(Calendar cal) {
        int year = cal.get(Calendar.YEAR);
        if (cal.get(Calendar.ERA) == GregorianCalendar.BC) {
            year = 1 - year;
        }
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        return ((long) year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day;
    }

    /**
     * Convert a time in milliseconds in UTC to the nanoseconds since midnight
     * (in the default timezone).
     *
     * @param ms the milliseconds
     * @return the nanoseconds
     */
    public static long nanosFromDate(long ms) {
        ms += getTimeZone().getOffset(ms);
        long absoluteDay = ms / MILLIS_PER_DAY;
        // Round toward negative infinity
        if (ms < 0 && (absoluteDay * MILLIS_PER_DAY != ms)) {
            absoluteDay--;
        }
        return (ms - absoluteDay * MILLIS_PER_DAY) * 1_000_000;
    }

    /**
     * Convert a java.util.Calendar to nanoseconds since midnight.
     *
     * @param cal the calendar
     * @return the nanoseconds
     */
    private static long nanosFromCalendar(Calendar cal) {
        int h = cal.get(Calendar.HOUR_OF_DAY);
        int m = cal.get(Calendar.MINUTE);
        int s = cal.get(Calendar.SECOND);
        int millis = cal.get(Calendar.MILLISECOND);
        return ((((((h * 60L) + m) * 60) + s) * 1000) + millis) * 1000000;
    }

    /**
     * Calculate the absolute day for a January, 1 of the specified year.
     *
     * @param year
     *            the year
     * @return the absolute day
     */
    public static long absoluteDayFromYear(long year) {
        year--;
        long a = ((year * 1461L) >> 2) - 719_177;
        if (year < 1582) {
            // Julian calendar
            a += 13;
        } else if (year < 1900 || year > 2099) {
            // Gregorian calendar (slow mode)
            a += (year / 400) - (year / 100) + 15;
        }
        return a;
    }

    /**
     * Calculate the absolute day from an encoded date value.
     *
     * @param dateValue the date value
     * @return the absolute day
     */
    public static long absoluteDayFromDateValue(long dateValue) {
        long y = yearFromDateValue(dateValue);
        int m = monthFromDateValue(dateValue);
        int d = dayFromDateValue(dateValue);
        if (m <= 2) {
            y--;
            m += 12;
        }
        long a = ((y * 1461L) >> 2) + DAYS_OFFSET[m - 3] + d - 719_484;
        if (y <= 1582 && ((y < 1582) || (m * 100 + d < 10_15))) {
            // Julian calendar (cutover at 1582-10-04 / 1582-10-15)
            a += 13;
        } else if (y < 1900 || y > 2099) {
            // Gregorian calendar (slow mode)
            a += (y / 400) - (y / 100) + 15;
        }
        return a;
    }

    /**
     * Calculate the encoded date value from an absolute day.
     *
     * @param absoluteDay the absolute day
     * @return the date value
     */
    public static long dateValueFromAbsoluteDay(long absoluteDay) {
        long d = absoluteDay + 719_468;
        long y100, offset;
        if (d > 578_040) {
            // Gregorian calendar
            long y400 = d / 146_097;
            d -= y400 * 146_097;
            y100 = d / 36_524;
            d -= y100 * 36_524;
            offset = y400 * 400 + y100 * 100;
        } else {
            // Julian calendar
            y100 = 0;
            d += 292_200_000_002L;
            offset = -800_000_000;
        }
        long y4 = d / 1461;
        d -= y4 * 1461;
        long y = d / 365;
        d -= y * 365;
        if (d == 0 && (y == 4 || y100 == 4)) {
            y--;
            d += 365;
        }
        y += offset + y4 * 4;
        // month of a day
        int m = ((int) d * 2 + 1) * 5 / 306;
        d -= DAYS_OFFSET[m] - 1;
        if (m >= 10) {
            y++;
            m -= 12;
        }
        return dateValue(y, m + 3, (int) d);
    }
}
