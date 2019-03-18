/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group Iso8601: Initial Developer: Robert Rathsack (firstName dot lastName at
 * gmx dot de)
 */
package org.h2.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.h2.engine.Mode;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueNull;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

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

    private static final int SHIFT_YEAR = 9;
    private static final int SHIFT_MONTH = 5;

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
     * Multipliers for {@link #convertScale(long, int)}.
     */
    private static final int[] CONVERT_SCALE_TABLE = { 1_000_000_000, 100_000_000,
            10_000_000, 1_000_000, 100_000, 10_000, 1_000, 100, 10 };

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
    private static TimeZone getTimeZone() {
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
     * Convert the date to the specified time zone.
     *
     * @param value the date (might be ValueNull)
     * @param calendar the calendar
     * @return the date using the correct time zone
     */
    public static Date convertDate(Value value, Calendar calendar) {
        if (value == ValueNull.INSTANCE) {
            return null;
        }
        ValueDate d = (ValueDate) value.convertTo(Value.DATE);
        Calendar cal = (Calendar) calendar.clone();
        cal.clear();
        cal.setLenient(true);
        long dateValue = d.getDateValue();
        long ms = convertToMillis(cal, yearFromDateValue(dateValue),
                monthFromDateValue(dateValue), dayFromDateValue(dateValue), 0,
                0, 0, 0);
        return new Date(ms);
    }

    /**
     * Convert the time to the specified time zone.
     *
     * @param value the time (might be ValueNull)
     * @param calendar the calendar
     * @return the time using the correct time zone
     */
    public static Time convertTime(Value value, Calendar calendar) {
        if (value == ValueNull.INSTANCE) {
            return null;
        }
        ValueTime t = (ValueTime) value.convertTo(Value.TIME);
        Calendar cal = (Calendar) calendar.clone();
        cal.clear();
        cal.setLenient(true);
        long nanos = t.getNanos();
        long millis = nanos / 1_000_000;
        nanos -= millis * 1_000_000;
        long s = millis / 1_000;
        millis -= s * 1_000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        return new Time(convertToMillis(cal, 1970, 1, 1, (int) h, (int) m, (int) s, (int) millis));
    }

    /**
     * Convert the timestamp to the specified time zone.
     *
     * @param value the timestamp (might be ValueNull)
     * @param calendar the calendar
     * @return the timestamp using the correct time zone
     */
    public static Timestamp convertTimestamp(Value value, Calendar calendar) {
        if (value == ValueNull.INSTANCE) {
            return null;
        }
        ValueTimestamp ts = (ValueTimestamp) value.convertTo(Value.TIMESTAMP);
        Calendar cal = (Calendar) calendar.clone();
        cal.clear();
        cal.setLenient(true);
        long dateValue = ts.getDateValue();
        long nanos = ts.getTimeNanos();
        long millis = nanos / 1_000_000;
        nanos -= millis * 1_000_000;
        long s = millis / 1_000;
        millis -= s * 1_000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        long ms = convertToMillis(cal, yearFromDateValue(dateValue),
                monthFromDateValue(dateValue), dayFromDateValue(dateValue),
                (int) h, (int) m, (int) s, (int) millis);
        Timestamp x = new Timestamp(ms);
        x.setNanos((int) (nanos + millis * 1_000_000));
        return x;
    }

    /**
     * Convert a java.util.Date using the specified calendar.
     *
     * @param x the date
     * @param calendar the calendar
     * @return the date
     */
    public static ValueDate convertDate(Date x, Calendar calendar) {
        if (calendar == null) {
            throw DbException.getInvalidValueException("calendar", null);
        }
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(x.getTime());
        long dateValue = dateValueFromCalendar(cal);
        return ValueDate.fromDateValue(dateValue);
    }

    /**
     * Convert the time using the specified calendar.
     *
     * @param x the time
     * @param calendar the calendar
     * @return the time
     */
    public static ValueTime convertTime(Time x, Calendar calendar) {
        if (calendar == null) {
            throw DbException.getInvalidValueException("calendar", null);
        }
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(x.getTime());
        long nanos = nanosFromCalendar(cal);
        return ValueTime.fromNanos(nanos);
    }

    /**
     * Convert the timestamp using the specified calendar.
     *
     * @param x the time
     * @param calendar the calendar
     * @return the timestamp
     */
    public static ValueTimestamp convertTimestamp(Timestamp x,
            Calendar calendar) {
        if (calendar == null) {
            throw DbException.getInvalidValueException("calendar", null);
        }
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(x.getTime());
        long dateValue = dateValueFromCalendar(cal);
        long nanos = nanosFromCalendar(cal);
        nanos += x.getNanos() % 1_000_000;
        return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
    }

    /**
     * Parse a date string. The format is: [+|-]year-month-day
     *
     * @param s the string to parse
     * @param start the parse index start
     * @param end the parse index end
     * @return the date value
     * @throws IllegalArgumentException if there is a problem
     */
    public static long parseDateValue(String s, int start, int end) {
        if (s.charAt(start) == '+') {
            // +year
            start++;
        }
        // start at position 1 to support "-year"
        int s1 = s.indexOf('-', start + 1);
        int s2 = s.indexOf('-', s1 + 1);
        if (s1 <= 0 || s2 <= s1) {
            throw new IllegalArgumentException(s);
        }
        int year = Integer.parseInt(s.substring(start, s1));
        int month = Integer.parseInt(s.substring(s1 + 1, s2));
        int day = Integer.parseInt(s.substring(s2 + 1, end));
        if (!isValidDate(year, month, day)) {
            throw new IllegalArgumentException(year + "-" + month + "-" + day);
        }
        return dateValue(year, month, day);
    }

    /**
     * Parse a time string. The format is: [-]hour:minute:second[.nanos] or
     * alternatively [-]hour.minute.second[.nanos].
     *
     * @param s the string to parse
     * @param start the parse index start
     * @param end the parse index end
     * @param timeOfDay whether the result need to be within 0 (inclusive) and 1
     *            day (exclusive)
     * @return the time in nanoseconds
     * @throws IllegalArgumentException if there is a problem
     */
    public static long parseTimeNanos(String s, int start, int end,
            boolean timeOfDay) {
        int hour = 0, minute = 0, second = 0;
        long nanos = 0;
        int s1 = s.indexOf(':', start);
        int s2 = s.indexOf(':', s1 + 1);
        int s3 = s.indexOf('.', s2 + 1);
        if (s1 <= 0 || s2 <= s1) {
            // if first try fails try to use IBM DB2 time format
            // [-]hour.minute.second[.nanos]
            s1 = s.indexOf('.', start);
            s2 = s.indexOf('.', s1 + 1);
            s3 = s.indexOf('.', s2 + 1);

            if (s1 <= 0 || s2 <= s1) {
                throw new IllegalArgumentException(s);
            }
        }
        boolean negative;
        hour = Integer.parseInt(s.substring(start, s1));
        if (hour < 0 || hour == 0 && s.charAt(0) == '-') {
            if (timeOfDay) {
                /*
                 * This also forbids -00:00:00 and similar values.
                 */
                throw new IllegalArgumentException(s);
            }
            negative = true;
            hour = -hour;
        } else {
            negative = false;
        }
        minute = Integer.parseInt(s.substring(s1 + 1, s2));
        if (s3 < 0) {
            second = Integer.parseInt(s.substring(s2 + 1, end));
        } else {
            second = Integer.parseInt(s.substring(s2 + 1, s3));
            String n = (s.substring(s3 + 1, end) + "000000000").substring(0, 9);
            nanos = Integer.parseInt(n);
        }
        if (hour >= 2_000_000 || minute < 0 || minute >= 60 || second < 0
                || second >= 60) {
            throw new IllegalArgumentException(s);
        }
        if (timeOfDay && hour >= 24) {
            throw new IllegalArgumentException(s);
        }
        nanos += ((((hour * 60L) + minute) * 60) + second) * 1_000_000_000;
        return negative ? -nanos : nanos;
    }

    /**
     * See:
     * https://stackoverflow.com/questions/3976616/how-to-find-nth-occurrence-of-character-in-a-string#answer-3976656
     */
    private static int findNthIndexOf(String str, char chr, int n) {
        int pos = str.indexOf(chr);
        while (--n > 0 && pos != -1) {
            pos = str.indexOf(chr, pos + 1);
        }
        return pos;
    }

    /**
     * Parses timestamp value from the specified string.
     *
     * @param s
     *            string to parse
     * @param mode
     *            database mode, or {@code null}
     * @param withTimeZone
     *            if {@code true} return {@link ValueTimestampTimeZone} instead of
     *            {@link ValueTimestamp}
     * @return parsed timestamp
     */
    public static Value parseTimestamp(String s, Mode mode, boolean withTimeZone) {
        int dateEnd = s.indexOf(' ');
        if (dateEnd < 0) {
            // ISO 8601 compatibility
            dateEnd = s.indexOf('T');
            if (dateEnd < 0 && mode != null && mode.allowDB2TimestampFormat) {
                // DB2 also allows dash between date and time
                dateEnd = findNthIndexOf(s, '-', 3);
            }
        }
        int timeStart;
        if (dateEnd < 0) {
            dateEnd = s.length();
            timeStart = -1;
        } else {
            timeStart = dateEnd + 1;
        }
        long dateValue = parseDateValue(s, 0, dateEnd);
        long nanos;
        short tzMinutes = 0;
        if (timeStart < 0) {
            nanos = 0;
        } else {
            int timeEnd = s.length();
            TimeZone tz = null;
            if (s.endsWith("Z")) {
                tz = UTC;
                timeEnd--;
            } else {
                int timeZoneStart = s.indexOf('+', dateEnd + 1);
                if (timeZoneStart < 0) {
                    timeZoneStart = s.indexOf('-', dateEnd + 1);
                }
                if (timeZoneStart >= 0) {
                    // Allow [timeZoneName] part after time zone offset
                    int offsetEnd = s.indexOf('[', timeZoneStart + 1);
                    if (offsetEnd < 0) {
                        offsetEnd = s.length();
                    }
                    String tzName = "GMT" + s.substring(timeZoneStart, offsetEnd);
                    tz = TimeZone.getTimeZone(tzName);
                    if (!tz.getID().startsWith(tzName)) {
                        throw new IllegalArgumentException(
                                tzName + " (" + tz.getID() + "?)");
                    }
                    if (s.charAt(timeZoneStart - 1) == ' ') {
                        timeZoneStart--;
                    }
                    timeEnd = timeZoneStart;
                } else {
                    timeZoneStart = s.indexOf(' ', dateEnd + 1);
                    if (timeZoneStart > 0) {
                        String tzName = s.substring(timeZoneStart + 1);
                        tz = TimeZone.getTimeZone(tzName);
                        if (!tz.getID().startsWith(tzName)) {
                            throw new IllegalArgumentException(tzName);
                        }
                        timeEnd = timeZoneStart;
                    }
                }
            }
            nanos = parseTimeNanos(s, dateEnd + 1, timeEnd, true);
            if (tz != null) {
                if (withTimeZone) {
                    if (tz != UTC) {
                        long millis = convertDateTimeValueToMillis(tz, dateValue, nanos / 1_000_000);
                        tzMinutes = (short) (tz.getOffset(millis) / 1000 / 60);
                    }
                } else {
                    long millis = convertDateTimeValueToMillis(tz, dateValue, nanos / 1_000_000);
                    dateValue = dateValueFromDate(millis);
                    nanos = nanos % 1_000_000 + nanosFromDate(millis);
                }
            }
        }
        if (withTimeZone) {
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, nanos, tzMinutes);
        }
        return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
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
     * Extracts date value and nanos of day from the specified value.
     *
     * @param value
     *            value to extract fields from
     * @return array with date value and nanos of day
     */
    public static long[] dateAndTimeFromValue(Value value) {
        long dateValue = EPOCH_DATE_VALUE;
        long timeNanos = 0;
        if (value instanceof ValueTimestamp) {
            ValueTimestamp v = (ValueTimestamp) value;
            dateValue = v.getDateValue();
            timeNanos = v.getTimeNanos();
        } else if (value instanceof ValueDate) {
            dateValue = ((ValueDate) value).getDateValue();
        } else if (value instanceof ValueTime) {
            timeNanos = ((ValueTime) value).getNanos();
        } else if (value instanceof ValueTimestampTimeZone) {
            ValueTimestampTimeZone v = (ValueTimestampTimeZone) value;
            dateValue = v.getDateValue();
            timeNanos = v.getTimeNanos();
        } else {
            ValueTimestamp v = (ValueTimestamp) value.convertTo(Value.TIMESTAMP);
            dateValue = v.getDateValue();
            timeNanos = v.getTimeNanos();
        }
        return new long[] {dateValue, timeNanos};
    }

    /**
     * Creates a new date-time value with the same type as original value. If
     * original value is a ValueTimestampTimeZone, returned value will have the same
     * time zone offset as original value.
     *
     * @param original
     *            original value
     * @param dateValue
     *            date value for the returned value
     * @param timeNanos
     *            nanos of day for the returned value
     * @param forceTimestamp
     *            if {@code true} return ValueTimestamp if original argument is
     *            ValueDate or ValueTime
     * @return new value with specified date value and nanos of day
     */
    public static Value dateTimeToValue(Value original, long dateValue, long timeNanos, boolean forceTimestamp) {
        if (!(original instanceof ValueTimestamp)) {
            if (!forceTimestamp) {
                if (original instanceof ValueDate) {
                    return ValueDate.fromDateValue(dateValue);
                }
                if (original instanceof ValueTime) {
                    return ValueTime.fromNanos(timeNanos);
                }
            }
            if (original instanceof ValueTimestampTimeZone) {
                return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos,
                        ((ValueTimestampTimeZone) original).getTimeZoneOffsetMins());
            }
        }
        return ValueTimestamp.fromDateValueAndNanos(dateValue, timeNanos);
    }

    /**
     * Get the number of milliseconds since 1970-01-01 in the local timezone,
     * but without daylight saving time into account.
     *
     * @param d the date
     * @return the milliseconds
     */
    public static long getTimeLocalWithoutDst(java.util.Date d) {
        return d.getTime() + zoneOffsetMillis;
    }

    /**
     * Convert the number of milliseconds since 1970-01-01 in the local timezone
     * to UTC, but without daylight saving time into account.
     *
     * @param millis the number of milliseconds in the local timezone
     * @return the number of milliseconds in UTC
     */
    public static long getTimeUTCWithoutDst(long millis) {
        return millis - zoneOffsetMillis;
    }

    /**
     * Returns day of week.
     *
     * @param dateValue
     *            the date value
     * @param firstDayOfWeek
     *            first day of week, Monday as 1, Sunday as 7 or 0
     * @return day of week
     * @see #getIsoDayOfWeek(long)
     */
    public static int getDayOfWeek(long dateValue, int firstDayOfWeek) {
        return getDayOfWeekFromAbsolute(absoluteDayFromDateValue(dateValue), firstDayOfWeek);
    }

    /**
     * Get the day of the week from the absolute day value.
     *
     * @param absoluteValue the absolute day
     * @param firstDayOfWeek the first day of the week
     * @return the day of week
     */
    public static int getDayOfWeekFromAbsolute(long absoluteValue, int firstDayOfWeek) {
        return absoluteValue >= 0 ? (int) ((absoluteValue - firstDayOfWeek + 11) % 7) + 1
                : (int) ((absoluteValue - firstDayOfWeek - 2) % 7) + 7;
    }

    /**
     * Returns number of day in year.
     *
     * @param dateValue
     *            the date value
     * @return number of day in year
     */
    public static int getDayOfYear(long dateValue) {
        return (int) (absoluteDayFromDateValue(dateValue) - absoluteDayFromYear(yearFromDateValue(dateValue))) + 1;
    }

    /**
     * Returns ISO day of week.
     *
     * @param dateValue
     *            the date value
     * @return ISO day of week, Monday as 1 to Sunday as 7
     * @see #getSundayDayOfWeek(long)
     */
    public static int getIsoDayOfWeek(long dateValue) {
        return getDayOfWeek(dateValue, 1);
    }

    /**
     * Returns ISO number of week in year.
     *
     * @param dateValue
     *            the date value
     * @return number of week in year
     * @see #getIsoWeekYear(long)
     * @see #getWeekOfYear(long, int, int)
     */
    public static int getIsoWeekOfYear(long dateValue) {
        return getWeekOfYear(dateValue, 1, 4);
    }

    /**
     * Returns ISO week year.
     *
     * @param dateValue
     *            the date value
     * @return ISO week year
     * @see #getIsoWeekOfYear(long)
     * @see #getWeekYear(long, int, int)
     */
    public static int getIsoWeekYear(long dateValue) {
        return getWeekYear(dateValue, 1, 4);
    }

    /**
     * Returns day of week with Sunday as 1.
     *
     * @param dateValue
     *            the date value
     * @return day of week, Sunday as 1 to Monday as 7
     * @see #getIsoDayOfWeek(long)
     */
    public static int getSundayDayOfWeek(long dateValue) {
        return getDayOfWeek(dateValue, 0);
    }

    /**
     * Returns number of week in year.
     *
     * @param dateValue
     *            the date value
     * @param firstDayOfWeek
     *            first day of week, Monday as 1, Sunday as 7 or 0
     * @param minimalDaysInFirstWeek
     *            minimal days in first week of year
     * @return number of week in year
     * @see #getIsoWeekOfYear(long)
     */
    public static int getWeekOfYear(long dateValue, int firstDayOfWeek, int minimalDaysInFirstWeek) {
        long abs = absoluteDayFromDateValue(dateValue);
        int year = yearFromDateValue(dateValue);
        long base = getWeekOfYearBase(year, firstDayOfWeek, minimalDaysInFirstWeek);
        if (abs - base < 0) {
            base = getWeekOfYearBase(year - 1, firstDayOfWeek, minimalDaysInFirstWeek);
        } else if (monthFromDateValue(dateValue) == 12 && 24 + minimalDaysInFirstWeek < dayFromDateValue(dateValue)) {
            if (abs >= getWeekOfYearBase(year + 1, firstDayOfWeek, minimalDaysInFirstWeek)) {
                return 1;
            }
        }
        return (int) ((abs - base) / 7) + 1;
    }

    private static long getWeekOfYearBase(int year, int firstDayOfWeek, int minimalDaysInFirstWeek) {
        long first = absoluteDayFromYear(year);
        int daysInFirstWeek = 8 - getDayOfWeekFromAbsolute(first, firstDayOfWeek);
        long base = first + daysInFirstWeek;
        if (daysInFirstWeek >= minimalDaysInFirstWeek) {
            base -= 7;
        }
        return base;
    }

    /**
     * Returns week year.
     *
     * @param dateValue
     *            the date value
     * @param firstDayOfWeek
     *            first day of week, Monday as 1, Sunday as 7 or 0
     * @param minimalDaysInFirstWeek
     *            minimal days in first week of year
     * @return week year
     * @see #getIsoWeekYear(long)
     */
    public static int getWeekYear(long dateValue, int firstDayOfWeek, int minimalDaysInFirstWeek) {
        long abs = absoluteDayFromDateValue(dateValue);
        int year = yearFromDateValue(dateValue);
        long base = getWeekOfYearBase(year, firstDayOfWeek, minimalDaysInFirstWeek);
        if (abs - base < 0) {
            return year - 1;
        } else if (monthFromDateValue(dateValue) == 12 && 24 + minimalDaysInFirstWeek < dayFromDateValue(dateValue)) {
            if (abs >= getWeekOfYearBase(year + 1, firstDayOfWeek, minimalDaysInFirstWeek)) {
                return year + 1;
            }
        }
        return year;
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
     * Verify if the specified date is valid.
     *
     * @param year the year
     * @param month the month (January is 1)
     * @param day the day (1 is the first of the month)
     * @return true if it is valid
     */
    public static boolean isValidDate(int year, int month, int day) {
        if (month < 1 || month > 12 || day < 1) {
            return false;
        }
        if (year == 1582 && month == 10) {
            // special case: days 1582-10-05 .. 1582-10-14 don't exist
            return day < 5 || (day > 14 && day <= 31);
        }
        return day <= getDaysInMonth(year, month);
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
     * Get the date value from a given denormalized date with possible out of range
     * values of month and/or day. Used after addition or subtraction month or years
     * to (from) it to get a valid date.
     *
     * @param year
     *            the year
     * @param month
     *            the month, if out of range month and year will be normalized
     * @param day
     *            the day of the month, if out of range it will be saturated
     * @return the date value
     */
    public static long dateValueFromDenormalizedDate(long year, long month, int day) {
        long mm1 = month - 1;
        long yd = mm1 / 12;
        if (mm1 < 0 && yd * 12 != mm1) {
            yd--;
        }
        int y = (int) (year + yd);
        int m = (int) (month - yd * 12);
        if (day < 1) {
            day = 1;
        } else {
            int max = getDaysInMonth(y, m);
            if (day > max) {
                day = max;
            }
        }
        return dateValue(y, m, day);
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
     * Calculate the normalized timestamp.
     *
     * @param absoluteDay the absolute day
     * @param nanos the nanoseconds (may be negative or larger than one day)
     * @return the timestamp
     */
    public static ValueTimestamp normalizeTimestamp(long absoluteDay,
            long nanos) {
        if (nanos > NANOS_PER_DAY || nanos < 0) {
            long d;
            if (nanos > NANOS_PER_DAY) {
                d = nanos / NANOS_PER_DAY;
            } else {
                d = (nanos - NANOS_PER_DAY + 1) / NANOS_PER_DAY;
            }
            nanos -= d * NANOS_PER_DAY;
            absoluteDay += d;
        }
        return ValueTimestamp.fromDateValueAndNanos(
                dateValueFromAbsoluteDay(absoluteDay), nanos);
    }

    /**
     * Converts local date value and nanoseconds to timestamp with time zone.
     *
     * @param dateValue
     *            date value
     * @param timeNanos
     *            nanoseconds since midnight
     * @return timestamp with time zone
     */
    public static ValueTimestampTimeZone timestampTimeZoneFromLocalDateValueAndNanos(long dateValue, long timeNanos) {
        int timeZoneOffset = getTimeZoneOffsetMillis(null, dateValue, timeNanos);
        int offsetMins = timeZoneOffset / 60_000;
        int correction = timeZoneOffset % 60_000;
        if (correction != 0) {
            timeNanos -= correction;
            if (timeNanos < 0) {
                timeNanos += NANOS_PER_DAY;
                dateValue = decrementDateValue(dateValue);
            } else if (timeNanos >= NANOS_PER_DAY) {
                timeNanos -= NANOS_PER_DAY;
                dateValue = incrementDateValue(dateValue);
            }
        }
        return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos, (short) offsetMins);
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
     * Calculate the absolute day from an encoded date value in proleptic Gregorian
     * calendar.
     *
     * @param dateValue the date value
     * @return the absolute day in proleptic Gregorian calendar
     */
    public static long prolepticGregorianAbsoluteDayFromDateValue(long dateValue) {
        long y = yearFromDateValue(dateValue);
        int m = monthFromDateValue(dateValue);
        int d = dayFromDateValue(dateValue);
        if (m <= 2) {
            y--;
            m += 12;
        }
        long a = ((y * 1461L) >> 2) + DAYS_OFFSET[m - 3] + d - 719_484;
        if (y < 1900 || y > 2099) {
            // Slow mode
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

    /**
     * Return the next date value.
     *
     * @param dateValue
     *            the date value
     * @return the next date value
     */
    public static long incrementDateValue(long dateValue) {
        int year = yearFromDateValue(dateValue);
        if (year == 1582) {
            // Use slow way instead of rarely needed large custom code.
            return dateValueFromAbsoluteDay(absoluteDayFromDateValue(dateValue) + 1);
        }
        int day = dayFromDateValue(dateValue);
        if (day < 28) {
            return dateValue + 1;
        }
        int month = monthFromDateValue(dateValue);
        if (day < getDaysInMonth(year, month)) {
            return dateValue + 1;
        }
        if (month < 12) {
            month++;
        } else {
            month = 1;
            year++;
        }
        return dateValue(year, month, 1);
    }

    /**
     * Return the previous date value.
     *
     * @param dateValue
     *            the date value
     * @return the previous date value
     */
    public static long decrementDateValue(long dateValue) {
        int year = yearFromDateValue(dateValue);
        if (year == 1582) {
            // Use slow way instead of rarely needed large custom code.
            return dateValueFromAbsoluteDay(absoluteDayFromDateValue(dateValue) - 1);
        }
        if (dayFromDateValue(dateValue) > 1) {
            return dateValue - 1;
        }
        int month = monthFromDateValue(dateValue);
        if (month > 1) {
            month--;
        } else {
            month = 12;
            year--;
        }
        return dateValue(year, month, getDaysInMonth(year, month));
    }

    /**
     * Append a date to the string builder.
     *
     * @param buff the target string builder
     * @param dateValue the date value
     */
    public static void appendDate(StringBuilder buff, long dateValue) {
        int y = yearFromDateValue(dateValue);
        int m = monthFromDateValue(dateValue);
        int d = dayFromDateValue(dateValue);
        if (y > 0 && y < 10_000) {
            StringUtils.appendZeroPadded(buff, 4, y);
        } else {
            buff.append(y);
        }
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, d);
    }

    /**
     * Append a time to the string builder.
     *
     * @param buff the target string builder
     * @param nanos the time in nanoseconds
     */
    public static void appendTime(StringBuilder buff, long nanos) {
        if (nanos < 0) {
            buff.append('-');
            nanos = -nanos;
        }
        /*
         * nanos now either in range from 0 to Long.MAX_VALUE or equals to
         * Long.MIN_VALUE. We need to divide nanos by 1000000 with unsigned division to
         * get correct result. The simplest way to do this with such constraints is to
         * divide -nanos by -1000000.
         */
        long ms = -nanos / -1_000_000;
        nanos -= ms * 1_000_000;
        long s = ms / 1_000;
        ms -= s * 1_000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        StringUtils.appendZeroPadded(buff, 2, h);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, s);
        if (ms > 0 || nanos > 0) {
            buff.append('.');
            int start = buff.length();
            StringUtils.appendZeroPadded(buff, 3, ms);
            if (nanos > 0) {
                StringUtils.appendZeroPadded(buff, 6, nanos);
            }
            for (int i = buff.length() - 1; i > start; i--) {
                if (buff.charAt(i) != '0') {
                    break;
                }
                buff.deleteCharAt(i);
            }
        }
    }

    /**
     * Append a time zone to the string builder.
     *
     * @param buff the target string builder
     * @param tz the time zone in minutes
     */
    public static void appendTimeZone(StringBuilder buff, short tz) {
        if (tz < 0) {
            buff.append('-');
            tz = (short) -tz;
        } else {
            buff.append('+');
        }
        int hours = tz / 60;
        tz -= hours * 60;
        int mins = tz;
        StringUtils.appendZeroPadded(buff, 2, hours);
        if (mins != 0) {
            buff.append(':');
            StringUtils.appendZeroPadded(buff, 2, mins);
        }
    }

    /**
     * Formats timestamp with time zone as string.
     *
     * @param dateValue the year-month-day bit field
     * @param timeNanos nanoseconds since midnight
     * @param timeZoneOffsetMins the time zone offset in minutes
     * @return formatted string
     */
    public static String timestampTimeZoneToString(long dateValue, long timeNanos, short timeZoneOffsetMins) {
        StringBuilder buff = new StringBuilder(ValueTimestampTimeZone.MAXIMUM_PRECISION);
        appendDate(buff, dateValue);
        buff.append(' ');
        appendTime(buff, timeNanos);
        appendTimeZone(buff, timeZoneOffsetMins);
        return buff.toString();
    }

    /**
     * Generates time zone name for the specified offset in minutes.
     *
     * @param offsetMins
     *            offset in minutes
     * @return time zone name
     */
    public static String timeZoneNameFromOffsetMins(int offsetMins) {
        if (offsetMins == 0) {
            return "UTC";
        }
        StringBuilder b = new StringBuilder(9);
        b.append("GMT");
        if (offsetMins < 0) {
            b.append('-');
            offsetMins = -offsetMins;
        } else {
            b.append('+');
        }
        StringUtils.appendZeroPadded(b, 2, offsetMins / 60);
        b.append(':');
        StringUtils.appendZeroPadded(b, 2, offsetMins % 60);
        return b.toString();
    }

    /**
     * Converts scale of nanoseconds.
     *
     * @param nanosOfDay nanoseconds of day
     * @param scale fractional seconds precision
     * @return scaled value
     */
    public static long convertScale(long nanosOfDay, int scale) {
        if (scale >= 9) {
            return nanosOfDay;
        }
        int m = CONVERT_SCALE_TABLE[scale];
        long mod = nanosOfDay % m;
        if (mod >= m >>> 1) {
            nanosOfDay += m;
        }
        return nanosOfDay - mod;
    }

}
