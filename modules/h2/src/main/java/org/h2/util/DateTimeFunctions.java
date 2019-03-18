/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import static org.h2.expression.Function.CENTURY;
import static org.h2.expression.Function.DAY_OF_MONTH;
import static org.h2.expression.Function.DAY_OF_WEEK;
import static org.h2.expression.Function.DAY_OF_YEAR;
import static org.h2.expression.Function.DECADE;
import static org.h2.expression.Function.EPOCH;
import static org.h2.expression.Function.HOUR;
import static org.h2.expression.Function.ISO_DAY_OF_WEEK;
import static org.h2.expression.Function.ISO_WEEK;
import static org.h2.expression.Function.ISO_YEAR;
import static org.h2.expression.Function.MICROSECOND;
import static org.h2.expression.Function.MILLENNIUM;
import static org.h2.expression.Function.MILLISECOND;
import static org.h2.expression.Function.MINUTE;
import static org.h2.expression.Function.MONTH;
import static org.h2.expression.Function.NANOSECOND;
import static org.h2.expression.Function.QUARTER;
import static org.h2.expression.Function.SECOND;
import static org.h2.expression.Function.TIMEZONE_HOUR;
import static org.h2.expression.Function.TIMEZONE_MINUTE;
import static org.h2.expression.Function.WEEK;
import static org.h2.expression.Function.YEAR;
import java.math.BigDecimal;
import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.TimeZone;
import org.h2.api.ErrorCode;
import org.h2.expression.Function;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueInt;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Date and time functions.
 */
public final class DateTimeFunctions {
    private static final HashMap<String, Integer> DATE_PART = new HashMap<>();

    /**
     * English names of months and week days.
     */
    private static volatile String[][] MONTHS_AND_WEEKS;

    static {
        // DATE_PART
        DATE_PART.put("SQL_TSI_YEAR", YEAR);
        DATE_PART.put("YEAR", YEAR);
        DATE_PART.put("YYYY", YEAR);
        DATE_PART.put("YY", YEAR);
        DATE_PART.put("SQL_TSI_MONTH", MONTH);
        DATE_PART.put("MONTH", MONTH);
        DATE_PART.put("MM", MONTH);
        DATE_PART.put("M", MONTH);
        DATE_PART.put("QUARTER", QUARTER);
        DATE_PART.put("SQL_TSI_WEEK", WEEK);
        DATE_PART.put("WW", WEEK);
        DATE_PART.put("WK", WEEK);
        DATE_PART.put("WEEK", WEEK);
        DATE_PART.put("ISO_WEEK", ISO_WEEK);
        DATE_PART.put("DAY", DAY_OF_MONTH);
        DATE_PART.put("DD", DAY_OF_MONTH);
        DATE_PART.put("D", DAY_OF_MONTH);
        DATE_PART.put("SQL_TSI_DAY", DAY_OF_MONTH);
        DATE_PART.put("DAY_OF_WEEK", DAY_OF_WEEK);
        DATE_PART.put("DAYOFWEEK", DAY_OF_WEEK);
        DATE_PART.put("DOW", DAY_OF_WEEK);
        DATE_PART.put("ISO_DAY_OF_WEEK", ISO_DAY_OF_WEEK);
        DATE_PART.put("DAYOFYEAR", DAY_OF_YEAR);
        DATE_PART.put("DAY_OF_YEAR", DAY_OF_YEAR);
        DATE_PART.put("DY", DAY_OF_YEAR);
        DATE_PART.put("DOY", DAY_OF_YEAR);
        DATE_PART.put("SQL_TSI_HOUR", HOUR);
        DATE_PART.put("HOUR", HOUR);
        DATE_PART.put("HH", HOUR);
        DATE_PART.put("SQL_TSI_MINUTE", MINUTE);
        DATE_PART.put("MINUTE", MINUTE);
        DATE_PART.put("MI", MINUTE);
        DATE_PART.put("N", MINUTE);
        DATE_PART.put("SQL_TSI_SECOND", SECOND);
        DATE_PART.put("SECOND", SECOND);
        DATE_PART.put("SS", SECOND);
        DATE_PART.put("S", SECOND);
        DATE_PART.put("MILLISECOND", MILLISECOND);
        DATE_PART.put("MILLISECONDS", MILLISECOND);
        DATE_PART.put("MS", MILLISECOND);
        DATE_PART.put("EPOCH", EPOCH);
        DATE_PART.put("MICROSECOND", MICROSECOND);
        DATE_PART.put("MICROSECONDS", MICROSECOND);
        DATE_PART.put("MCS", MICROSECOND);
        DATE_PART.put("NANOSECOND", NANOSECOND);
        DATE_PART.put("NS", NANOSECOND);
        DATE_PART.put("TIMEZONE_HOUR", TIMEZONE_HOUR);
        DATE_PART.put("TIMEZONE_MINUTE", TIMEZONE_MINUTE);
        DATE_PART.put("DECADE", DECADE);
        DATE_PART.put("CENTURY", CENTURY);
        DATE_PART.put("MILLENNIUM", MILLENNIUM);
    }

    /**
     * DATEADD function.
     *
     * @param part
     *            name of date-time part
     * @param count
     *            count to add
     * @param v
     *            value to add to
     * @return result
     */
    public static Value dateadd(String part, long count, Value v) {
        int field = getDatePart(part);
        if (field != MILLISECOND && field != MICROSECOND && field != NANOSECOND
                && (count > Integer.MAX_VALUE || count < Integer.MIN_VALUE)) {
            throw DbException.getInvalidValueException("DATEADD count", count);
        }
        boolean withDate = !(v instanceof ValueTime);
        boolean withTime = !(v instanceof ValueDate);
        boolean forceTimestamp = false;
        long[] a = DateTimeUtils.dateAndTimeFromValue(v);
        long dateValue = a[0];
        long timeNanos = a[1];
        switch (field) {
        case QUARTER:
            count *= 3;
            //$FALL-THROUGH$
        case YEAR:
        case MONTH: {
            if (!withDate) {
                throw DbException.getInvalidValueException("DATEADD time part", part);
            }
            long year = DateTimeUtils.yearFromDateValue(dateValue);
            long month = DateTimeUtils.monthFromDateValue(dateValue);
            int day = DateTimeUtils.dayFromDateValue(dateValue);
            if (field == YEAR) {
                year += count;
            } else {
                month += count;
            }
            dateValue = DateTimeUtils.dateValueFromDenormalizedDate(year, month, day);
            return DateTimeUtils.dateTimeToValue(v, dateValue, timeNanos, forceTimestamp);
        }
        case WEEK:
        case ISO_WEEK:
            count *= 7;
            //$FALL-THROUGH$
        case DAY_OF_WEEK:
        case ISO_DAY_OF_WEEK:
        case DAY_OF_MONTH:
        case DAY_OF_YEAR:
            if (!withDate) {
                throw DbException.getInvalidValueException("DATEADD time part", part);
            }
            dateValue = DateTimeUtils
                    .dateValueFromAbsoluteDay(DateTimeUtils.absoluteDayFromDateValue(dateValue) + count);
            return DateTimeUtils.dateTimeToValue(v, dateValue, timeNanos, forceTimestamp);
        case HOUR:
            count *= 3_600_000_000_000L;
            break;
        case MINUTE:
            count *= 60_000_000_000L;
            break;
        case SECOND:
        case EPOCH:
            count *= 1_000_000_000;
            break;
        case MILLISECOND:
            count *= 1_000_000;
            break;
        case MICROSECOND:
            count *= 1_000;
            break;
        case NANOSECOND:
            break;
        case TIMEZONE_HOUR:
            count *= 60;
            //$FALL-THROUGH$
        case TIMEZONE_MINUTE: {
            if (!(v instanceof ValueTimestampTimeZone)) {
                throw DbException.getUnsupportedException("DATEADD " + part);
            }
            count += ((ValueTimestampTimeZone) v).getTimeZoneOffsetMins();
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos, (short) count);
        }
        default:
            throw DbException.getUnsupportedException("DATEADD " + part);
        }
        if (!withTime) {
            // Treat date as timestamp at the start of this date
            forceTimestamp = true;
        }
        timeNanos += count;
        if (timeNanos >= DateTimeUtils.NANOS_PER_DAY || timeNanos < 0) {
            long d;
            if (timeNanos >= DateTimeUtils.NANOS_PER_DAY) {
                d = timeNanos / DateTimeUtils.NANOS_PER_DAY;
            } else {
                d = (timeNanos - DateTimeUtils.NANOS_PER_DAY + 1) / DateTimeUtils.NANOS_PER_DAY;
            }
            timeNanos -= d * DateTimeUtils.NANOS_PER_DAY;
            return DateTimeUtils.dateTimeToValue(v,
                    DateTimeUtils.dateValueFromAbsoluteDay(DateTimeUtils.absoluteDayFromDateValue(dateValue) + d),
                    timeNanos, forceTimestamp);
        }
        return DateTimeUtils.dateTimeToValue(v, dateValue, timeNanos, forceTimestamp);
    }

    /**
     * Calculate the number of crossed unit boundaries between two timestamps. This
     * method is supported for MS SQL Server compatibility.
     *
     * <pre>
     * DATEDIFF(YEAR, '2004-12-31', '2005-01-01') = 1
     * </pre>
     *
     * @param part
     *            the part
     * @param v1
     *            the first date-time value
     * @param v2
     *            the second date-time value
     * @return the number of crossed boundaries
     */
    public static long datediff(String part, Value v1, Value v2) {
        int field = getDatePart(part);
        long[] a1 = DateTimeUtils.dateAndTimeFromValue(v1);
        long dateValue1 = a1[0];
        long absolute1 = DateTimeUtils.absoluteDayFromDateValue(dateValue1);
        long[] a2 = DateTimeUtils.dateAndTimeFromValue(v2);
        long dateValue2 = a2[0];
        long absolute2 = DateTimeUtils.absoluteDayFromDateValue(dateValue2);
        switch (field) {
        case NANOSECOND:
        case MICROSECOND:
        case MILLISECOND:
        case SECOND:
        case EPOCH:
        case MINUTE:
        case HOUR:
            long timeNanos1 = a1[1];
            long timeNanos2 = a2[1];
            switch (field) {
            case NANOSECOND:
                return (absolute2 - absolute1) * DateTimeUtils.NANOS_PER_DAY + (timeNanos2 - timeNanos1);
            case MICROSECOND:
                return (absolute2 - absolute1) * (DateTimeUtils.MILLIS_PER_DAY * 1_000)
                        + (timeNanos2 / 1_000 - timeNanos1 / 1_000);
            case MILLISECOND:
                return (absolute2 - absolute1) * DateTimeUtils.MILLIS_PER_DAY
                        + (timeNanos2 / 1_000_000 - timeNanos1 / 1_000_000);
            case SECOND:
            case EPOCH:
                return (absolute2 - absolute1) * 86_400 + (timeNanos2 / 1_000_000_000 - timeNanos1 / 1_000_000_000);
            case MINUTE:
                return (absolute2 - absolute1) * 1_440 + (timeNanos2 / 60_000_000_000L - timeNanos1 / 60_000_000_000L);
            case HOUR:
                return (absolute2 - absolute1) * 24
                        + (timeNanos2 / 3_600_000_000_000L - timeNanos1 / 3_600_000_000_000L);
            }
            // Fake fall-through
            // $FALL-THROUGH$
        case DAY_OF_MONTH:
        case DAY_OF_YEAR:
        case DAY_OF_WEEK:
        case ISO_DAY_OF_WEEK:
            return absolute2 - absolute1;
        case WEEK:
            return weekdiff(absolute1, absolute2, 0);
        case ISO_WEEK:
            return weekdiff(absolute1, absolute2, 1);
        case MONTH:
            return (DateTimeUtils.yearFromDateValue(dateValue2) - DateTimeUtils.yearFromDateValue(dateValue1)) * 12
                    + DateTimeUtils.monthFromDateValue(dateValue2) - DateTimeUtils.monthFromDateValue(dateValue1);
        case QUARTER:
            return (DateTimeUtils.yearFromDateValue(dateValue2) - DateTimeUtils.yearFromDateValue(dateValue1)) * 4
                    + (DateTimeUtils.monthFromDateValue(dateValue2) - 1) / 3
                    - (DateTimeUtils.monthFromDateValue(dateValue1) - 1) / 3;
        case YEAR:
            return DateTimeUtils.yearFromDateValue(dateValue2) - DateTimeUtils.yearFromDateValue(dateValue1);
        case TIMEZONE_HOUR:
        case TIMEZONE_MINUTE: {
            int offsetMinutes1;
            if (v1 instanceof ValueTimestampTimeZone) {
                offsetMinutes1 = ((ValueTimestampTimeZone) v1).getTimeZoneOffsetMins();
            } else {
                offsetMinutes1 = DateTimeUtils.getTimeZoneOffsetMillis(null, dateValue1, a1[1]);
            }
            int offsetMinutes2;
            if (v2 instanceof ValueTimestampTimeZone) {
                offsetMinutes2 = ((ValueTimestampTimeZone) v2).getTimeZoneOffsetMins();
            } else {
                offsetMinutes2 = DateTimeUtils.getTimeZoneOffsetMillis(null, dateValue2, a2[1]);
            }
            if (field == TIMEZONE_HOUR) {
                return (offsetMinutes2 / 60) - (offsetMinutes1 / 60);
            } else {
                return offsetMinutes2 - offsetMinutes1;
            }
        }
        default:
            throw DbException.getUnsupportedException("DATEDIFF " + part);
        }
    }

    /**
     * Extracts specified field from the specified date-time value.
     *
     * @param part
     *            the date part
     * @param value
     *            the date-time value
     * @return extracted field
     */
    public static Value extract(String part, Value value) {
        Value result;
        int field = getDatePart(part);
        if (field != EPOCH) {
            result = ValueInt.get(getIntDatePart(value, field));
        } else {

            // Case where we retrieve the EPOCH time.
            // First we retrieve the dateValue and his time in nanoseconds.
            long[] a = DateTimeUtils.dateAndTimeFromValue(value);
            long dateValue = a[0];
            long timeNanos = a[1];
            // We compute the time in nanoseconds and the total number of days.
            BigDecimal timeNanosBigDecimal = new BigDecimal(timeNanos);
            BigDecimal numberOfDays = new BigDecimal(DateTimeUtils.absoluteDayFromDateValue(dateValue));
            BigDecimal nanosSeconds = new BigDecimal(1_000_000_000);
            BigDecimal secondsPerDay = new BigDecimal(DateTimeUtils.SECONDS_PER_DAY);

            // Case where the value is of type time e.g. '10:00:00'
            if (value instanceof ValueTime) {

                // In order to retrieve the EPOCH time we only have to convert the time
                // in nanoseconds (previously retrieved) in seconds.
                result = ValueDecimal.get(timeNanosBigDecimal.divide(nanosSeconds));

            } else if (value instanceof ValueDate) {

                // Case where the value is of type date '2000:01:01', we have to retrieve the
                // total number of days and multiply it by the number of seconds in a day.
                result = ValueDecimal.get(numberOfDays.multiply(secondsPerDay));

            } else if (value instanceof ValueTimestampTimeZone) {

                // Case where the value is a of type ValueTimestampTimeZone
                // ('2000:01:01 10:00:00+05').
                // We retrieve the time zone offset in minutes
                ValueTimestampTimeZone v = (ValueTimestampTimeZone) value;
                BigDecimal timeZoneOffsetSeconds = new BigDecimal(v.getTimeZoneOffsetMins() * 60);
                // Sum the time in nanoseconds and the total number of days in seconds
                // and adding the timeZone offset in seconds.
                result = ValueDecimal.get(timeNanosBigDecimal.divide(nanosSeconds)
                        .add(numberOfDays.multiply(secondsPerDay)).subtract(timeZoneOffsetSeconds));

            } else {

                // By default, we have the date and the time ('2000:01:01 10:00:00') if no type
                // is given.
                // We just have to sum the time in nanoseconds and the total number of days in
                // seconds.
                result = ValueDecimal
                        .get(timeNanosBigDecimal.divide(nanosSeconds).add(numberOfDays.multiply(secondsPerDay)));
            }
        }
        return result;
    }

    /**
     * Truncate the given date to the unit specified
     *
     * @param datePartStr the time unit (e.g. 'DAY', 'HOUR', etc.)
     * @param valueDate the date
     * @return date truncated to 'day'
     */
    public static Value truncateDate(String datePartStr, Value valueDate) {

        int timeUnit = getDatePart(datePartStr);

        // Retrieve the dateValue and the time in nanoseconds of the date.
        long[] fieldDateAndTime = DateTimeUtils.dateAndTimeFromValue(valueDate);
        long dateValue = fieldDateAndTime[0];
        long timeNanosRetrieved = fieldDateAndTime[1];

        // Variable used to the time in nanoseconds of the date truncated.
        long timeNanos;

        // Compute the number of time unit in the date, for example, the
        // number of time unit 'HOUR' in '15:14:13' is '15'. Then convert the
        // result to nanoseconds.
        switch (timeUnit) {

        case MICROSECOND:

            long nanoInMicroSecond = 1_000L;
            long microseconds = timeNanosRetrieved / nanoInMicroSecond;
            timeNanos = microseconds * nanoInMicroSecond;
            break;

        case MILLISECOND:

            long nanoInMilliSecond = 1_000_000L;
            long milliseconds = timeNanosRetrieved / nanoInMilliSecond;
            timeNanos = milliseconds * nanoInMilliSecond;
            break;

        case SECOND:

            long nanoInSecond = 1_000_000_000L;
            long seconds = timeNanosRetrieved / nanoInSecond;
            timeNanos = seconds * nanoInSecond;
            break;

        case MINUTE:

            long nanoInMinute = 60_000_000_000L;
            long minutes = timeNanosRetrieved / nanoInMinute;
            timeNanos = minutes * nanoInMinute;
            break;

        case HOUR:

            long nanoInHour = 3_600_000_000_000L;
            long hours = timeNanosRetrieved / nanoInHour;
            timeNanos = hours * nanoInHour;
            break;

        case DAY_OF_MONTH:

            timeNanos = 0L;
            break;

        case WEEK:

            long absoluteDay = DateTimeUtils.absoluteDayFromDateValue(dateValue);
            int dayOfWeek = DateTimeUtils.getDayOfWeekFromAbsolute(absoluteDay, 1);
            if (dayOfWeek != 1) {
                dateValue = DateTimeUtils.dateValueFromAbsoluteDay(absoluteDay - dayOfWeek + 1);
            }
            timeNanos = 0L;
            break;

        case MONTH: {

            long year = DateTimeUtils.yearFromDateValue(dateValue);
            int month = DateTimeUtils.monthFromDateValue(dateValue);
            dateValue = DateTimeUtils.dateValue(year, month, 1);
            timeNanos = 0L;
            break;

        }
        case QUARTER: {

            long year = DateTimeUtils.yearFromDateValue(dateValue);
            int month = DateTimeUtils.monthFromDateValue(dateValue);
            month = ((month - 1) / 3) * 3 + 1;
            dateValue = DateTimeUtils.dateValue(year, month, 1);
            timeNanos = 0L;
            break;

        }
        case YEAR: {

            long year = DateTimeUtils.yearFromDateValue(dateValue);
            dateValue = DateTimeUtils.dateValue(year, 1, 1);
            timeNanos = 0L;
            break;

        }
        case DECADE: {

            long year = DateTimeUtils.yearFromDateValue(dateValue);
            year = (year / 10) * 10;
            dateValue = DateTimeUtils.dateValue(year, 1, 1);
            timeNanos = 0L;
            break;

        }
        case CENTURY: {

            long year = DateTimeUtils.yearFromDateValue(dateValue);
            year = ((year - 1) / 100) * 100 + 1;
            dateValue = DateTimeUtils.dateValue(year, 1, 1);
            timeNanos = 0L;
            break;

        }
        case MILLENNIUM: {

            long year = DateTimeUtils.yearFromDateValue(dateValue);
            year = ((year - 1) / 1000) * 1000 + 1;
            dateValue = DateTimeUtils.dateValue(year, 1, 1);
            timeNanos = 0L;
            break;

        }
        default:

            // Return an exception in the timeUnit is not recognized
            throw DbException.getUnsupportedException(datePartStr);

        }

        Value result;

        if (valueDate instanceof ValueTimestampTimeZone) {

            // Case we create a timestamp with timezone with the dateValue and
            // timeNanos computed.
            ValueTimestampTimeZone vTmp = (ValueTimestampTimeZone) valueDate;
            result = ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos, vTmp.getTimeZoneOffsetMins());

        } else {

            // By default, we create a timestamp with the dateValue and
            // timeNanos computed.
            result = ValueTimestamp.fromDateValueAndNanos(dateValue, timeNanos);

        }

        return result;
    }

    /**
     * Formats a date using a format string.
     *
     * @param date
     *            the date to format
     * @param format
     *            the format string
     * @param locale
     *            the locale
     * @param timeZone
     *            the timezone
     * @return the formatted date
     */
    public static String formatDateTime(java.util.Date date, String format, String locale, String timeZone) {
        SimpleDateFormat dateFormat = getDateFormat(format, locale, timeZone);
        synchronized (dateFormat) {
            return dateFormat.format(date);
        }
    }

    private static SimpleDateFormat getDateFormat(String format, String locale, String timeZone) {
        try {
            // currently, a new instance is create for each call
            // however, could cache the last few instances
            SimpleDateFormat df;
            if (locale == null) {
                df = new SimpleDateFormat(format);
            } else {
                Locale l = new Locale(locale);
                df = new SimpleDateFormat(format, l);
            }
            if (timeZone != null) {
                df.setTimeZone(TimeZone.getTimeZone(timeZone));
            }
            return df;
        } catch (Exception e) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, e, format + "/" + locale + "/" + timeZone);
        }
    }

    private static int getDatePart(String part) {
        Integer p = DATE_PART.get(StringUtils.toUpperEnglish(part));
        if (p == null) {
            throw DbException.getInvalidValueException("date part", part);
        }
        return p.intValue();
    }

    /**
     * Get the specified field of a date, however with years normalized to positive
     * or negative, and month starting with 1.
     *
     * @param date
     *            the date value
     * @param field
     *            the field type, see {@link Function} for constants
     * @return the value
     */
    public static int getIntDatePart(Value date, int field) {
        long[] a = DateTimeUtils.dateAndTimeFromValue(date);
        long dateValue = a[0];
        long timeNanos = a[1];
        switch (field) {
        case YEAR:
            return DateTimeUtils.yearFromDateValue(dateValue);
        case MONTH:
            return DateTimeUtils.monthFromDateValue(dateValue);
        case DAY_OF_MONTH:
            return DateTimeUtils.dayFromDateValue(dateValue);
        case HOUR:
            return (int) (timeNanos / 3_600_000_000_000L % 24);
        case MINUTE:
            return (int) (timeNanos / 60_000_000_000L % 60);
        case SECOND:
            return (int) (timeNanos / 1_000_000_000 % 60);
        case MILLISECOND:
            return (int) (timeNanos / 1_000_000 % 1_000);
        case MICROSECOND:
            return (int) (timeNanos / 1_000 % 1_000_000);
        case NANOSECOND:
            return (int) (timeNanos % 1_000_000_000);
        case DAY_OF_YEAR:
            return DateTimeUtils.getDayOfYear(dateValue);
        case DAY_OF_WEEK:
            return DateTimeUtils.getSundayDayOfWeek(dateValue);
        case WEEK:
            GregorianCalendar gc = DateTimeUtils.getCalendar();
            return DateTimeUtils.getWeekOfYear(dateValue, gc.getFirstDayOfWeek() - 1, gc.getMinimalDaysInFirstWeek());
        case QUARTER:
            return (DateTimeUtils.monthFromDateValue(dateValue) - 1) / 3 + 1;
        case ISO_YEAR:
            return DateTimeUtils.getIsoWeekYear(dateValue);
        case ISO_WEEK:
            return DateTimeUtils.getIsoWeekOfYear(dateValue);
        case ISO_DAY_OF_WEEK:
            return DateTimeUtils.getIsoDayOfWeek(dateValue);
        case TIMEZONE_HOUR:
        case TIMEZONE_MINUTE: {
            int offsetMinutes;
            if (date instanceof ValueTimestampTimeZone) {
                offsetMinutes = ((ValueTimestampTimeZone) date).getTimeZoneOffsetMins();
            } else {
                offsetMinutes = DateTimeUtils.getTimeZoneOffsetMillis(null, dateValue, timeNanos);
            }
            if (field == TIMEZONE_HOUR) {
                return offsetMinutes / 60;
            }
            return offsetMinutes % 60;
        }
        }
        throw DbException.getUnsupportedException("getDatePart(" + date + ", " + field + ')');
    }

    /**
     * Return names of month or weeks.
     *
     * @param field
     *            0 for months, 1 for weekdays
     * @return names of month or weeks
     */
    public static String[] getMonthsAndWeeks(int field) {
        String[][] result = MONTHS_AND_WEEKS;
        if (result == null) {
            result = new String[2][];
            DateFormatSymbols dfs = DateFormatSymbols.getInstance(Locale.ENGLISH);
            result[0] = dfs.getMonths();
            result[1] = dfs.getWeekdays();
            MONTHS_AND_WEEKS = result;
        }
        return result[field];
    }

    /**
     * Check if a given string is a valid date part string.
     *
     * @param part
     *            the string
     * @return true if it is
     */
    public static boolean isDatePart(String part) {
        return DATE_PART.containsKey(StringUtils.toUpperEnglish(part));
    }

    /**
     * Parses a date using a format string.
     *
     * @param date
     *            the date to parse
     * @param format
     *            the parsing format
     * @param locale
     *            the locale
     * @param timeZone
     *            the timeZone
     * @return the parsed date
     */
    public static java.util.Date parseDateTime(String date, String format, String locale, String timeZone) {
        SimpleDateFormat dateFormat = getDateFormat(format, locale, timeZone);
        try {
            synchronized (dateFormat) {
                return dateFormat.parse(date);
            }
        } catch (Exception e) {
            // ParseException
            throw DbException.get(ErrorCode.PARSE_ERROR_1, e, date);
        }
    }

    private static long weekdiff(long absolute1, long absolute2, int firstDayOfWeek) {
        absolute1 += 4 - firstDayOfWeek;
        long r1 = absolute1 / 7;
        if (absolute1 < 0 && (r1 * 7 != absolute1)) {
            r1--;
        }
        absolute2 += 4 - firstDayOfWeek;
        long r2 = absolute2 / 7;
        if (absolute2 < 0 && (r2 * 7 != absolute2)) {
            r2--;
        }
        return r2 - r1;
    }

    private DateTimeFunctions() {
    }
}
