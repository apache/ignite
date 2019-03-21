/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 * Iso8601: Initial Developer: Philippe Marschall (firstName dot lastName
 * at gmail dot com)
 */
package org.h2.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.message.DbException;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueInterval;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * This utility class contains time conversion functions for Java 8
 * Date and Time API classes.
 *
 * <p>This class is implemented using reflection so that it compiles on
 * Java 7 as well.</p>
 *
 * <p>Custom conversion methods between H2 internal values and JSR-310 classes
 * are used in most cases without intermediate conversions to java.sql classes.
 * Direct conversion is simpler, faster, and it does not inherit limitations
 * and issues from java.sql classes and conversion methods provided by JDK.</p>
 *
 * <p>The only one exclusion is a conversion between {@link Timestamp} and
 * Instant.</p>
 *
 * <p>Once the driver requires Java 8 and Android API 26 all the reflection
 * can be removed.</p>
 */
public class LocalDateTimeUtils {

    /**
     * {@code Class<java.time.LocalDate>} or {@code null}.
     */
    public static final Class<?> LOCAL_DATE;
    /**
     * {@code Class<java.time.LocalTime>} or {@code null}.
     */
    public static final Class<?> LOCAL_TIME;
    /**
     * {@code Class<java.time.LocalDateTime>} or {@code null}.
     */
    public static final Class<?> LOCAL_DATE_TIME;
    /**
     * {@code Class<java.time.Instant>} or {@code null}.
     */
    public static final Class<?> INSTANT;
    /**
     * {@code Class<java.time.OffsetDateTime>} or {@code null}.
     */
    public static final Class<?> OFFSET_DATE_TIME;

    /**
     * {@code Class<java.time.ZoneOffset>} or {@code null}.
     */
    private static final Class<?> ZONE_OFFSET;

    /**
     * {@code Class<java.time.Period>} or {@code null}.
     */
    public static final Class<?> PERIOD;

    /**
     * {@code Class<java.time.Duration>} or {@code null}.
     */
    public static final Class<?> DURATION;

    /**
     * {@code java.time.LocalTime#ofNanoOfDay()} or {@code null}.
     */
    private static final Method LOCAL_TIME_OF_NANO;

    /**
     * {@code java.time.LocalTime#toNanoOfDay()} or {@code null}.
     */
    private static final Method LOCAL_TIME_TO_NANO;

    /**
     * {@code java.time.LocalDate#of(int, int, int)} or {@code null}.
     */
    private static final Method LOCAL_DATE_OF_YEAR_MONTH_DAY;
    /**
     * {@code java.time.LocalDate#getYear()} or {@code null}.
     */
    private static final Method LOCAL_DATE_GET_YEAR;
    /**
     * {@code java.time.LocalDate#getMonthValue()} or {@code null}.
     */
    private static final Method LOCAL_DATE_GET_MONTH_VALUE;
    /**
     * {@code java.time.LocalDate#getDayOfMonth()} or {@code null}.
     */
    private static final Method LOCAL_DATE_GET_DAY_OF_MONTH;
    /**
     * {@code java.time.LocalDate#atStartOfDay()} or {@code null}.
     */
    private static final Method LOCAL_DATE_AT_START_OF_DAY;

    /**
     * {@code java.time.Instant#getEpochSecond()} or {@code null}.
     */
    private static final Method INSTANT_GET_EPOCH_SECOND;
    /**
     * {@code java.time.Instant#getNano()} or {@code null}.
     */
    private static final Method INSTANT_GET_NANO;
    /**
     * {@code java.sql.Timestamp.toInstant()} or {@code null}.
     */
    private static final Method TIMESTAMP_TO_INSTANT;

    /**
     * {@code java.time.LocalDateTime#plusNanos(long)} or {@code null}.
     */
    private static final Method LOCAL_DATE_TIME_PLUS_NANOS;
    /**
     * {@code java.time.LocalDateTime#toLocalDate()} or {@code null}.
     */
    private static final Method LOCAL_DATE_TIME_TO_LOCAL_DATE;
    /**
     * {@code java.time.LocalDateTime#toLocalTime()} or {@code null}.
     */
    private static final Method LOCAL_DATE_TIME_TO_LOCAL_TIME;

    /**
     * {@code java.time.ZoneOffset#ofTotalSeconds(int)} or {@code null}.
     */
    private static final Method ZONE_OFFSET_OF_TOTAL_SECONDS;

    /**
     * {@code java.time.OffsetDateTime#of(LocalDateTime, ZoneOffset)} or
     * {@code null}.
     */
    private static final Method OFFSET_DATE_TIME_OF_LOCAL_DATE_TIME_ZONE_OFFSET;
    /**
     * {@code java.time.OffsetDateTime#toLocalDateTime()} or {@code null}.
     */
    private static final Method OFFSET_DATE_TIME_TO_LOCAL_DATE_TIME;
    /**
     * {@code java.time.OffsetDateTime#getOffset()} or {@code null}.
     */
    private static final Method OFFSET_DATE_TIME_GET_OFFSET;

    /**
     * {@code java.time.ZoneOffset#getTotalSeconds()} or {@code null}.
     */
    private static final Method ZONE_OFFSET_GET_TOTAL_SECONDS;

    /**
     * {@code java.time.Period#of(int, int, int)} or {@code null}.
     */
    private static final Method PERIOD_OF;

    /**
     * {@code java.time.Period#getYears()} or {@code null}.
     */
    private static final Method PERIOD_GET_YEARS;

    /**
     * {@code java.time.Period#getMonths()} or {@code null}.
     */
    private static final Method PERIOD_GET_MONTHS;

    /**
     * {@code java.time.Period#getDays()} or {@code null}.
     */
    private static final Method PERIOD_GET_DAYS;

    /**
     * {@code java.time.Duration#ofSeconds(long, long)} or {@code null}.
     */
    private static final Method DURATION_OF_SECONDS;

    /**
     * {@code java.time.Duration#getSeconds()} or {@code null}.
     */
    private static final Method DURATION_GET_SECONDS;

    /**
     * {@code java.time.Duration#getNano()} or {@code null}.
     */
    private static final Method DURATION_GET_NANO;

    private static final boolean IS_JAVA8_DATE_API_PRESENT;

    static {
        LOCAL_DATE = tryGetClass("java.time.LocalDate");
        LOCAL_TIME = tryGetClass("java.time.LocalTime");
        LOCAL_DATE_TIME = tryGetClass("java.time.LocalDateTime");
        INSTANT = tryGetClass("java.time.Instant");
        OFFSET_DATE_TIME = tryGetClass("java.time.OffsetDateTime");
        ZONE_OFFSET = tryGetClass("java.time.ZoneOffset");
        PERIOD = tryGetClass("java.time.Period");
        DURATION = tryGetClass("java.time.Duration");
        IS_JAVA8_DATE_API_PRESENT = LOCAL_DATE != null && LOCAL_TIME != null &&
                LOCAL_DATE_TIME != null && INSTANT != null &&
                OFFSET_DATE_TIME != null && ZONE_OFFSET != null && PERIOD != null && DURATION != null;

        if (IS_JAVA8_DATE_API_PRESENT) {
            LOCAL_TIME_OF_NANO = getMethod(LOCAL_TIME, "ofNanoOfDay", long.class);

            LOCAL_TIME_TO_NANO = getMethod(LOCAL_TIME, "toNanoOfDay");

            LOCAL_DATE_OF_YEAR_MONTH_DAY = getMethod(LOCAL_DATE, "of",
                    int.class, int.class, int.class);
            LOCAL_DATE_GET_YEAR = getMethod(LOCAL_DATE, "getYear");
            LOCAL_DATE_GET_MONTH_VALUE = getMethod(LOCAL_DATE, "getMonthValue");
            LOCAL_DATE_GET_DAY_OF_MONTH = getMethod(LOCAL_DATE, "getDayOfMonth");
            LOCAL_DATE_AT_START_OF_DAY = getMethod(LOCAL_DATE, "atStartOfDay");

            INSTANT_GET_EPOCH_SECOND = getMethod(INSTANT, "getEpochSecond");
            INSTANT_GET_NANO = getMethod(INSTANT, "getNano");
            TIMESTAMP_TO_INSTANT = getMethod(Timestamp.class, "toInstant");

            LOCAL_DATE_TIME_PLUS_NANOS = getMethod(LOCAL_DATE_TIME, "plusNanos", long.class);
            LOCAL_DATE_TIME_TO_LOCAL_DATE = getMethod(LOCAL_DATE_TIME, "toLocalDate");
            LOCAL_DATE_TIME_TO_LOCAL_TIME = getMethod(LOCAL_DATE_TIME, "toLocalTime");

            ZONE_OFFSET_OF_TOTAL_SECONDS = getMethod(ZONE_OFFSET, "ofTotalSeconds", int.class);

            OFFSET_DATE_TIME_TO_LOCAL_DATE_TIME = getMethod(OFFSET_DATE_TIME, "toLocalDateTime");
            OFFSET_DATE_TIME_GET_OFFSET = getMethod(OFFSET_DATE_TIME, "getOffset");
            OFFSET_DATE_TIME_OF_LOCAL_DATE_TIME_ZONE_OFFSET = getMethod(
                    OFFSET_DATE_TIME, "of", LOCAL_DATE_TIME, ZONE_OFFSET);

            ZONE_OFFSET_GET_TOTAL_SECONDS = getMethod(ZONE_OFFSET, "getTotalSeconds");

            PERIOD_OF = getMethod(PERIOD, "of", int.class, int.class, int.class);
            PERIOD_GET_YEARS = getMethod(PERIOD, "getYears");
            PERIOD_GET_MONTHS = getMethod(PERIOD, "getMonths");
            PERIOD_GET_DAYS = getMethod(PERIOD, "getDays");

            DURATION_OF_SECONDS = getMethod(DURATION, "ofSeconds", long.class, long.class);
            DURATION_GET_SECONDS = getMethod(DURATION, "getSeconds");
            DURATION_GET_NANO = getMethod(DURATION, "getNano");
        } else {
            LOCAL_TIME_OF_NANO = null;
            LOCAL_TIME_TO_NANO = null;
            LOCAL_DATE_OF_YEAR_MONTH_DAY = null;
            LOCAL_DATE_GET_YEAR = null;
            LOCAL_DATE_GET_MONTH_VALUE = null;
            LOCAL_DATE_GET_DAY_OF_MONTH = null;
            LOCAL_DATE_AT_START_OF_DAY = null;
            INSTANT_GET_EPOCH_SECOND = null;
            INSTANT_GET_NANO = null;
            TIMESTAMP_TO_INSTANT = null;
            LOCAL_DATE_TIME_PLUS_NANOS = null;
            LOCAL_DATE_TIME_TO_LOCAL_DATE = null;
            LOCAL_DATE_TIME_TO_LOCAL_TIME = null;
            ZONE_OFFSET_OF_TOTAL_SECONDS = null;
            OFFSET_DATE_TIME_TO_LOCAL_DATE_TIME = null;
            OFFSET_DATE_TIME_GET_OFFSET = null;
            OFFSET_DATE_TIME_OF_LOCAL_DATE_TIME_ZONE_OFFSET = null;
            ZONE_OFFSET_GET_TOTAL_SECONDS = null;
            PERIOD_OF = null;
            PERIOD_GET_YEARS = null;
            PERIOD_GET_MONTHS = null;
            PERIOD_GET_DAYS = null;
            DURATION_OF_SECONDS = null;
            DURATION_GET_SECONDS = null;
            DURATION_GET_NANO = null;
        }
    }

    private LocalDateTimeUtils() {
        // utility class
    }

    /**
     * Checks if the Java 8 Date and Time API is present.
     *
     * <p>This is the case on Java 8 and later and not the case on
     * Java 7. Versions older than Java 7 are not supported.</p>
     *
     * @return if the Java 8 Date and Time API is present
     */
    public static boolean isJava8DateApiPresent() {
        return IS_JAVA8_DATE_API_PRESENT;
    }

    private static Class<?> tryGetClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    private static Method getMethod(Class<?> clazz, String methodName,
            Class<?>... parameterTypes) {
        try {
            return clazz.getMethod(methodName, parameterTypes);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Java 8 or later but method " +
                    clazz.getName() + "#" + methodName + "(" +
                    Arrays.toString(parameterTypes) + ") is missing", e);
        }
    }

    /**
     * Converts a value to a LocalDate.
     *
     * <p>This method should only called from Java 8 or later.</p>
     *
     * @param value the value to convert
     * @return the LocalDate
     */
    public static Object valueToLocalDate(Value value) {
        try {
            return localDateFromDateValue(((ValueDate) value.convertTo(Value.DATE)).getDateValue());
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "date conversion failed");
        }
    }

    /**
     * Converts a value to a LocalTime.
     *
     * <p>This method should only called from Java 8 or later.</p>
     *
     * @param value the value to convert
     * @return the LocalTime
     */
    public static Object valueToLocalTime(Value value) {
        try {
            return LOCAL_TIME_OF_NANO.invoke(null,
                    ((ValueTime) value.convertTo(Value.TIME)).getNanos());
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "time conversion failed");
        }
    }

    /**
     * Converts a value to a LocalDateTime.
     *
     * <p>This method should only called from Java 8 or later.</p>
     *
     * @param value the value to convert
     * @return the LocalDateTime
     */
    public static Object valueToLocalDateTime(Value value) {
        ValueTimestamp valueTimestamp = (ValueTimestamp) value.convertTo(Value.TIMESTAMP);
        long dateValue = valueTimestamp.getDateValue();
        long timeNanos = valueTimestamp.getTimeNanos();
        try {
            return localDateTimeFromDateNanos(dateValue, timeNanos);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "timestamp conversion failed");
        }
    }

    /**
     * Converts a value to a Instant.
     *
     * <p>This method should only called from Java 8 or later.</p>
     *
     * @param value the value to convert
     * @return the Instant
     */
    public static Object valueToInstant(Value value) {
        try {
            return TIMESTAMP_TO_INSTANT.invoke(value.getTimestamp());
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "timestamp conversion failed");
        }
    }

    /**
     * Converts a value to a OffsetDateTime.
     *
     * <p>This method should only called from Java 8 or later.</p>
     *
     * @param value the value to convert
     * @return the OffsetDateTime
     */
    public static Object valueToOffsetDateTime(Value value) {
        ValueTimestampTimeZone valueTimestampTimeZone = (ValueTimestampTimeZone) value.convertTo(Value.TIMESTAMP_TZ);
        long dateValue = valueTimestampTimeZone.getDateValue();
        long timeNanos = valueTimestampTimeZone.getTimeNanos();
        try {
            Object localDateTime = localDateTimeFromDateNanos(dateValue, timeNanos);

            short timeZoneOffsetMins = valueTimestampTimeZone.getTimeZoneOffsetMins();
            int offsetSeconds = (int) TimeUnit.MINUTES.toSeconds(timeZoneOffsetMins);

            Object offset = ZONE_OFFSET_OF_TOTAL_SECONDS.invoke(null, offsetSeconds);

            return OFFSET_DATE_TIME_OF_LOCAL_DATE_TIME_ZONE_OFFSET.invoke(null,
                    localDateTime, offset);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "timestamp with time zone conversion failed");
        }
    }

    /**
     * Converts a value to a Period.
     *
     * <p>This method should only called from Java 8 or later.</p>
     *
     * @param value the value to convert
     * @return the Period
     */
    public static Object valueToPeriod(Value value) {
        if (!(value instanceof ValueInterval)) {
            value = value.convertTo(Value.INTERVAL_YEAR_TO_MONTH);
        }
        if (!DataType.isYearMonthIntervalType(value.getValueType())) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, (Throwable) null, value.getString());
        }
        ValueInterval v = (ValueInterval) value;
        IntervalQualifier qualifier = v.getQualifier();
        boolean negative = v.isNegative();
        long leading = v.getLeading();
        long remaining = v.getRemaining();
        int y = Value.convertToInt(IntervalUtils.yearsFromInterval(qualifier, negative, leading, remaining), null);
        int m = Value.convertToInt(IntervalUtils.monthsFromInterval(qualifier, negative, leading, remaining), null);
        try {
            return PERIOD_OF.invoke(null, y, m, 0);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "timestamp with time zone conversion failed");
        }
    }

    /**
     * Converts a value to a Duration.
     *
     * <p>This method should only called from Java 8 or later.</p>
     *
     * @param value the value to convert
     * @return the Duration
     */
    public static Object valueToDuration(Value value) {
        if (!(value instanceof ValueInterval)) {
            value = value.convertTo(Value.INTERVAL_DAY_TO_SECOND);
        }
        if (DataType.isYearMonthIntervalType(value.getValueType())) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, (Throwable) null, value.getString());
        }
        BigInteger[] dr = IntervalUtils.intervalToAbsolute((ValueInterval) value)
                .divideAndRemainder(BigInteger.valueOf(1_000_000_000));
        try {
            return DURATION_OF_SECONDS.invoke(null, dr[0].longValue(), dr[1].longValue());
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "timestamp with time zone conversion failed");
        }
    }

    /**
     * Converts a LocalDate to a Value.
     *
     * @param localDate the LocalDate to convert, not {@code null}
     * @return the value
     */
    public static Value localDateToDateValue(Object localDate) {
        try {
            return ValueDate.fromDateValue(dateValueFromLocalDate(localDate));
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "date conversion failed");
        }
    }

    /**
     * Converts a LocalTime to a Value.
     *
     * @param localTime the LocalTime to convert, not {@code null}
     * @return the value
     */
    public static Value localTimeToTimeValue(Object localTime) {
        try {
            return ValueTime.fromNanos((Long) LOCAL_TIME_TO_NANO.invoke(localTime));
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "time conversion failed");
        }
    }

    /**
     * Converts a LocalDateTime to a Value.
     *
     * @param localDateTime the LocalDateTime to convert, not {@code null}
     * @return the value
     */
    public static Value localDateTimeToValue(Object localDateTime) {
        try {
            Object localDate = LOCAL_DATE_TIME_TO_LOCAL_DATE.invoke(localDateTime);
            long dateValue = dateValueFromLocalDate(localDate);
            long timeNanos = timeNanosFromLocalDateTime(localDateTime);
            return ValueTimestamp.fromDateValueAndNanos(dateValue, timeNanos);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "local date time conversion failed");
        }
    }

    /**
     * Converts a Instant to a Value.
     *
     * @param instant the Instant to convert, not {@code null}
     * @return the value
     */
    public static Value instantToValue(Object instant) {
        try {
            long epochSecond = (long) INSTANT_GET_EPOCH_SECOND.invoke(instant);
            int nano = (int) INSTANT_GET_NANO.invoke(instant);
            long absoluteDay = epochSecond / 86_400;
            // Round toward negative infinity
            if (epochSecond < 0 && (absoluteDay * 86_400 != epochSecond)) {
                absoluteDay--;
            }
            long timeNanos = (epochSecond - absoluteDay * 86_400) * 1_000_000_000 + nano;
            return ValueTimestampTimeZone.fromDateValueAndNanos(
                    DateTimeUtils.dateValueFromAbsoluteDay(absoluteDay), timeNanos, (short) 0);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "instant conversion failed");
        }
    }

    /**
     * Converts a OffsetDateTime to a Value.
     *
     * @param offsetDateTime the OffsetDateTime to convert, not {@code null}
     * @return the value
     */
    public static ValueTimestampTimeZone offsetDateTimeToValue(Object offsetDateTime) {
        try {
            Object localDateTime = OFFSET_DATE_TIME_TO_LOCAL_DATE_TIME.invoke(offsetDateTime);
            Object localDate = LOCAL_DATE_TIME_TO_LOCAL_DATE.invoke(localDateTime);
            Object zoneOffset = OFFSET_DATE_TIME_GET_OFFSET.invoke(offsetDateTime);

            long dateValue = dateValueFromLocalDate(localDate);
            long timeNanos = timeNanosFromLocalDateTime(localDateTime);
            short timeZoneOffsetMins = zoneOffsetToOffsetMinute(zoneOffset);
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue,
                    timeNanos, timeZoneOffsetMins);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "time conversion failed");
        }
    }

    private static long dateValueFromLocalDate(Object localDate)
                    throws IllegalAccessException, InvocationTargetException {
        int year = (Integer) LOCAL_DATE_GET_YEAR.invoke(localDate);
        int month = (Integer) LOCAL_DATE_GET_MONTH_VALUE.invoke(localDate);
        int day = (Integer) LOCAL_DATE_GET_DAY_OF_MONTH.invoke(localDate);
        return DateTimeUtils.dateValue(year, month, day);
    }

    private static long timeNanosFromLocalDateTime(Object localDateTime)
                    throws IllegalAccessException, InvocationTargetException {
        Object localTime = LOCAL_DATE_TIME_TO_LOCAL_TIME.invoke(localDateTime);
        return (Long) LOCAL_TIME_TO_NANO.invoke(localTime);
    }

    private static short zoneOffsetToOffsetMinute(Object zoneOffset)
                    throws IllegalAccessException, InvocationTargetException {
        int totalSeconds = (Integer) ZONE_OFFSET_GET_TOTAL_SECONDS.invoke(zoneOffset);
        return (short) TimeUnit.SECONDS.toMinutes(totalSeconds);
    }

    private static Object localDateFromDateValue(long dateValue)
                    throws IllegalAccessException, InvocationTargetException {

        int year = DateTimeUtils.yearFromDateValue(dateValue);
        int month = DateTimeUtils.monthFromDateValue(dateValue);
        int day = DateTimeUtils.dayFromDateValue(dateValue);
        try {
            return LOCAL_DATE_OF_YEAR_MONTH_DAY.invoke(null, year, month, day);
        } catch (InvocationTargetException e) {
            if (year <= 1500 && (year & 3) == 0 && month == 2 && day == 29) {
                // If proleptic Gregorian doesn't have such date use the next day
                return LOCAL_DATE_OF_YEAR_MONTH_DAY.invoke(null, year, 3, 1);
            }
            throw e;
        }
    }

    private static Object localDateTimeFromDateNanos(long dateValue, long timeNanos)
                    throws IllegalAccessException, InvocationTargetException {
        Object localDate = localDateFromDateValue(dateValue);
        Object localDateTime = LOCAL_DATE_AT_START_OF_DAY.invoke(localDate);
        return LOCAL_DATE_TIME_PLUS_NANOS.invoke(localDateTime, timeNanos);
    }

    /**
     * Converts a Period to a Value.
     *
     * @param period the Period to convert, not {@code null}
     * @return the value
     */
    public static ValueInterval periodToValue(Object period) {
        try {
            int days = (int) PERIOD_GET_DAYS.invoke(period);
            if (days != 0) {
                throw DbException.getInvalidValueException("Period.days", days);
            }
            int years = (int) PERIOD_GET_YEARS.invoke(period);
            int months = (int) PERIOD_GET_MONTHS.invoke(period);
            IntervalQualifier qualifier;
            boolean negative = false;
            long leading = 0L, remaining = 0L;
            if (years == 0) {
                if (months == 0L) {
                    // Use generic qualifier
                    qualifier = IntervalQualifier.YEAR_TO_MONTH;
                } else {
                    qualifier = IntervalQualifier.MONTH;
                    leading = months;
                    if (leading < 0) {
                        leading = -leading;
                        negative = true;
                    }
                }
            } else {
                if (months == 0L) {
                    qualifier = IntervalQualifier.YEAR;
                    leading = years;
                    if (leading < 0) {
                        leading = -leading;
                        negative = true;
                    }
                } else {
                    qualifier = IntervalQualifier.YEAR_TO_MONTH;
                    leading = years * 12 + months;
                    if (leading < 0) {
                        leading = -leading;
                        negative = true;
                    }
                    remaining = leading % 12;
                    leading /= 12;
                }
            }
            return ValueInterval.from(qualifier, negative, leading, remaining);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "interval conversion failed");
        }
    }

    /**
     * Converts a Duration to a Value.
     *
     * @param duration the Duration to convert, not {@code null}
     * @return the value
     */
    public static ValueInterval durationToValue(Object duration) {
        try {
            long seconds = (long) DURATION_GET_SECONDS.invoke(duration);
            int nano = (int) DURATION_GET_NANO.invoke(duration);
            boolean negative = seconds < 0;
            seconds = Math.abs(seconds);
            if (negative && nano != 0) {
                nano = 1_000_000_000 - nano;
                seconds--;
            }
            return ValueInterval.from(IntervalQualifier.SECOND, negative, seconds, nano);
        } catch (IllegalAccessException e) {
            throw DbException.convert(e);
        } catch (InvocationTargetException e) {
            throw DbException.convertInvocation(e, "interval conversion failed");
        }
    }

}
