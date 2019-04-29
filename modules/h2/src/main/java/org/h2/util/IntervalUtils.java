/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import static org.h2.util.DateTimeUtils.NANOS_PER_DAY;
import static org.h2.util.DateTimeUtils.NANOS_PER_HOUR;
import static org.h2.util.DateTimeUtils.NANOS_PER_MINUTE;
import static org.h2.util.DateTimeUtils.NANOS_PER_SECOND;

import java.math.BigInteger;

import org.h2.api.ErrorCode;
import org.h2.api.IntervalQualifier;
import org.h2.message.DbException;
import org.h2.value.ValueInterval;

/**
 * This utility class contains interval conversion functions.
 */
public class IntervalUtils {

    private static final BigInteger NANOS_PER_SECOND_BI = BigInteger.valueOf(NANOS_PER_SECOND);

    private static final BigInteger NANOS_PER_MINUTE_BI = BigInteger.valueOf(NANOS_PER_MINUTE);

    private static final BigInteger NANOS_PER_HOUR_BI = BigInteger.valueOf(NANOS_PER_HOUR);

    /**
     * The number of nanoseconds per day as BigInteger.
     */
    public static final BigInteger NANOS_PER_DAY_BI = BigInteger.valueOf(NANOS_PER_DAY);

    private static final BigInteger MONTHS_PER_YEAR_BI = BigInteger.valueOf(12);

    private static final BigInteger HOURS_PER_DAY_BI = BigInteger.valueOf(24);

    private static final BigInteger MINUTES_PER_DAY_BI = BigInteger.valueOf(24 * 60);

    private static final BigInteger MINUTES_PER_HOUR_BI = BigInteger.valueOf(60);

    private static final BigInteger LEADING_MIN = BigInteger.valueOf(-999_999_999_999_999_999L);

    private static final BigInteger LEADING_MAX = BigInteger.valueOf(999_999_999_999_999_999L);

    private IntervalUtils() {
        // utility class
    }

    /**
     * Parses the specified string as {@code INTERVAL} value.
     *
     * @param qualifier
     *            the default qualifier to use if string does not have one
     * @param s
     *            the string with type information to parse
     * @return the interval value. Type of value can be different from the
     *         specified qualifier.
     */
    public static ValueInterval parseFormattedInterval(IntervalQualifier qualifier, String s) {
        int i = 0;
        i = skipWS(s, i);
        if (!s.regionMatches(true, i, "INTERVAL", 0, 8)) {
            return parseInterval(qualifier, false, s);
        }
        i = skipWS(s, i + 8);
        boolean negative = false;
        char ch = s.charAt(i);
        if (ch == '-') {
            negative = true;
            i = skipWS(s, i + 1);
            ch = s.charAt(i);
        } else if (ch == '+') {
            i = skipWS(s, i + 1);
            ch = s.charAt(i);
        }
        if (ch != '\'') {
            throw new IllegalArgumentException(s);
        }
        int start = ++i;
        int l = s.length();
        for (;;) {
            if (i == l) {
                throw new IllegalArgumentException(s);
            }
            if (s.charAt(i) == '\'') {
                break;
            }
            i++;
        }
        String v = s.substring(start, i);
        i = skipWS(s, i + 1);
        if (s.regionMatches(true, i, "YEAR", 0, 4)) {
            i += 4;
            int j = skipWSEnd(s, i);
            if (j == l) {
                return parseInterval(IntervalQualifier.YEAR, negative, v);
            }
            if (j > i && s.regionMatches(true, j, "TO", 0, 2)) {
                j += 2;
                i = skipWS(s, j);
                if (i > j && s.regionMatches(true, i, "MONTH", 0, 5)) {
                    if (skipWSEnd(s, i + 5) == l) {
                        return parseInterval(IntervalQualifier.YEAR_TO_MONTH, negative, v);
                    }
                }
            }
        } else if (s.regionMatches(true, i, "MONTH", 0, 5)) {
            if (skipWSEnd(s, i + 5) == l) {
                return parseInterval(IntervalQualifier.MONTH, negative, v);
            }
        }
        if (s.regionMatches(true, i, "DAY", 0, 3)) {
            i += 3;
            int j = skipWSEnd(s, i);
            if (j == l) {
                return parseInterval(IntervalQualifier.DAY, negative, v);
            }
            if (j > i && s.regionMatches(true, j, "TO", 0, 2)) {
                j += 2;
                i = skipWS(s, j);
                if (i > j) {
                    if (s.regionMatches(true, i, "HOUR", 0, 4)) {
                        if (skipWSEnd(s, i + 4) == l) {
                            return parseInterval(IntervalQualifier.DAY_TO_HOUR, negative, v);
                        }
                    } else if (s.regionMatches(true, i, "MINUTE", 0, 6)) {
                        if (skipWSEnd(s, i + 6) == l) {
                            return parseInterval(IntervalQualifier.DAY_TO_MINUTE, negative, v);
                        }
                    } else if (s.regionMatches(true, i, "SECOND", 0, 6)) {
                        if (skipWSEnd(s, i + 6) == l) {
                            return parseInterval(IntervalQualifier.DAY_TO_SECOND, negative, v);
                        }
                    }
                }
            }
        }
        if (s.regionMatches(true, i, "HOUR", 0, 4)) {
            i += 4;
            int j = skipWSEnd(s, i);
            if (j == l) {
                return parseInterval(IntervalQualifier.HOUR, negative, v);
            }
            if (j > i && s.regionMatches(true, j, "TO", 0, 2)) {
                j += 2;
                i = skipWS(s, j);
                if (i > j) {
                    if (s.regionMatches(true, i, "MINUTE", 0, 6)) {
                        if (skipWSEnd(s, i + 6) == l) {
                            return parseInterval(IntervalQualifier.HOUR_TO_MINUTE, negative, v);
                        }
                    } else if (s.regionMatches(true, i, "SECOND", 0, 6)) {
                        if (skipWSEnd(s, i + 6) == l) {
                            return parseInterval(IntervalQualifier.HOUR_TO_SECOND, negative, v);
                        }
                    }
                }
            }
        }
        if (s.regionMatches(true, i, "MINUTE", 0, 6)) {
            i += 6;
            int j = skipWSEnd(s, i);
            if (j == l) {
                return parseInterval(IntervalQualifier.MINUTE, negative, v);
            }
            if (j > i && s.regionMatches(true, j, "TO", 0, 2)) {
                j += 2;
                i = skipWS(s, j);
                if (i > j && s.regionMatches(true, i, "SECOND", 0, 6)) {
                    if (skipWSEnd(s, i + 6) == l) {
                        return parseInterval(IntervalQualifier.MINUTE_TO_SECOND, negative, v);
                    }
                }
            }
        }
        if (s.regionMatches(true, i, "SECOND", 0, 6)) {
            if (skipWSEnd(s, i + 6) == l) {
                return parseInterval(IntervalQualifier.SECOND, negative, v);
            }
        }
        throw new IllegalArgumentException(s);
    }

    private static int skipWS(String s, int i) {
        for (int l = s.length();; i++) {
            if (i == l) {
                throw new IllegalArgumentException(s);
            }
            if (!Character.isWhitespace(s.charAt(i))) {
                return i;
            }
        }
    }

    private static int skipWSEnd(String s, int i) {
        for (int l = s.length();; i++) {
            if (i == l) {
                return i;
            }
            if (!Character.isWhitespace(s.charAt(i))) {
                return i;
            }
        }
    }

    /**
     * Parses the specified string as {@code INTERVAL} value.
     *
     * @param qualifier
     *            the qualifier of interval
     * @param negative
     *            whether the interval is negative
     * @param s
     *            the string to parse
     * @return the interval value
     */
    public static ValueInterval parseInterval(IntervalQualifier qualifier, boolean negative, String s) {
        long leading, remaining;
        switch (qualifier) {
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
            leading = parseIntervalLeading(s, 0, s.length(), negative);
            remaining = 0;
            break;
        case SECOND: {
            int dot = s.indexOf('.');
            if (dot < 0) {
                leading = parseIntervalLeading(s, 0, s.length(), negative);
                remaining = 0;
            } else {
                leading = parseIntervalLeading(s, 0, dot, negative);
                remaining = DateTimeUtils.parseNanos(s, dot + 1, s.length());
            }
            break;
        }
        case YEAR_TO_MONTH:
            return parseInterval2(qualifier, s, '-', 11, negative);
        case DAY_TO_HOUR:
            return parseInterval2(qualifier, s, ' ', 23, negative);
        case DAY_TO_MINUTE: {
            int space = s.indexOf(' ');
            if (space < 0) {
                leading = parseIntervalLeading(s, 0, s.length(), negative);
                remaining = 0;
            } else {
                leading = parseIntervalLeading(s, 0, space, negative);
                int colon = s.indexOf(':', space + 1);
                if (colon < 0) {
                    remaining = parseIntervalRemaining(s, space + 1, s.length(), 23) * 60;
                } else {
                    remaining = parseIntervalRemaining(s, space + 1, colon, 23) * 60
                            + parseIntervalRemaining(s, colon + 1, s.length(), 59);
                }
            }
            break;
        }
        case DAY_TO_SECOND: {
            int space = s.indexOf(' ');
            if (space < 0) {
                leading = parseIntervalLeading(s, 0, s.length(), negative);
                remaining = 0;
            } else {
                leading = parseIntervalLeading(s, 0, space, negative);
                int colon = s.indexOf(':', space + 1);
                if (colon < 0) {
                    remaining = parseIntervalRemaining(s, space + 1, s.length(), 23) * NANOS_PER_HOUR;
                } else {
                    int colon2 = s.indexOf(':', colon + 1);
                    if (colon2 < 0) {
                        remaining = parseIntervalRemaining(s, space + 1, colon, 23) * NANOS_PER_HOUR
                                + parseIntervalRemaining(s, colon + 1, s.length(), 59) * NANOS_PER_MINUTE;
                    } else {
                        remaining = parseIntervalRemaining(s, space + 1, colon, 23) * NANOS_PER_HOUR
                                + parseIntervalRemaining(s, colon + 1, colon2, 59) * NANOS_PER_MINUTE
                                + parseIntervalRemainingSeconds(s, colon2 + 1);
                    }
                }
            }
            break;
        }
        case HOUR_TO_MINUTE:
            return parseInterval2(qualifier, s, ':', 59, negative);
        case HOUR_TO_SECOND: {
            int colon = s.indexOf(':');
            if (colon < 0) {
                leading = parseIntervalLeading(s, 0, s.length(), negative);
                remaining = 0;
            } else {
                leading = parseIntervalLeading(s, 0, colon, negative);
                int colon2 = s.indexOf(':', colon + 1);
                if (colon2 < 0) {
                    remaining = parseIntervalRemaining(s, colon + 1, s.length(), 59) * NANOS_PER_MINUTE;
                } else {
                    remaining = parseIntervalRemaining(s, colon + 1, colon2, 59) * NANOS_PER_MINUTE
                            + parseIntervalRemainingSeconds(s, colon2 + 1);
                }
            }
            break;
        }
        case MINUTE_TO_SECOND: {
            int dash = s.indexOf(':');
            if (dash < 0) {
                leading = parseIntervalLeading(s, 0, s.length(), negative);
                remaining = 0;
            } else {
                leading = parseIntervalLeading(s, 0, dash, negative);
                remaining = parseIntervalRemainingSeconds(s, dash + 1);
            }
            break;
        }
        default:
            throw new IllegalArgumentException();
        }
        negative = leading < 0;
        if (negative) {
            if (leading != Long.MIN_VALUE) {
                leading = -leading;
            } else {
                leading = 0;
            }
        }
        return ValueInterval.from(qualifier, negative, leading, remaining);
    }

    private static ValueInterval parseInterval2(IntervalQualifier qualifier, String s,
            char ch, int max, boolean negative) {
        long leading;
        long remaining;
        int dash = s.indexOf(ch, 1);
        if (dash < 0) {
            leading = parseIntervalLeading(s, 0, s.length(), negative);
            remaining = 0;
        } else {
            leading = parseIntervalLeading(s, 0, dash, negative);
            remaining = parseIntervalRemaining(s, dash + 1, s.length(), max);
        }
        negative = leading < 0;
        if (negative) {
            if (leading != Long.MIN_VALUE) {
                leading = -leading;
            } else {
                leading = 0;
            }
        }
        return ValueInterval.from(qualifier, negative, leading, remaining);
    }

    private static long parseIntervalLeading(String s, int start, int end, boolean negative) {
        long leading = Long.parseLong(s.substring(start, end));
        if (leading == 0) {
            return negative ^ s.charAt(start) == '-' ? Long.MIN_VALUE : 0;
        }
        return negative ? -leading : leading;
    }

    private static long parseIntervalRemaining(String s, int start, int end, int max) {
        int v = StringUtils.parseUInt31(s, start, end);
        if (v > max) {
            throw new IllegalArgumentException(s);
        }
        return v;
    }

    private static long parseIntervalRemainingSeconds(String s, int start) {
        int seconds, nanos;
        int dot = s.indexOf('.', start + 1);
        if (dot < 0) {
            seconds = StringUtils.parseUInt31(s, start, s.length());
            nanos = 0;
        } else {
            seconds = StringUtils.parseUInt31(s, start, dot);
            nanos = DateTimeUtils.parseNanos(s, dot + 1, s.length());
        }
        if (seconds > 59) {
            throw new IllegalArgumentException(s);
        }
        return seconds * NANOS_PER_SECOND + nanos;
    }

    /**
     * Formats interval as a string and appends it to a specified string
     * builder.
     *
     * @param buff
     *            string builder to append to
     * @param qualifier
     *            qualifier of the interval
     * @param negative
     *            whether interval is negative
     * @param leading
     *            the value of leading field
     * @param remaining
     *            the value of all remaining fields
     * @return the specified string builder
     */
    public static StringBuilder appendInterval(StringBuilder buff, IntervalQualifier qualifier, boolean negative,
            long leading, long remaining) {
        buff.append("INTERVAL '");
        if (negative) {
            buff.append('-');
        }
        switch (qualifier) {
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
            buff.append(leading);
            break;
        case SECOND:
            buff.append(leading);
            appendNanos(buff, remaining);
            break;
        case YEAR_TO_MONTH:
            buff.append(leading).append('-').append(remaining);
            break;
        case DAY_TO_HOUR:
            buff.append(leading).append(' ');
            StringUtils.appendZeroPadded(buff, 2, remaining);
            break;
        case DAY_TO_MINUTE:
            buff.append(leading).append(' ');
            StringUtils.appendZeroPadded(buff, 2, remaining / 60);
            buff.append(':');
            StringUtils.appendZeroPadded(buff, 2, remaining % 60);
            break;
        case DAY_TO_SECOND: {
            long nanos = remaining % NANOS_PER_MINUTE;
            remaining /= NANOS_PER_MINUTE;
            buff.append(leading).append(' ');
            StringUtils.appendZeroPadded(buff, 2, remaining / 60);
            buff.append(':');
            StringUtils.appendZeroPadded(buff, 2, remaining % 60);
            buff.append(':');
            appendSecondsWithNanos(buff, nanos);
            break;
        }
        case HOUR_TO_MINUTE:
            buff.append(leading).append(':');
            StringUtils.appendZeroPadded(buff, 2, remaining);
            break;
        case HOUR_TO_SECOND:
            buff.append(leading).append(':');
            StringUtils.appendZeroPadded(buff, 2, remaining / NANOS_PER_MINUTE);
            buff.append(':');
            appendSecondsWithNanos(buff, remaining % NANOS_PER_MINUTE);
            break;
        case MINUTE_TO_SECOND:
            buff.append(leading).append(':');
            appendSecondsWithNanos(buff, remaining);
            break;
        }
        return buff.append("' ").append(qualifier);
    }

    private static void appendSecondsWithNanos(StringBuilder buff, long nanos) {
        StringUtils.appendZeroPadded(buff, 2, nanos / NANOS_PER_SECOND);
        appendNanos(buff, nanos % NANOS_PER_SECOND);
    }

    private static void appendNanos(StringBuilder buff, long nanos) {
        if (nanos > 0) {
            buff.append('.');
            StringUtils.appendZeroPadded(buff, 9, nanos);
            DateTimeUtils.stripTrailingZeroes(buff);
        }
    }

    /**
     * Converts interval value to an absolute value.
     *
     * @param interval
     *            the interval value
     * @return absolute value in months for year-month intervals, in nanoseconds
     *         for day-time intervals
     */
    public static BigInteger intervalToAbsolute(ValueInterval interval) {
        BigInteger r;
        switch (interval.getQualifier()) {
        case YEAR:
            r = BigInteger.valueOf(interval.getLeading()).multiply(MONTHS_PER_YEAR_BI);
            break;
        case MONTH:
            r = BigInteger.valueOf(interval.getLeading());
            break;
        case DAY:
            r = BigInteger.valueOf(interval.getLeading()).multiply(NANOS_PER_DAY_BI);
            break;
        case HOUR:
            r = BigInteger.valueOf(interval.getLeading()).multiply(NANOS_PER_HOUR_BI);
            break;
        case MINUTE:
            r = BigInteger.valueOf(interval.getLeading()).multiply(NANOS_PER_MINUTE_BI);
            break;
        case SECOND:
            r = intervalToAbsolute(interval, NANOS_PER_SECOND_BI);
            break;
        case YEAR_TO_MONTH:
            r = intervalToAbsolute(interval, MONTHS_PER_YEAR_BI);
            break;
        case DAY_TO_HOUR:
            r = intervalToAbsolute(interval, HOURS_PER_DAY_BI, NANOS_PER_HOUR_BI);
            break;
        case DAY_TO_MINUTE:
            r = intervalToAbsolute(interval, MINUTES_PER_DAY_BI, NANOS_PER_MINUTE_BI);
            break;
        case DAY_TO_SECOND:
            r = intervalToAbsolute(interval, NANOS_PER_DAY_BI);
            break;
        case HOUR_TO_MINUTE:
            r = intervalToAbsolute(interval, MINUTES_PER_HOUR_BI, NANOS_PER_MINUTE_BI);
            break;
        case HOUR_TO_SECOND:
            r = intervalToAbsolute(interval, NANOS_PER_HOUR_BI);
            break;
        case MINUTE_TO_SECOND:
            r = intervalToAbsolute(interval, NANOS_PER_MINUTE_BI);
            break;
        default:
            throw new IllegalArgumentException();
        }
        return interval.isNegative() ? r.negate() : r;
    }

    private static BigInteger intervalToAbsolute(ValueInterval interval, BigInteger multiplier,
            BigInteger totalMultiplier) {
        return intervalToAbsolute(interval, multiplier).multiply(totalMultiplier);
    }

    private static BigInteger intervalToAbsolute(ValueInterval interval, BigInteger multiplier) {
        return BigInteger.valueOf(interval.getLeading()).multiply(multiplier)
                .add(BigInteger.valueOf(interval.getRemaining()));
    }

    /**
     * Converts absolute value to an interval value.
     *
     * @param qualifier
     *            the qualifier of interval
     * @param absolute
     *            absolute value in months for year-month intervals, in
     *            nanoseconds for day-time intervals
     * @return the interval value
     */
    public static ValueInterval intervalFromAbsolute(IntervalQualifier qualifier, BigInteger absolute) {
        switch (qualifier) {
        case YEAR:
            return ValueInterval.from(qualifier, absolute.signum() < 0,
                    leadingExact(absolute.divide(MONTHS_PER_YEAR_BI)), 0);
        case MONTH:
            return ValueInterval.from(qualifier, absolute.signum() < 0, leadingExact(absolute), 0);
        case DAY:
            return ValueInterval.from(qualifier, absolute.signum() < 0,
                    leadingExact(absolute.divide(NANOS_PER_DAY_BI)), 0);
        case HOUR:
            return ValueInterval.from(qualifier, absolute.signum() < 0,
                    leadingExact(absolute.divide(NANOS_PER_HOUR_BI)), 0);
        case MINUTE:
            return ValueInterval.from(qualifier, absolute.signum() < 0,
                    leadingExact(absolute.divide(NANOS_PER_MINUTE_BI)), 0);
        case SECOND:
            return intervalFromAbsolute(qualifier, absolute, NANOS_PER_SECOND_BI);
        case YEAR_TO_MONTH:
            return intervalFromAbsolute(qualifier, absolute, MONTHS_PER_YEAR_BI);
        case DAY_TO_HOUR:
            return intervalFromAbsolute(qualifier, absolute.divide(NANOS_PER_HOUR_BI), HOURS_PER_DAY_BI);
        case DAY_TO_MINUTE:
            return intervalFromAbsolute(qualifier, absolute.divide(NANOS_PER_MINUTE_BI), MINUTES_PER_DAY_BI);
        case DAY_TO_SECOND:
            return intervalFromAbsolute(qualifier, absolute, NANOS_PER_DAY_BI);
        case HOUR_TO_MINUTE:
            return intervalFromAbsolute(qualifier, absolute.divide(NANOS_PER_MINUTE_BI), MINUTES_PER_HOUR_BI);
        case HOUR_TO_SECOND:
            return intervalFromAbsolute(qualifier, absolute, NANOS_PER_HOUR_BI);
        case MINUTE_TO_SECOND:
            return intervalFromAbsolute(qualifier, absolute, NANOS_PER_MINUTE_BI);
        default:
            throw new IllegalArgumentException();
        }
    }

    private static ValueInterval intervalFromAbsolute(IntervalQualifier qualifier, BigInteger absolute,
            BigInteger divisor) {
        BigInteger[] dr = absolute.divideAndRemainder(divisor);
        return ValueInterval.from(qualifier, absolute.signum() < 0, leadingExact(dr[0]), Math.abs(dr[1].longValue()));
    }

    private static long leadingExact(BigInteger absolute) {
        if (absolute.compareTo(LEADING_MAX) > 0 || absolute.compareTo(LEADING_MIN) < 0) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, absolute.toString());
        }
        return Math.abs(absolute.longValue());
    }

    /**
     * Ensures that all fields in interval are valid.
     *
     * @param qualifier
     *            qualifier
     * @param negative
     *            whether interval is negative
     * @param leading
     *            value of leading field
     * @param remaining
     *            values of all remaining fields
     * @return fixed value of negative field
     */
    public static boolean validateInterval(IntervalQualifier qualifier, boolean negative, long leading,
            long remaining) {
        if (qualifier == null) {
            throw new NullPointerException();
        }
        if (leading == 0L && remaining == 0L) {
            return false;
        }
        // Upper bound for remaining value (exclusive)
        long bound;
        switch (qualifier) {
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
            bound = 1;
            break;
        case SECOND:
            bound = NANOS_PER_SECOND;
            break;
        case YEAR_TO_MONTH:
            bound = 12;
            break;
        case DAY_TO_HOUR:
            bound = 24;
            break;
        case DAY_TO_MINUTE:
            bound = 24 * 60;
            break;
        case DAY_TO_SECOND:
            bound = NANOS_PER_DAY;
            break;
        case HOUR_TO_MINUTE:
            bound = 60;
            break;
        case HOUR_TO_SECOND:
            bound = NANOS_PER_HOUR;
            break;
        case MINUTE_TO_SECOND:
            bound = NANOS_PER_MINUTE;
            break;
        default:
            throw DbException.getInvalidValueException("interval", qualifier);
        }
        if (leading < 0L || leading >= 1_000_000_000_000_000_000L) {
            throw DbException.getInvalidValueException("interval", Long.toString(leading));
        }
        if (remaining < 0L || remaining >= bound) {
            throw DbException.getInvalidValueException("interval", Long.toString(remaining));
        }
        return negative;
    }

    /**
     * Returns years value of interval, if any.
     *
     * @param qualifier
     *            qualifier
     * @param negative
     *            whether interval is negative
     * @param leading
     *            value of leading field
     * @param remaining
     *            values of all remaining fields
     * @return years, or 0
     */
    public static long yearsFromInterval(IntervalQualifier qualifier, boolean negative, long leading, long remaining) {
        if (qualifier == IntervalQualifier.YEAR || qualifier == IntervalQualifier.YEAR_TO_MONTH) {
            long v = leading;
            if (negative) {
                v = -v;
            }
            return v;
        } else {
            return 0;
        }
    }

    /**
     * Returns months value of interval, if any.
     *
     * @param qualifier
     *            qualifier
     * @param negative
     *            whether interval is negative
     * @param leading
     *            value of leading field
     * @param remaining
     *            values of all remaining fields
     * @return months, or 0
     */
    public static long monthsFromInterval(IntervalQualifier qualifier, boolean negative, long leading, long remaining)
    {
        long v;
        if (qualifier == IntervalQualifier.MONTH) {
            v = leading;
        } else if (qualifier == IntervalQualifier.YEAR_TO_MONTH) {
            v = remaining;
        } else {
            return 0;
        }
        if (negative) {
            v = -v;
        }
        return v;
    }

    /**
     * Returns days value of interval, if any.
     *
     * @param qualifier
     *            qualifier
     * @param negative
     *            whether interval is negative
     * @param leading
     *            value of leading field
     * @param remaining
     *            values of all remaining fields
     * @return days, or 0
     */
    public static long daysFromInterval(IntervalQualifier qualifier, boolean negative, long leading, long remaining) {
        switch (qualifier) {
        case DAY:
        case DAY_TO_HOUR:
        case DAY_TO_MINUTE:
        case DAY_TO_SECOND:
            long v = leading;
            if (negative) {
                v = -v;
            }
            return v;
        default:
            return 0;
        }
    }

    /**
     * Returns hours value of interval, if any.
     *
     * @param qualifier
     *            qualifier
     * @param negative
     *            whether interval is negative
     * @param leading
     *            value of leading field
     * @param remaining
     *            values of all remaining fields
     * @return hours, or 0
     */
    public static long hoursFromInterval(IntervalQualifier qualifier, boolean negative, long leading, long remaining) {
        long v;
        switch (qualifier) {
        case HOUR:
        case HOUR_TO_MINUTE:
        case HOUR_TO_SECOND:
            v = leading;
            break;
        case DAY_TO_HOUR:
            v = remaining;
            break;
        case DAY_TO_MINUTE:
            v = remaining / 60;
            break;
        case DAY_TO_SECOND:
            v = remaining / NANOS_PER_HOUR;
            break;
        default:
            return 0;
        }
        if (negative) {
            v = -v;
        }
        return v;
    }

    /**
     * Returns minutes value of interval, if any.
     *
     * @param qualifier
     *            qualifier
     * @param negative
     *            whether interval is negative
     * @param leading
     *            value of leading field
     * @param remaining
     *            values of all remaining fields
     * @return minutes, or 0
     */
    public static long minutesFromInterval(IntervalQualifier qualifier, boolean negative, long leading,
            long remaining) {
        long v;
        switch (qualifier) {
        case MINUTE:
        case MINUTE_TO_SECOND:
            v = leading;
            break;
        case DAY_TO_MINUTE:
            v = remaining % 60;
            break;
        case DAY_TO_SECOND:
            v = remaining / NANOS_PER_MINUTE % 60;
            break;
        case HOUR_TO_MINUTE:
            v = remaining;
            break;
        case HOUR_TO_SECOND:
            v = remaining / NANOS_PER_MINUTE;
            break;
        default:
            return 0;
        }
        if (negative) {
            v = -v;
        }
        return v;
    }

    /**
     * Returns nanoseconds value of interval, if any.
     *
     * @param qualifier
     *            qualifier
     * @param negative
     *            whether interval is negative
     * @param leading
     *            value of leading field
     * @param remaining
     *            values of all remaining fields
     * @return nanoseconds, or 0
     */
    public static long nanosFromInterval(IntervalQualifier qualifier, boolean negative, long leading, long remaining) {
        long v;
        switch (qualifier) {
        case SECOND:
            v = leading * NANOS_PER_SECOND + remaining;
            break;
        case DAY_TO_SECOND:
        case HOUR_TO_SECOND:
            v = remaining % NANOS_PER_MINUTE;
            break;
        case MINUTE_TO_SECOND:
            v = remaining;
            break;
        default:
            return 0;
        }
        if (negative) {
            v = -v;
        }
        return v;
    }

}
