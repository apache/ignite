/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cache.query.index.sorted.keys;

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
    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;

    /**
     * The number of nanoseconds per day.
     */
    public static final long NANOS_PER_DAY = MILLIS_PER_DAY * 1_000_000;

    /** Shift year. */
    private static final int SHIFT_YEAR = 9;

    /** Shift month. */
    private static final int SHIFT_MONTH = 5;

    /** Min date value. */
    public static final long MIN_DATE_VALUE = (-999_999_999L << SHIFT_YEAR) + (1 << SHIFT_MONTH) + 1;

    /** Max date value. */
    public static final long MAX_DATE_VALUE = (999_999_999L << SHIFT_YEAR) + (12 << SHIFT_MONTH) + 31;

    /**
     * Offsets of month within a year, starting with March, April,...
     */
    private static final int[] DAYS_OFFSET = { 0, 31, 61, 92, 122, 153, 184,
        214, 245, 275, 306, 337, 366 };

    /**
     * Cached local time zone.
     */
    private static volatile TimeZone timeZone;

    /**
     * Returns local time zone.
     *
     * @return local time zone
     */
    private static TimeZone getTimeZone() {
        TimeZone tz = timeZone;
        if (tz == null)
            timeZone = tz = TimeZone.getDefault();

        return tz;
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
        if (ms < 0 && (absoluteDay * MILLIS_PER_DAY != ms))
            absoluteDay--;

        return dateValueFromAbsoluteDay(absoluteDay);
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
        if (ms < 0 && (absoluteDay * MILLIS_PER_DAY != ms))
            absoluteDay--;

        return (ms - absoluteDay * MILLIS_PER_DAY) * 1_000_000;
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
