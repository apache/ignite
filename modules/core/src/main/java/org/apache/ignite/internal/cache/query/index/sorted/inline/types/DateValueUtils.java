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

package org.apache.ignite.internal.cache.query.index.sorted.inline.types;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * DateValue is a representation of a date in bit form:
 *
 * dv = (year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day.
 */
public class DateValueUtils {
    /** Calendar with UTC time zone instance. */
    private static final ThreadLocal<Calendar> UTC_CALENDAR = ThreadLocal.withInitial(
        () -> Calendar.getInstance(TimeZone.getTimeZone("UTC")));

    /** Cached default time zone. */
    private static final TimeZone DEFAULT_TZ = TimeZone.getDefault();

    /** Forbid instantiation of this class. Just hold constants there. */
    private DateValueUtils() {}

    /** */
    private static final int SHIFT_YEAR = 9;

    /** */
    private static final int SHIFT_MONTH = 5;

    /** */
    private static final long MONTH_MASK = ~(-1L << (SHIFT_YEAR - SHIFT_MONTH));

    /** */
    private static final long DAY_MASK = ~(-1L << SHIFT_MONTH);

    /** Min date value. */
    public static final long MIN_DATE_VALUE = (-999_999_999L << SHIFT_YEAR) + (1 << SHIFT_MONTH) + 1;

    /** Max date value. */
    public static final long MAX_DATE_VALUE = (999_999_999L << SHIFT_YEAR) + (12 << SHIFT_MONTH) + 31;

    /** The number of milliseconds per day. */
    public static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;

    /** The number of nanoseconds per day. */
    public static final long NANOS_PER_DAY = MILLIS_PER_DAY * 1_000_000;

    /**
     * Extract the year from a date value.
     */
    private static int yearFromDateValue(long dateVal) {
        return (int)(dateVal >>> SHIFT_YEAR);
    }

    /**
     * Extract the month from a date value.
     */
    private static int monthFromDateValue(long dateVal) {
        return (int)((dateVal >>> SHIFT_MONTH) & MONTH_MASK);
    }

    /**
     * Extract the day of month from a date value.
     */
    private static int dayFromDateValue(long dateVal) {
        return (int)(dateVal & DAY_MASK);
    }

    /**
     * Construct date value from components.
     */
    public static long dateValue(int year, int month, int day) {
        return ((long)year << SHIFT_YEAR) | month << SHIFT_MONTH | day;
    }

    /**
     * Convert date value to epoch milliseconds.
     */
    public static long millisFromDateValue(long dateVal) {
        Calendar cal = UTC_CALENDAR.get();

        cal.clear();

        cal.set(yearFromDateValue(dateVal), monthFromDateValue(dateVal) - 1, dayFromDateValue(dateVal));

        return cal.getTimeInMillis();
    }

    /**
     * Convert epoch milliseconds to date value.
     */
    public static long dateValueFromMillis(long millis) {
        Calendar cal = UTC_CALENDAR.get();

        cal.setTimeInMillis(millis);

        return dateValue(
            cal.get(Calendar.ERA) == GregorianCalendar.BC ? 1 - cal.get(Calendar.YEAR) : cal.get(Calendar.YEAR),
            cal.get(Calendar.MONTH) + 1,
            cal.get(Calendar.DAY_OF_MONTH)
        );
    }

    /**
     * Convert millis in default time zone to UTC millis.
     */
    public static long utcMillisFromDefaultTz(long tzMillis) {
        return tzMillis + DEFAULT_TZ.getOffset(tzMillis);
    }

    /**
     * Convert millis in UTC to default time zone millis.
     */
    public static long defaultTzMillisFromUtc(long utcMillis) {
        // Taking into account DST, offset can be changed after converting from UTC to time-zone.
        return utcMillis - DEFAULT_TZ.getOffset(utcMillis - DEFAULT_TZ.getOffset(utcMillis));

    }

    /** */
    public static Timestamp convertToTimestamp(LocalDateTime locDateTime) {
        LocalDate locDate = locDateTime.toLocalDate();
        LocalTime locTime = locDateTime.toLocalTime();

        long dateVal = dateValue(locDate.getYear(), locDate.getMonthValue(), locDate.getDayOfMonth());
        long millis = millisFromDateValue(dateVal) + TimeUnit.NANOSECONDS.toMillis(locTime.toNanoOfDay());
        long nanos = locTime.toNanoOfDay() % 1_000_000_000L;

        Timestamp res = new Timestamp(defaultTzMillisFromUtc(millis));
        res.setNanos((int)nanos);

        return res;
    }

    /** */
    public static Time convertToSqlTime(LocalTime locTime) {
        long millis = TimeUnit.NANOSECONDS.toMillis(locTime.toNanoOfDay());

        return new Time(defaultTzMillisFromUtc(millis));
    }

    /** */
    public static Date convertToSqlDate(LocalDate locDate) {
        long dateVal = dateValue(locDate.getYear(), locDate.getMonthValue(), locDate.getDayOfMonth());
        long millis = millisFromDateValue(dateVal);

        return new Date(defaultTzMillisFromUtc(millis));
    }
}
