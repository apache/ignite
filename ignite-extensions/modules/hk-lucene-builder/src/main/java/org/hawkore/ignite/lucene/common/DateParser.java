/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.UUID;

import org.hawkore.ignite.lucene.IndexException;

/**
 * Unified class for parsing {@link Date}s from {@link Object}s.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class DateParser {

    // A grand day! millis at 00:00:00.000 15 Oct 1582.
    private static final long START_EPOCH = -12219292800000L;
    /** The default date pattern for parsing {@code String}s and truncations. */
    public static final String DEFAULT_PATTERN = "yyyy/MM/dd HH:mm:ss.SSS Z";
    public static final ZoneId DEFAULT_TIME_ZONE = ZoneId.systemDefault();
    /** The {@link SimpleDateFormat} pattern. */
    public final String pattern;
    /** The thread safe date format. */
    private final ThreadLocal<DateFormat> formatter;

    /**
     * Constructor with pattern.
     *
     * @param pattern
     *     the {@link SimpleDateFormat} pattern
     */
    public DateParser(String pattern) {
        this.pattern = pattern == null ? DEFAULT_PATTERN : pattern;
        formatter = formatter(this.pattern);
    }

    private static ThreadLocal<DateFormat> formatter(final String pattern) {
        new SimpleDateFormat(pattern);
        ThreadLocal<DateFormat> formatter = ThreadLocal.withInitial(() -> new SimpleDateFormat(pattern));
        formatter.get().setLenient(false);
        return formatter;
    }

    /**
     * Returns the {@link Date} represented by the specified {@link Object}, or
     * {@code null} if the specified {@link Object} is {@code null}.
     *
     * @param value
     *     the {@link Object} to be parsed
     * @param <K>
     *     the type of the value to be parsed
     * @return the parsed {@link Date}
     */
    public final <K> Date parse(K data) {

        if (data == null) {
            return null;
        }

        try {
            Object value = data;

            // add support to Java 8 Temporal classes
            if (value instanceof LocalDate)
                value = convertToDate((LocalDate)value);
            else if (value instanceof LocalDateTime)
                value = convertToDate((LocalDateTime)value);
            else if (value instanceof OffsetDateTime)
                value = convertToDate((OffsetDateTime)value);
            else if (value instanceof ZonedDateTime)
                value = convertToDate((ZonedDateTime)value);
            else if (value instanceof Instant)
                value = convertToDate((Instant)value);
            else if (value instanceof OffsetTime)
                value = convertToDate((OffsetTime)value);
            else if (value instanceof LocalTime)
                value = convertToDate((LocalTime)value);

            if (value instanceof Date) {
                Date date = (Date)value;
                if (date.getTime() == Long.MAX_VALUE || date.getTime() == Long.MIN_VALUE) {
                    return date;
                } else {
                    String string = formatter.get().format(date);
                    return formatter.get().parse(string);
                }
            } else if (value instanceof UUID) {
                long timestamp = unixTimestamp((UUID)value);
                Date date = new Date(timestamp);
                return formatter.get().parse(formatter.get().format(date));
            } else if (Number.class.isAssignableFrom(value.getClass())) {
                Long number = ((Number)value).longValue();
                try {
                    return formatter.get().parse(number.toString());
                } catch (Exception e) {
                    // Allocates a Date object and initializes it to represent
                    // the specified number of milliseconds since the standard
                    // base time known as "the epoch", namely January 1, 1970,
                    // 00:00:00 GMT.
                    Date date = new Date(number);
                    //fix reduced date by formatter near January 1, 1970, 00:00:00 GMT, may produce negative millis
                    long millis = Math.max(0, formatter.get().parse(formatter.get().format(date)).getTime());
                    return new Date(millis);
                }
            } else {
                return formatter.get().parse(value.toString());
            }
        } catch (Exception e) {
            throw new IndexException(e, "Error parsing {} with value '{}' using date pattern {}",
                data.getClass().getSimpleName(), data, pattern);
        }
    }

    public String toString(Date date) {
        return formatter.get().format(date);
    }

    public String toString() {
        return pattern;
    }

    /**
     * @param uuid
     * @return milliseconds since Unix epoch
     */
    public static long unixTimestamp(UUID uuid) {
        return (uuid.timestamp() / 10000) + START_EPOCH;
    }

    /**
     * @param localDate
     * @return Date
     */
    public static Date convertToDate(LocalDate localDate) {
        return Date.from(localDate.atStartOfDay(
            DEFAULT_TIME_ZONE).toInstant());
    }

    /**
     * @param localDateTime
     * @return Date
     */
    public static Date convertToDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(
            DEFAULT_TIME_ZONE).toInstant());
    }

    /**
     * @param offsetDateTime
     * @return Date
     */
    public static Date convertToDate(OffsetDateTime offsetDateTime) {
        return Date.from(offsetDateTime.toLocalDateTime().toInstant(ZoneOffset.of(offsetDateTime.getOffset().getId())));
    }

    /**
     * @param zonedDateTime
     * @return Date
     */
    public static Date convertToDate(ZonedDateTime zonedDateTime) {
        return Date.from(zonedDateTime.toInstant());
    }

    /**
     * @param instant
     * @return Date
     */
    public static Date convertToDate(Instant instant) {
        return Date.from(instant);
    }

    /**
     * @param offsetTime
     * @return Date
     */
    public static Date convertToDate(OffsetTime offsetTime) {

        return Date.from(offsetTime.atDate(LocalDate.ofEpochDay(0)).toInstant());
    }

    /**
     * @param localTime
     * @return Date
     */
    public static Date convertToDate(LocalTime localTime) {
        return Date.from(
            localTime.atDate(LocalDate.ofEpochDay(0)).toInstant(DEFAULT_TIME_ZONE.getRules().getOffset(Instant.now())));
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((pattern == null) ? 0 : pattern.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DateParser other = (DateParser)obj;
        if (pattern == null) {
            if (other.pattern != null) {
                return false;
            }
        } else if (!pattern.equals(other.pattern)) {
            return false;
        }
        return true;
    }

    // for the curious, here is how I generated START_EPOCH
    // Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"));
    // c.set(Calendar.YEAR, 1582);
    // c.set(Calendar.MONTH, Calendar.OCTOBER);
    // c.set(Calendar.DAY_OF_MONTH, 15);
    // c.set(Calendar.HOUR_OF_DAY, 0);
    // c.set(Calendar.MINUTE, 0);
    // c.set(Calendar.SECOND, 0);
    // c.set(Calendar.MILLISECOND, 0);
    // long START_EPOCH = c.getTimeInMillis();
}
