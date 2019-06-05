/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

import java.io.Serializable;
import org.h2.util.DateTimeUtils;

/**
 * How we expose "TIMESTAMP WITH TIME ZONE" in our ResultSets.
 */
public class TimestampWithTimeZone implements Serializable, Cloneable {

    /**
     * The serial version UID.
     */
    private static final long serialVersionUID = 4413229090646777107L;

    /**
     * A bit field with bits for the year, month, and day (see DateTimeUtils for
     * encoding)
     */
    private final long dateValue;
    /**
     * The nanoseconds since midnight.
     */
    private final long timeNanos;
    /**
     * Time zone offset from UTC in minutes, range of -12hours to +12hours
     */
    private final short timeZoneOffsetMins;

    public TimestampWithTimeZone(long dateValue, long timeNanos, short timeZoneOffsetMins) {
        this.dateValue = dateValue;
        this.timeNanos = timeNanos;
        this.timeZoneOffsetMins = timeZoneOffsetMins;
    }

    /**
     * @return the year-month-day bit field
     */
    public long getYMD() {
        return dateValue;
    }

    /**
     * Gets the year.
     *
     * <p>The year is in the specified time zone and not UTC. So for
     * {@code 2015-12-31 19:00:00.00-10:00} the value returned
     * will be {@code 2015} even though in UTC the year is {@code 2016}.</p>
     *
     * @return the year
     */
    public int getYear() {
        return DateTimeUtils.yearFromDateValue(dateValue);
    }

    /**
     * Gets the month 1-based.
     *
     * <p>The month is in the specified time zone and not UTC. So for
     * {@code 2015-12-31 19:00:00.00-10:00} the value returned
     * is {@code 12} even though in UTC the month is {@code 1}.</p>
     *
     * @return the month
     */
    public int getMonth() {
        return DateTimeUtils.monthFromDateValue(dateValue);
    }

    /**
     * Gets the day of month 1-based.
     *
     * <p>The day of month is in the specified time zone and not UTC. So for
     * {@code 2015-12-31 19:00:00.00-10:00} the value returned
     * is {@code 31} even though in UTC the day of month is {@code 1}.</p>
     *
     * @return the day of month
     */
    public int getDay() {
        return DateTimeUtils.dayFromDateValue(dateValue);
    }

    /**
     * Gets the nanoseconds since midnight.
     *
     * <p>The nanoseconds are relative to midnight in the specified
     * time zone. So for {@code 2016-09-24 00:00:00.000000001-00:01} the
     * value returned is {@code 1} even though {@code 60000000001}
     * nanoseconds have passed since midnight in UTC.</p>
     *
     * @return the nanoseconds since midnight
     */
    public long getNanosSinceMidnight() {
        return timeNanos;
    }

    /**
     * The time zone offset in minutes.
     *
     * @return the offset
     */
    public short getTimeZoneOffsetMins() {
        return timeZoneOffsetMins;
    }

    @Override
    public String toString() {
        return DateTimeUtils.timestampTimeZoneToString(dateValue, timeNanos, timeZoneOffsetMins);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (dateValue ^ (dateValue >>> 32));
        result = prime * result + (int) (timeNanos ^ (timeNanos >>> 32));
        result = prime * result + timeZoneOffsetMins;
        return result;
    }

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
        TimestampWithTimeZone other = (TimestampWithTimeZone) obj;
        if (dateValue != other.dateValue) {
            return false;
        }
        if (timeNanos != other.timeNanos) {
            return false;
        }
        if (timeZoneOffsetMins != other.timeZoneOffsetMins) {
            return false;
        }
        return true;
    }

}
