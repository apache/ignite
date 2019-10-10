/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.h2.api.ErrorCode;
import org.h2.api.TimestampWithTimeZone;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;

/**
 * Implementation of the TIMESTAMP WITH TIME ZONE data type.
 *
 * @see <a href="https://en.wikipedia.org/wiki/ISO_8601#Time_zone_designators">
 *      ISO 8601 Time zone designators</a>
 */
public class ValueTimestampTimeZone extends Value {

    /**
     * The default precision and display size of the textual representation of a timestamp.
     * Example: 2001-01-01 23:59:59.123456+10:00
     */
    public static final int DEFAULT_PRECISION = 32;

    /**
     * The maximum precision and display size of the textual representation of a timestamp.
     * Example: 2001-01-01 23:59:59.123456789+10:00
     */
    public static final int MAXIMUM_PRECISION = 35;

    /**
     * The default scale for timestamps.
     */
    static final int DEFAULT_SCALE = ValueTimestamp.DEFAULT_SCALE;

    /**
     * The default scale for timestamps.
     */
    static final int MAXIMUM_SCALE = ValueTimestamp.MAXIMUM_SCALE;

    /**
     * Get display size for the specified scale.
     *
     * @param scale scale
     * @return display size
     */
    public static int getDisplaySize(int scale) {
        return scale == 0 ? 25 : 26 + scale;
    }

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
     * Time zone offset from UTC in minutes, range of -18 hours to +18 hours. This
     * range is compatible with OffsetDateTime from JSR-310.
     */
    private final short timeZoneOffsetMins;

    private ValueTimestampTimeZone(long dateValue, long timeNanos,
            short timeZoneOffsetMins) {
        if (timeNanos < 0 || timeNanos >= DateTimeUtils.NANOS_PER_DAY) {
            throw new IllegalArgumentException(
                    "timeNanos out of range " + timeNanos);
        }
        /*
         * Some current and historic time zones have offsets larger than 12 hours.
         * JSR-310 determines 18 hours as maximum possible offset in both directions, so
         * we use this limit too for compatibility.
         */
        if (timeZoneOffsetMins < (-18 * 60)
                || timeZoneOffsetMins > (18 * 60)) {
            throw new IllegalArgumentException(
                    "timeZoneOffsetMins out of range " + timeZoneOffsetMins);
        }
        this.dateValue = dateValue;
        this.timeNanos = timeNanos;
        this.timeZoneOffsetMins = timeZoneOffsetMins;
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value, a bit field with bits for the year,
     *            month, and day
     * @param timeNanos the nanoseconds since midnight
     * @param timeZoneOffsetMins the timezone offset in minutes
     * @return the value
     */
    public static ValueTimestampTimeZone fromDateValueAndNanos(long dateValue,
            long timeNanos, short timeZoneOffsetMins) {
        return (ValueTimestampTimeZone) Value.cache(new ValueTimestampTimeZone(
                dateValue, timeNanos, timeZoneOffsetMins));
    }

    /**
     * Get or create a timestamp value for the given timestamp.
     *
     * @param timestamp the timestamp
     * @return the value
     */
    public static ValueTimestampTimeZone get(TimestampWithTimeZone timestamp) {
        return fromDateValueAndNanos(timestamp.getYMD(),
                timestamp.getNanosSinceMidnight(),
                timestamp.getTimeZoneOffsetMins());
    }

    /**
     * Parse a string to a ValueTimestamp. This method supports the format
     * +/-year-month-day hour:minute:seconds.fractional and an optional timezone
     * part.
     *
     * @param s the string to parse
     * @return the date
     */
    public static ValueTimestampTimeZone parse(String s) {
        try {
            return (ValueTimestampTimeZone) DateTimeUtils.parseTimestamp(s, null, true);
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e,
                    "TIMESTAMP WITH TIME ZONE", s);
        }
    }

    /**
     * A bit field with bits for the year, month, and day (see DateTimeUtils for
     * encoding).
     *
     * @return the data value
     */
    public long getDateValue() {
        return dateValue;
    }

    /**
     * The nanoseconds since midnight.
     *
     * @return the nanoseconds
     */
    public long getTimeNanos() {
        return timeNanos;
    }

    /**
     * The timezone offset in minutes.
     *
     * @return the offset
     */
    public short getTimeZoneOffsetMins() {
        return timeZoneOffsetMins;
    }

    @Override
    public Timestamp getTimestamp() {
        return DateTimeUtils.convertTimestampTimeZoneToTimestamp(dateValue, timeNanos, timeZoneOffsetMins);
    }

    @Override
    public int getType() {
        return Value.TIMESTAMP_TZ;
    }

    @Override
    public String getString() {
        return DateTimeUtils.timestampTimeZoneToString(dateValue, timeNanos, timeZoneOffsetMins);
    }

    @Override
    public String getSQL() {
        return "TIMESTAMP WITH TIME ZONE '" + getString() + "'";
    }

    @Override
    public long getPrecision() {
        return MAXIMUM_PRECISION;
    }

    @Override
    public int getScale() {
        return MAXIMUM_SCALE;
    }

    @Override
    public int getDisplaySize() {
        return MAXIMUM_PRECISION;
    }

    @Override
    public boolean checkPrecision(long precision) {
        // TIMESTAMP WITH TIME ZONE data type does not have precision parameter
        return true;
    }

    @Override
    public Value convertScale(boolean onlyToSmallerScale, int targetScale) {
        if (targetScale >= MAXIMUM_SCALE) {
            return this;
        }
        if (targetScale < 0) {
            throw DbException.getInvalidValueException("scale", targetScale);
        }
        long n = timeNanos;
        long n2 = DateTimeUtils.convertScale(n, targetScale);
        if (n2 == n) {
            return this;
        }
        long dv = dateValue;
        if (n2 >= DateTimeUtils.NANOS_PER_DAY) {
            n2 -= DateTimeUtils.NANOS_PER_DAY;
            dv = DateTimeUtils.incrementDateValue(dv);
        }
        return fromDateValueAndNanos(dv, n2, timeZoneOffsetMins);
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueTimestampTimeZone t = (ValueTimestampTimeZone) o;
        // Maximum time zone offset is +/-18 hours so difference in days between local
        // and UTC cannot be more than one day
        long dateValueA = dateValue;
        long timeA = timeNanos - timeZoneOffsetMins * 60_000_000_000L;
        if (timeA < 0) {
            timeA += DateTimeUtils.NANOS_PER_DAY;
            dateValueA = DateTimeUtils.decrementDateValue(dateValueA);
        } else if (timeA >= DateTimeUtils.NANOS_PER_DAY) {
            timeA -= DateTimeUtils.NANOS_PER_DAY;
            dateValueA = DateTimeUtils.incrementDateValue(dateValueA);
        }
        long dateValueB = t.dateValue;
        long timeB = t.timeNanos - t.timeZoneOffsetMins * 60_000_000_000L;
        if (timeB < 0) {
            timeB += DateTimeUtils.NANOS_PER_DAY;
            dateValueB = DateTimeUtils.decrementDateValue(dateValueB);
        } else if (timeB >= DateTimeUtils.NANOS_PER_DAY) {
            timeB -= DateTimeUtils.NANOS_PER_DAY;
            dateValueB = DateTimeUtils.incrementDateValue(dateValueB);
        }
        int cmp = Long.compare(dateValueA, dateValueB);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(timeA, timeB);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof ValueTimestampTimeZone)) {
            return false;
        }
        ValueTimestampTimeZone x = (ValueTimestampTimeZone) other;
        return dateValue == x.dateValue && timeNanos == x.timeNanos
                && timeZoneOffsetMins == x.timeZoneOffsetMins;
    }

    @Override
    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32) ^ timeNanos
                ^ (timeNanos >>> 32) ^ timeZoneOffsetMins);
    }

    @Override
    public Object getObject() {
        return new TimestampWithTimeZone(dateValue, timeNanos,
                timeZoneOffsetMins);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setString(parameterIndex, getString());
    }

    @Override
    public Value add(Value v) {
        throw DbException.getUnsupportedException(
                "manipulating TIMESTAMP WITH TIME ZONE values is unsupported");
    }

    @Override
    public Value subtract(Value v) {
        throw DbException.getUnsupportedException(
                "manipulating TIMESTAMP WITH TIME ZONE values is unsupported");
    }

}
