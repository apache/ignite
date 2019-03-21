/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import static org.h2.util.DateTimeUtils.NANOS_PER_DAY;
import static org.h2.util.DateTimeUtils.NANOS_PER_HOUR;
import static org.h2.util.DateTimeUtils.NANOS_PER_MINUTE;
import static org.h2.util.DateTimeUtils.NANOS_PER_SECOND;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.api.Interval;
import org.h2.api.IntervalQualifier;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.IntervalUtils;

/**
 * Implementation of the INTERVAL data type.
 */
public class ValueInterval extends Value {

    /**
     * The default leading field precision for intervals.
     */
    public static final int DEFAULT_PRECISION = 2;

    /**
     * The maximum leading field precision for intervals.
     */
    public static final int MAXIMUM_PRECISION = 18;

    /**
     * The default scale for intervals with seconds.
     */
    static final int DEFAULT_SCALE = 6;

    /**
     * The maximum scale for intervals with seconds.
     */
    public static final int MAXIMUM_SCALE = 9;

    private final int valueType;

    private TypeInfo type;

    private final boolean negative;

    private final long leading;

    private final long remaining;

    /**
     * Create a ValueInterval instance.
     *
     * @param qualifier
     *            qualifier
     * @param negative
     *            whether interval is negative
     * @param leading
     *            value of leading field
     * @param remaining
     *            values of all remaining fields
     * @return interval value
     */
    public static ValueInterval from(IntervalQualifier qualifier, boolean negative, long leading, long remaining) {
        negative = IntervalUtils.validateInterval(qualifier, negative, leading, remaining);
        return (ValueInterval) Value
                .cache(new ValueInterval(qualifier.ordinal() + INTERVAL_YEAR, negative, leading, remaining));
    }

    /**
     * Returns display size for the specified qualifier, precision and
     * fractional seconds precision.
     *
     * @param type
     *            the value type
     * @param precision
     *            leading field precision
     * @param scale
     *            fractional seconds precision. Ignored if specified type of
     *            interval does not have seconds.
     * @return display size
     */
    public static int getDisplaySize(int type, int precision, int scale) {
        switch (type) {
        case INTERVAL_YEAR:
        case INTERVAL_HOUR:
            // INTERVAL '-11' YEAR
            // INTERVAL '-11' HOUR
            return 17 + precision;
        case INTERVAL_MONTH:
            // INTERVAL '-11' MONTH
            return 18 + precision;
        case INTERVAL_DAY:
            // INTERVAL '-11' DAY
            return 16 + precision;
        case INTERVAL_MINUTE:
            // INTERVAL '-11' MINUTE
            return 19 + precision;
        case INTERVAL_SECOND:
            // INTERVAL '-11' SECOND
            // INTERVAL '-11.999999' SECOND
            return scale > 0 ? 20 + precision + scale : 19 + precision;
        case INTERVAL_YEAR_TO_MONTH:
            // INTERVAL '-11-11' YEAR TO MONTH
            return 29 + precision;
        case INTERVAL_DAY_TO_HOUR:
            // INTERVAL '-11 23' DAY TO HOUR
            return 27 + precision;
        case INTERVAL_DAY_TO_MINUTE:
            // INTERVAL '-11 23:59' DAY TO MINUTE
            return 32 + precision;
        case INTERVAL_DAY_TO_SECOND:
            // INTERVAL '-11 23:59.59' DAY TO SECOND
            // INTERVAL '-11 23:59.59.999999' DAY TO SECOND
            return scale > 0 ? 36 + precision + scale : 35 + precision;
        case INTERVAL_HOUR_TO_MINUTE:
            // INTERVAL '-11:59' HOUR TO MINUTE
            return 30 + precision;
        case INTERVAL_HOUR_TO_SECOND:
            // INTERVAL '-11:59:59' HOUR TO SECOND
            // INTERVAL '-11:59:59.999999' HOUR TO SECOND
            return scale > 0 ? 34 + precision + scale : 33 + precision;
        case INTERVAL_MINUTE_TO_SECOND:
            // INTERVAL '-11:59' MINUTE TO SECOND
            // INTERVAL '-11:59.999999' MINUTE TO SECOND
            return scale > 0 ? 33 + precision + scale : 32 + precision;
        default:
            throw DbException.getUnsupportedException(Integer.toString(type));
        }
    }

    private ValueInterval(int type, boolean negative, long leading, long remaining) {
        this.valueType = type;
        this.negative = negative;
        this.leading = leading;
        this.remaining = remaining;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        return IntervalUtils.appendInterval(builder, getQualifier(), negative, leading, remaining);
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            long l = leading;
            int precision = 0;
            while (l > 0) {
                precision++;
                l /= 10;
            }
            if (precision == 0) {
                precision = 1;
            }
            this.type = type = new TypeInfo(valueType, precision, 0,
                    getDisplaySize(valueType, MAXIMUM_PRECISION, MAXIMUM_SCALE), null);
        }
        return type;
    }

    @Override
    public int getValueType() {
        return valueType;
    }

    @Override
    public int getMemory() {
        // Java 11 with -XX:-UseCompressedOops
        return 48;
    }

    @Override
    public Value convertScale(boolean onlyToSmallerScale, int targetScale) {
        if (targetScale >= MAXIMUM_SCALE) {
            return this;
        }
        if (targetScale < 0) {
            throw DbException.getInvalidValueException("scale", targetScale);
        }
        IntervalQualifier qualifier = getQualifier();
        if (!qualifier.hasSeconds()) {
            return this;
        }
        long r = DateTimeUtils.convertScale(remaining, targetScale);
        if (r == remaining) {
            return this;
        }
        long l = leading;
        switch (valueType) {
        case INTERVAL_SECOND:
            if (r >= NANOS_PER_SECOND) {
                l++;
                r -= NANOS_PER_SECOND;
            }
            break;
        case INTERVAL_DAY_TO_SECOND:
            if (r >= NANOS_PER_DAY) {
                l++;
                r -= NANOS_PER_DAY;
            }
            break;
        case INTERVAL_HOUR_TO_SECOND:
            if (r >= NANOS_PER_HOUR) {
                l++;
                r -= NANOS_PER_HOUR;
            }
            break;
        case INTERVAL_MINUTE_TO_SECOND:
            if (r >= NANOS_PER_MINUTE) {
                l++;
                r -= NANOS_PER_MINUTE;
            }
            break;
        }
        return from(qualifier, negative, l, r);
    }

    @Override
    public String getString() {
        return IntervalUtils.appendInterval(new StringBuilder(), getQualifier(), negative, leading, remaining)
                .toString();
    }

    @Override
    public Object getObject() {
        return new Interval(getQualifier(), negative, leading, remaining);
    }

    /**
     * Returns the interval qualifier.
     *
     * @return the interval qualifier
     */
    public IntervalQualifier getQualifier() {
        return IntervalQualifier.valueOf(valueType - INTERVAL_YEAR);
    }

    /**
     * Returns where the interval is negative.
     *
     * @return where the interval is negative
     */
    public boolean isNegative() {
        return negative;
    }

    /**
     * Returns value of leading field of this interval. For {@code SECOND}
     * intervals returns integer part of seconds.
     *
     * @return value of leading field
     */
    public long getLeading() {
        return leading;
    }

    /**
     * Returns combined value of remaining fields of this interval. For
     * {@code SECOND} intervals returns nanoseconds.
     *
     * @return combined value of remaining fields
     */
    public long getRemaining() {
        return remaining;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setString(parameterIndex, getString());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + valueType;
        result = prime * result + (negative ? 1231 : 1237);
        result = prime * result + (int) (leading ^ leading >>> 32);
        result = prime * result + (int) (remaining ^ remaining >>> 32);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ValueInterval)) {
            return false;
        }
        ValueInterval other = (ValueInterval) obj;
        return valueType == other.valueType && negative == other.negative && leading == other.leading
                && remaining == other.remaining;
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode) {
        ValueInterval other = (ValueInterval) v;
        if (negative != other.negative) {
            return negative ? -1 : 1;
        }
        int cmp = Long.compare(leading, other.leading);
        if (cmp == 0) {
            cmp = Long.compare(remaining, other.remaining);
        }
        return negative ? -cmp : cmp;
    }

    @Override
    public int getSignum() {
        return negative ? -1 : leading == 0L && remaining == 0L ? 0 : 1;
    }

    @Override
    public Value add(Value v) {
        return IntervalUtils.intervalFromAbsolute(getQualifier(),
                IntervalUtils.intervalToAbsolute(this).add(IntervalUtils.intervalToAbsolute((ValueInterval) v)));
    }

    @Override
    public Value subtract(Value v) {
        return IntervalUtils.intervalFromAbsolute(getQualifier(),
                IntervalUtils.intervalToAbsolute(this).subtract(IntervalUtils.intervalToAbsolute((ValueInterval) v)));
    }

    @Override
    public Value negate() {
        if (leading == 0L && remaining == 0L) {
            return this;
        }
        return Value.cache(new ValueInterval(valueType, !negative, leading, remaining));
    }

}
