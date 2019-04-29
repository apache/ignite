/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.api;

/**
 * Interval qualifier.
 */
public enum IntervalQualifier {

    /**
     * {@code YEAR}
     */
    YEAR,

    /**
     * {@code MONTH}
     */
    MONTH,

    /**
     * {@code DAY}
     */
    DAY,

    /**
     * {@code HOUR}
     */
    HOUR,

    /**
     * {@code MINUTE}
     */
    MINUTE,

    /**
     * {@code SECOND}
     */
    SECOND,

    /**
     * {@code YEAR TO MONTH}
     */
    YEAR_TO_MONTH,

    /**
     * {@code DAY TO HOUR}
     */
    DAY_TO_HOUR,

    /**
     * {@code DAY TO MINUTE}
     */
    DAY_TO_MINUTE,

    /**
     * {@code DAY TO SECOND}
     */
    DAY_TO_SECOND,

    /**
     * {@code HOUR TO MINUTE}
     */
    HOUR_TO_MINUTE,

    /**
     * {@code HOUR TO SECOND}
     */
    HOUR_TO_SECOND,

    /**
     * {@code MINUTE TO SECOND}
     */
    MINUTE_TO_SECOND;

    private final String string;

    /**
     * Returns the interval qualifier with the specified ordinal value.
     *
     * @param ordinal
     *            Java ordinal value (0-based)
     * @return interval qualifier with the specified ordinal value
     */
    public static IntervalQualifier valueOf(int ordinal) {
        switch (ordinal) {
        case 0:
            return YEAR;
        case 1:
            return MONTH;
        case 2:
            return DAY;
        case 3:
            return HOUR;
        case 4:
            return MINUTE;
        case 5:
            return SECOND;
        case 6:
            return YEAR_TO_MONTH;
        case 7:
            return DAY_TO_HOUR;
        case 8:
            return DAY_TO_MINUTE;
        case 9:
            return DAY_TO_SECOND;
        case 10:
            return HOUR_TO_MINUTE;
        case 11:
            return HOUR_TO_SECOND;
        case 12:
            return MINUTE_TO_SECOND;
        default:
            throw new IllegalArgumentException();
        }
    }

    private IntervalQualifier() {
        string = name().replace('_', ' ').intern();
    }

    /**
     * Returns whether interval with this qualifier is a year-month interval.
     *
     * @return whether interval with this qualifier is a year-month interval
     */
    public boolean isYearMonth() {
        return this == YEAR || this == MONTH || this == YEAR_TO_MONTH;
    }

    /**
     * Returns whether interval with this qualifier is a day-time interval.
     *
     * @return whether interval with this qualifier is a day-time interval
     */
    public boolean isDayTime() {
        return !isYearMonth();
    }

    /**
     * Returns whether interval with this qualifier has years.
     *
     * @return whether interval with this qualifier has years
     */
    public boolean hasYears() {
        return this == YEAR || this == YEAR_TO_MONTH;
    }

    /**
     * Returns whether interval with this qualifier has months.
     *
     * @return whether interval with this qualifier has months
     */
    public boolean hasMonths() {
        return this == MONTH || this == YEAR_TO_MONTH;
    }

    /**
     * Returns whether interval with this qualifier has days.
     *
     * @return whether interval with this qualifier has days
     */
    public boolean hasDays() {
        switch (this) {
        case DAY:
        case DAY_TO_HOUR:
        case DAY_TO_MINUTE:
        case DAY_TO_SECOND:
            return true;
        default:
            return false;
        }
    }

    /**
     * Returns whether interval with this qualifier has hours.
     *
     * @return whether interval with this qualifier has hours
     */
    public boolean hasHours() {
        switch (this) {
        case HOUR:
        case DAY_TO_HOUR:
        case DAY_TO_MINUTE:
        case DAY_TO_SECOND:
        case HOUR_TO_MINUTE:
        case HOUR_TO_SECOND:
            return true;
        default:
            return false;
        }
    }

    /**
     * Returns whether interval with this qualifier has minutes.
     *
     * @return whether interval with this qualifier has minutes
     */
    public boolean hasMinutes() {
        switch (this) {
        case MINUTE:
        case DAY_TO_MINUTE:
        case DAY_TO_SECOND:
        case HOUR_TO_MINUTE:
        case HOUR_TO_SECOND:
        case MINUTE_TO_SECOND:
            return true;
        default:
            return false;
        }
    }

    /**
     * Returns whether interval with this qualifier has seconds.
     *
     * @return whether interval with this qualifier has seconds
     */
    public boolean hasSeconds() {
        switch (this) {
        case SECOND:
        case DAY_TO_SECOND:
        case HOUR_TO_SECOND:
        case MINUTE_TO_SECOND:
            return true;
        default:
            return false;
        }
    }

    /**
     * Returns whether interval with this qualifier has multiple fields.
     *
     * @return whether interval with this qualifier has multiple fields
     */
    public boolean hasMultipleFields() {
        return ordinal() > 5;
    }

    @Override
    public String toString() {
        return string;
    }

    /**
     * Returns full type name.
     *
     * @param precision precision, or {@code -1}
     * @param scale fractional seconds precision, or {@code -1}
     * @return full type name
     */
    public String getTypeName(int precision, int scale) {
        StringBuilder b = new StringBuilder("INTERVAL ");
        switch (this) {
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
            b.append(string);
            if (precision > 0) {
                b.append('(').append(precision).append(')');
            }
            break;
        case SECOND:
            b.append(string);
            if (precision > 0 || scale >= 0) {
                b.append('(').append(precision > 0 ? precision : 2);
                if (scale >= 0) {
                    b.append(", ").append(scale);
                }
                b.append(')');
            }
            break;
        case YEAR_TO_MONTH:
            b.append("YEAR");
            if (precision > 0) {
                b.append('(').append(precision).append(')');
            }
            b.append(" TO MONTH");
            break;
        case DAY_TO_HOUR:
            b.append("DAY");
            if (precision > 0) {
                b.append('(').append(precision).append(')');
            }
            b.append(" TO HOUR");
            break;
        case DAY_TO_MINUTE:
            b.append("DAY");
            if (precision > 0) {
                b.append('(').append(precision).append(')');
            }
            b.append(" TO MINUTE");
            break;
        case DAY_TO_SECOND:
            b.append("DAY");
            if (precision > 0) {
                b.append('(').append(precision).append(')');
            }
            b.append(" TO SECOND");
            if (scale >= 0) {
                b.append('(').append(scale).append(')');
            }
            break;
        case HOUR_TO_MINUTE:
            b.append("HOUR");
            if (precision > 0) {
                b.append('(').append(precision).append(')');
            }
            b.append(" TO MINUTE");
            break;
        case HOUR_TO_SECOND:
            b.append("HOUR");
            if (precision > 0) {
                b.append('(').append(precision).append(')');
            }
            b.append(" TO SECOND");
            if (scale >= 0) {
                b.append('(').append(scale).append(')');
            }
            break;
        case MINUTE_TO_SECOND:
            b.append("MINUTE");
            if (precision > 0) {
                b.append('(').append(precision).append(')');
            }
            b.append(" TO SECOND");
            if (scale >= 0) {
                b.append('(').append(scale).append(')');
            }
        }
        return b.toString();
    }

}
