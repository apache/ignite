/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;

/**
 * Implementation of the DATE data type.
 */
public class ValueDate extends Value {

    /**
     * The default precision and display size of the textual representation of a date.
     * Example: 2000-01-02
     */
    public static final int PRECISION = 10;

    private final long dateValue;

    private ValueDate(long dateValue) {
        this.dateValue = dateValue;
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value
     * @return the value
     */
    public static ValueDate fromDateValue(long dateValue) {
        return (ValueDate) Value.cache(new ValueDate(dateValue));
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param date the date
     * @return the value
     */
    public static ValueDate get(Date date) {
        return fromDateValue(DateTimeUtils.dateValueFromDate(date.getTime()));
    }

    /**
     * Calculate the date value (in the default timezone) from a given time in
     * milliseconds in UTC.
     *
     * @param ms the milliseconds
     * @return the value
     */
    public static ValueDate fromMillis(long ms) {
        return fromDateValue(DateTimeUtils.dateValueFromDate(ms));
    }

    /**
     * Parse a string to a ValueDate.
     *
     * @param s the string to parse
     * @return the date
     */
    public static ValueDate parse(String s) {
        try {
            return fromDateValue(DateTimeUtils.parseDateValue(s, 0, s.length()));
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                    e, "DATE", s);
        }
    }

    public long getDateValue() {
        return dateValue;
    }

    @Override
    public Date getDate() {
        return DateTimeUtils.convertDateValueToDate(dateValue);
    }

    @Override
    public int getType() {
        return Value.DATE;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder(PRECISION);
        DateTimeUtils.appendDate(buff, dateValue);
        return buff.toString();
    }

    @Override
    public String getSQL() {
        return "DATE '" + getString() + "'";
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int getDisplaySize() {
        return PRECISION;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        return Long.compare(dateValue, ((ValueDate) o).dateValue);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ValueDate
                && dateValue == (((ValueDate) other).dateValue);
    }

    @Override
    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32));
    }

    @Override
    public Object getObject() {
        return getDate();
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setDate(parameterIndex, getDate());
    }

}
