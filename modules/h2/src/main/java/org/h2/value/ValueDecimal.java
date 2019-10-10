/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.MathUtils;

/**
 * Implementation of the DECIMAL data type.
 */
public class ValueDecimal extends Value {

    /**
     * The value 'zero'.
     */
    public static final Object ZERO = new ValueDecimal(BigDecimal.ZERO);

    /**
     * The value 'one'.
     */
    public static final Object ONE = new ValueDecimal(BigDecimal.ONE);

    /**
     * The default precision for a decimal value.
     */
    static final int DEFAULT_PRECISION = 65535;

    /**
     * The default scale for a decimal value.
     */
    static final int DEFAULT_SCALE = 32767;

    /**
     * The default display size for a decimal value.
     */
    static final int DEFAULT_DISPLAY_SIZE = 65535;

    private static final int DIVIDE_SCALE_ADD = 25;

    /**
     * The maximum scale of a BigDecimal value.
     */
    private static final int BIG_DECIMAL_SCALE_MAX = 100_000;

    private final BigDecimal value;
    private String valueString;
    private int precision;

    private ValueDecimal(BigDecimal value) {
        if (value == null) {
            throw new IllegalArgumentException("null");
        } else if (!value.getClass().equals(BigDecimal.class)) {
            throw DbException.get(ErrorCode.INVALID_CLASS_2,
                    BigDecimal.class.getName(), value.getClass().getName());
        }
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        return ValueDecimal.get(value.add(dec.value));
    }

    @Override
    public Value subtract(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        return ValueDecimal.get(value.subtract(dec.value));
    }

    @Override
    public Value negate() {
        return ValueDecimal.get(value.negate());
    }

    @Override
    public Value multiply(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        return ValueDecimal.get(value.multiply(dec.value));
    }

    @Override
    public Value divide(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        if (dec.value.signum() == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        BigDecimal bd = value.divide(dec.value,
                value.scale() + DIVIDE_SCALE_ADD,
                BigDecimal.ROUND_HALF_DOWN);
        if (bd.signum() == 0) {
            bd = BigDecimal.ZERO;
        } else if (bd.scale() > 0) {
            if (!bd.unscaledValue().testBit(0)) {
                bd = bd.stripTrailingZeros();
            }
        }
        return ValueDecimal.get(bd);
    }

    @Override
    public ValueDecimal modulus(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        if (dec.value.signum() == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        BigDecimal bd = value.remainder(dec.value);
        return ValueDecimal.get(bd);
    }

    @Override
    public String getSQL() {
        return getString();
    }

    @Override
    public int getType() {
        return Value.DECIMAL;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueDecimal v = (ValueDecimal) o;
        return value.compareTo(v.value);
    }

    @Override
    public int getSignum() {
        return value.signum();
    }

    @Override
    public BigDecimal getBigDecimal() {
        return value;
    }

    @Override
    public String getString() {
        if (valueString == null) {
            String p = value.toPlainString();
            if (p.length() < 40) {
                valueString = p;
            } else {
                valueString = value.toString();
            }
        }
        return valueString;
    }

    @Override
    public long getPrecision() {
        if (precision == 0) {
            precision = value.precision();
        }
        return precision;
    }

    @Override
    public boolean checkPrecision(long prec) {
        if (prec == DEFAULT_PRECISION) {
            return true;
        }
        return getPrecision() <= prec;
    }

    @Override
    public int getScale() {
        return value.scale();
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setBigDecimal(parameterIndex, value);
    }

    @Override
    public Value convertScale(boolean onlyToSmallerScale, int targetScale) {
        if (value.scale() == targetScale) {
            return this;
        }
        if (onlyToSmallerScale || targetScale >= DEFAULT_SCALE) {
            if (value.scale() < targetScale) {
                return this;
            }
        }
        BigDecimal bd = ValueDecimal.setScale(value, targetScale);
        return ValueDecimal.get(bd);
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (getPrecision() <= precision) {
            return this;
        }
        if (force) {
            return get(BigDecimal.valueOf(value.doubleValue()));
        }
        throw DbException.get(
                ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1,
                Long.toString(precision));
    }

    /**
     * Get or create big decimal value for the given big decimal.
     *
     * @param dec the bit decimal
     * @return the value
     */
    public static ValueDecimal get(BigDecimal dec) {
        if (BigDecimal.ZERO.equals(dec)) {
            return (ValueDecimal) ZERO;
        } else if (BigDecimal.ONE.equals(dec)) {
            return (ValueDecimal) ONE;
        }
        return (ValueDecimal) Value.cache(new ValueDecimal(dec));
    }

    @Override
    public int getDisplaySize() {
        // add 2 characters for '-' and '.'
        return MathUtils.convertLongToInt(getPrecision() + 2);
    }

    @Override
    public boolean equals(Object other) {
        // Two BigDecimal objects are considered equal only if they are equal in
        // value and scale (thus 2.0 is not equal to 2.00 when using equals;
        // however -0.0 and 0.0 are). Can not use compareTo because 2.0 and 2.00
        // have different hash codes
        return other instanceof ValueDecimal &&
                value.equals(((ValueDecimal) other).value);
    }

    @Override
    public int getMemory() {
        return value.precision() + 120;
    }

    /**
     * Set the scale of a BigDecimal value.
     *
     * @param bd the BigDecimal value
     * @param scale the new scale
     * @return the scaled value
     */
    public static BigDecimal setScale(BigDecimal bd, int scale) {
        if (scale > BIG_DECIMAL_SCALE_MAX || scale < -BIG_DECIMAL_SCALE_MAX) {
            throw DbException.getInvalidValueException("scale", scale);
        }
        return bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
    }

}
