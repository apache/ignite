package de.bwaldvogel.mongo.bson;

import static java.math.MathContext.DECIMAL128;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

import de.bwaldvogel.mongo.backend.Assert;

public final class Decimal128 extends Number implements Serializable, Comparable<Decimal128> {

    private static final long serialVersionUID = 1L;

    public static final Decimal128 ONE = new Decimal128(1, 3476778912330022912L);
    public static final Decimal128 TWO = new Decimal128(2, 3476778912330022912L);
    public static final Decimal128 NaN = new Decimal128(0, 8935141660703064064L);
    public static final Decimal128 POSITIVE_ZERO = new Decimal128(0, 3476778912330022912L);
    public static final Decimal128 NEGATIVE_ZERO = new Decimal128(0, -5746593124524752896L);
    public static final Decimal128 POSITIVE_INFINITY = new Decimal128(0, 8646911284551352320L);
    public static final Decimal128 NEGATIVE_INFINITY = new Decimal128(0, -576460752303423488L);

    private static final BigInteger BIG_INT_TEN = new BigInteger("10");
    private static final BigInteger BIG_INT_ONE = new BigInteger("1");
    private static final BigInteger BIG_INT_ZERO = new BigInteger("0");

    private static final long INFINITY_MASK = 0x7800000000000000L;
    private static final long NaN_MASK = 0x7c00000000000000L;
    private static final long SIGN_BIT_MASK = 1L << 63;
    private static final int MIN_EXPONENT = -6176;
    private static final int MAX_EXPONENT = 6111;

    private static final int EXPONENT_OFFSET = 6176;
    private static final int MAX_BIT_LENGTH = 113;

    private final long low;
    private final long high;

    public Decimal128(long low, long high) {
        this.low = low;
        this.high = high;
    }

    public Decimal128(BigDecimal value) {
        this(value, value.signum() == -1);
    }

    public static Decimal128 fromNumber(Number value) {
        if (value instanceof Decimal128) {
            return (Decimal128) value;
        } else if (value instanceof Long || value instanceof Integer || value instanceof Short) {
            return new Decimal128(BigDecimal.valueOf(value.longValue()));
        } else {
            return new Decimal128(BigDecimal.valueOf(value.doubleValue()));
        }
    }

    // isNegative is necessary to detect -0, which can't be represented with a BigDecimal
    private Decimal128(BigDecimal initialValue, boolean isNegative) {
        long localHigh = 0;
        long localLow = 0;

        BigDecimal value = clampAndRound(initialValue);

        long exponent = -value.scale();

        if ((exponent < MIN_EXPONENT) || (exponent > MAX_EXPONENT)) {
            throw new AssertionError("Exponent is out of range for Decimal128 encoding: " + exponent);
        }

        if (value.unscaledValue().bitLength() > MAX_BIT_LENGTH) {
            throw new AssertionError("Unscaled roundedValue is out of range for Decimal128 encoding:" + value.unscaledValue());
        }

        BigInteger significand = value.unscaledValue().abs();
        int bitLength = significand.bitLength();

        for (int i = 0; i < Math.min(64, bitLength); i++) {
            if (significand.testBit(i)) {
                localLow |= 1L << i;
            }
        }

        for (int i = 64; i < bitLength; i++) {
            if (significand.testBit(i)) {
                localHigh |= 1L << (i - 64);
            }
        }

        long biasedExponent = exponent + EXPONENT_OFFSET;

        localHigh |= biasedExponent << 49;

        if (value.signum() == -1 || isNegative) {
            localHigh |= SIGN_BIT_MASK;
        }

        high = localHigh;
        low = localLow;
    }

    public long getLow() {
        return low;
    }

    public long getHigh() {
        return high;
    }

    public BigDecimal toBigDecimal() {
        Assert.isFalse(isSpecial(), () -> this + " cannot be converted to BigDecimal");
        return toBigDecimalValue();
    }

    private BigDecimal toBigDecimalValue() {
        int scale = -getExponent();

        if (twoHighestCombinationBitsAreSet()) {
            return BigDecimal.valueOf(0, scale);
        }

        return new BigDecimal(new BigInteger(isNegative() ? -1 : 1, getBytes()), scale);
    }

    private byte[] getBytes() {
        byte[] bytes = new byte[15];

        long mask = 0x00000000000000ff;
        for (int i = 14; i >= 7; i--) {
            bytes[i] = (byte) ((low & mask) >>> ((14 - i) << 3));
            mask = mask << 8;
        }

        mask = 0x00000000000000ff;
        for (int i = 6; i >= 1; i--) {
            bytes[i] = (byte) ((high & mask) >>> ((6 - i) << 3));
            mask = mask << 8;
        }

        mask = 0x0001000000000000L;
        bytes[0] = (byte) ((high & mask) >>> 48);
        return bytes;
    }

    private int getExponent() {
        if (twoHighestCombinationBitsAreSet()) {
            return (int) ((high & 0x1fffe00000000000L) >>> 47) - EXPONENT_OFFSET;
        } else {
            return (int) ((high & 0x7fff800000000000L) >>> 49) - EXPONENT_OFFSET;
        }
    }

    private boolean twoHighestCombinationBitsAreSet() {
        return (high & 3L << 61) == 3L << 61;
    }

    private boolean isNegative() {
        return (high & SIGN_BIT_MASK) == SIGN_BIT_MASK;
    }

    private boolean isInfinite() {
        return (high & INFINITY_MASK) == INFINITY_MASK;
    }

    private boolean isNaN() {
        return (high & NaN_MASK) == NaN_MASK;
    }

    @Override
    public String toString() {
        if (isNaN()) {
            return "NaN";
        }
        if (isInfinite()) {
            return isNegative() ? "-Infinity" : "Infinity";
        }
        return (isNegative() ? "-" : "") + toStringWithBigDecimal();
    }

    private String toStringWithBigDecimal() {
        BigDecimal bigDecimal = toBigDecimalValue();
        String significand = bigDecimal.unscaledValue().abs().toString();

        int exponent = -bigDecimal.scale();
        int adjustedExponent = exponent + significand.length() - 1;
        if (exponent <= 0 && adjustedExponent >= -6) {
            return toNormalString(significand, exponent);
        } else {
            return toScientificString(significand, adjustedExponent);
        }
    }

    private String toNormalString(String significand, int exponent) {
        StringBuilder builder = new StringBuilder();
        if (exponent == 0) {
            builder.append(significand);
        } else {
            int pad = -exponent - significand.length();
            if (pad >= 0) {
                builder.append('0');
                builder.append('.');
                for (int i = 0; i < pad; i++) {
                    builder.append('0');
                }
                builder.append(significand, 0, significand.length());
            } else {
                builder.append(significand, 0, -pad);
                builder.append('.');
                builder.append(significand, -pad, -pad - exponent);
            }
        }
        return builder.toString();
    }

    private String toScientificString(String significand, int adjustedExponent) {
        StringBuilder builder = new StringBuilder();
        builder.append(significand.charAt(0));
        if (significand.length() > 1) {
            builder.append('.');
            builder.append(significand, 1, significand.length());
        }
        builder.append('E');
        if (adjustedExponent > 0) {
            builder.append('+');
        }
        builder.append(adjustedExponent);
        return builder.toString();
    }

    @Override
    public int intValue() {
        return toBigDecimal().intValue();
    }

    @Override
    public long longValue() {
        return toBigDecimal().intValue();
    }

    @Override
    public float floatValue() {
        if (isNaN()) {
            return Float.NaN;
        }
        if (isInfinite()) {
            return isNegative() ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
        }
        return toBigDecimal().floatValue();
    }

    @Override
    public double doubleValue() {
        if (isNaN()) {
            return Double.NaN;
        }
        if (isInfinite()) {
            return isNegative() ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }
        return toBigDecimal().doubleValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Decimal128 that = (Decimal128) o;
        return getLow() == that.getLow()
            && getHigh() == that.getHigh();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLow(), getHigh());
    }

    @Override
    public int compareTo(Decimal128 o) {
        if (isSpecial() || o.isSpecial()) {
            return Double.compare(doubleValue(), o.doubleValue());
        }
        return toBigDecimal().compareTo(o.toBigDecimal());
    }

    private boolean isSpecial() {
        return isNaN() || isInfinite();
    }

    private BigDecimal clampAndRound(BigDecimal initialValue) {
        if (-initialValue.scale() > MAX_EXPONENT) {
            int diff = -initialValue.scale() - MAX_EXPONENT;
            if (initialValue.unscaledValue().equals(BIG_INT_ZERO)) {
                return new BigDecimal(initialValue.unscaledValue(), -MAX_EXPONENT);
            } else if (diff + initialValue.precision() > 34) {
                throw new NumberFormatException("Exponent is out of range for Decimal128 encoding of " + initialValue);
            } else {
                BigInteger multiplier = BIG_INT_TEN.pow(diff);
                return new BigDecimal(initialValue.unscaledValue().multiply(multiplier), initialValue.scale() + diff);
            }
        } else if (-initialValue.scale() < MIN_EXPONENT) {
            // Increasing a very negative exponent may require decreasing precision, which is rounding
            // Only round exactly (by removing precision that is all zeroes).  An exception is thrown if the rounding would be inexact:
            // Exact:     .000...0011000  => 11000E-6177  => 1100E-6176  => .000001100
            // Inexact:   .000...0011001  => 11001E-6177  => 1100E-6176  => .000001100
            int diff = initialValue.scale() + MIN_EXPONENT;
            int undiscardedPrecision = ensureExactRounding(initialValue, diff);
            BigInteger divisor = undiscardedPrecision == 0 ? BIG_INT_ONE : BIG_INT_TEN.pow(diff);
            return new BigDecimal(initialValue.unscaledValue().divide(divisor), initialValue.scale() - diff);
        } else {
            BigDecimal roundedValue = initialValue.round(DECIMAL128);
            int extraPrecision = initialValue.precision() - roundedValue.precision();
            if (extraPrecision > 0) {
                // Again, only round exactly
                ensureExactRounding(initialValue, extraPrecision);
            }
            return roundedValue;
        }
    }

    private int ensureExactRounding(BigDecimal initialValue, int extraPrecision) {
        String significand = initialValue.unscaledValue().abs().toString();
        int undiscardedPrecision = Math.max(0, significand.length() - extraPrecision);
        for (int i = undiscardedPrecision; i < significand.length(); i++) {
            if (significand.charAt(i) != '0') {
                throw new NumberFormatException("Conversion to Decimal128 would require inexact rounding of " + initialValue);
            }
        }
        return undiscardedPrecision;
    }

}
