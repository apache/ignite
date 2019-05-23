package de.bwaldvogel.mongo.bson;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

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

    private static final long INFINITY_MASK = 0x7800000000000000L;
    private static final long NaN_MASK = 0x7c00000000000000L;
    private static final long SIGN_BIT_MASK = 1L << 63;

    private static final int EXPONENT_OFFSET = 6176;

    private final long low;
    private final long high;

    public Decimal128(long low, long high) {
        this.low = low;
        this.high = high;
    }

    public long getLow() {
        return low;
    }

    public long getHigh() {
        return high;
    }

    private BigDecimal toBigDecimal() {
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
        return (isNegative() ? "-" : "") + toStringInternal();
    }

    private String toStringInternal() {
        if (isInfinite()) {
            return "Infinity";
        } else {
            return toStringWithBigDecimal();
        }
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
    public int compareTo(Decimal128 o) {
        if (isSpecial() || o.isSpecial()) {
            return Double.compare(doubleValue(), o.doubleValue());
        }
        return toBigDecimal().compareTo(o.toBigDecimal());
    }

    private boolean isSpecial() {
        return isNaN() || isInfinite();
    }

}
