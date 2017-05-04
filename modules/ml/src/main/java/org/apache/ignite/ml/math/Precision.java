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

package org.apache.ignite.ml.math;

import java.math.BigDecimal;
import org.apache.ignite.ml.math.exceptions.MathArithmeticException;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;

/**
 * This class is based on the corresponding class from Apache Common Math lib.
 * Utilities for comparing numbers. *
 */
public class Precision {
    /**
     * <p>
     * Largest double-precision floating-point number such that
     * {@code 1 + EPSILON} is numerically equal to 1. This value is an upper
     * bound on the relative error due to rounding real numbers to double
     * precision floating-point numbers.
     * </p>
     * <p>
     * In IEEE 754 arithmetic, this is 2<sup>-53</sup>.
     * </p>
     *
     * @see <a href="http://en.wikipedia.org/wiki/Machine_epsilon">Machine epsilon</a>
     */
    public static final double EPSILON;

    /**
     * Safe minimum, such that {@code 1 / SAFE_MIN} does not overflow.
     * <br/>
     * In IEEE 754 arithmetic, this is also the smallest normalized
     * number 2<sup>-1022</sup>.
     */
    public static final double SAFE_MIN;

    /** Exponent offset in IEEE754 representation. */
    private static final long EXPONENT_OFFSET = 1023L;

    /** Offset to order signed double numbers lexicographically. */
    private static final long SGN_MASK = 0x8000000000000000L;
    /** Offset to order signed double numbers lexicographically. */
    private static final int SGN_MASK_FLOAT = 0x80000000;
    /** Positive zero. */
    private static final double POSITIVE_ZERO = 0d;
    /** Positive zero bits. */
    private static final long POSITIVE_ZERO_DOUBLE_BITS = Double.doubleToRawLongBits(+0.0);
    /** Negative zero bits. */
    private static final long NEGATIVE_ZERO_DOUBLE_BITS = Double.doubleToRawLongBits(-0.0);
    /** Positive zero bits. */
    private static final int POSITIVE_ZERO_FLOAT_BITS = Float.floatToRawIntBits(+0.0f);
    /** Negative zero bits. */
    private static final int NEGATIVE_ZERO_FLOAT_BITS = Float.floatToRawIntBits(-0.0f);
    /** */
    private static final String INVALID_ROUNDING_METHOD = "invalid rounding method {0}, " +
        "valid methods: {1} ({2}), {3} ({4}), {5} ({6}), {7} ({8}), {9} ({10}), {11} ({12}), {13} ({14}), {15} ({16})";

    static {
        /*
         *  This was previously expressed as = 0x1.0p-53;
         *  However, OpenJDK (Sparc Solaris) cannot handle such small
         *  constants: MATH-721
         */
        EPSILON = Double.longBitsToDouble((EXPONENT_OFFSET - 53L) << 52);

        /*
         * This was previously expressed as = 0x1.0p-1022;
         * However, OpenJDK (Sparc Solaris) cannot handle such small
         * constants: MATH-721
         */
        SAFE_MIN = Double.longBitsToDouble((EXPONENT_OFFSET - 1022L) << 52);
    }

    /**
     * Private constructor.
     */
    private Precision() {
    }

    /**
     * Compares two numbers given some amount of allowed error.
     *
     * @param x the first number
     * @param y the second number
     * @param eps the amount of error to allow when checking for equality
     * @return <ul><li>0 if  {@link #equals(double, double, double) equals(x, y, eps)}</li> <li>&lt; 0 if !{@link
     * #equals(double, double, double) equals(x, y, eps)} &amp;&amp; x &lt; y</li> <li>> 0 if !{@link #equals(double,
     * double, double) equals(x, y, eps)} &amp;&amp; x > y or either argument is NaN</li></ul>
     */
    public static int compareTo(double x, double y, double eps) {
        if (equals(x, y, eps))
            return 0;
        else if (x < y)
            return -1;
        return 1;
    }

    /**
     * Compares two numbers given some amount of allowed error.
     * Two float numbers are considered equal if there are {@code (maxUlps - 1)}
     * (or fewer) floating point numbers between them, i.e. two adjacent floating
     * point numbers are considered equal.
     * Adapted from <a
     * href="http://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/">
     * Bruce Dawson</a>. Returns {@code false} if either of the arguments is NaN.
     *
     * @param x first value
     * @param y second value
     * @param maxUlps {@code (maxUlps - 1)} is the number of floating point values between {@code x} and {@code y}.
     * @return <ul><li>0 if  {@link #equals(double, double, int) equals(x, y, maxUlps)}</li> <li>&lt; 0 if !{@link
     * #equals(double, double, int) equals(x, y, maxUlps)} &amp;&amp; x &lt; y</li> <li>&gt; 0 if !{@link
     * #equals(double, double, int) equals(x, y, maxUlps)} &amp;&amp; x > y or either argument is NaN</li></ul>
     */
    public static int compareTo(final double x, final double y, final int maxUlps) {
        if (equals(x, y, maxUlps))
            return 0;
        else if (x < y)
            return -1;
        return 1;
    }

    /**
     * Returns true iff they are equal as defined by
     * {@link #equals(float, float, int) equals(x, y, 1)}.
     *
     * @param x first value
     * @param y second value
     * @return {@code true} if the values are equal.
     */
    public static boolean equals(float x, float y) {
        return equals(x, y, 1);
    }

    /**
     * Returns true if both arguments are NaN or they are
     * equal as defined by {@link #equals(float, float) equals(x, y, 1)}.
     *
     * @param x first value
     * @param y second value
     * @return {@code true} if the values are equal or both are NaN.
     * @since 2.2
     */
    public static boolean equalsIncludingNaN(float x, float y) {
        return (x != x || y != y) ? !(x != x ^ y != y) : equals(x, y, 1);
    }

    /**
     * Returns true if the arguments are equal or within the range of allowed
     * error (inclusive).  Returns {@code false} if either of the arguments
     * is NaN.
     *
     * @param x first value
     * @param y second value
     * @param eps the amount of absolute error to allow.
     * @return {@code true} if the values are equal or within range of each other.
     * @since 2.2
     */
    public static boolean equals(float x, float y, float eps) {
        return equals(x, y, 1) || Math.abs(y - x) <= eps;
    }

    /**
     * Returns true if the arguments are both NaN, are equal, or are within the range
     * of allowed error (inclusive).
     *
     * @param x first value
     * @param y second value
     * @param eps the amount of absolute error to allow.
     * @return {@code true} if the values are equal or within range of each other, or both are NaN.
     * @since 2.2
     */
    public static boolean equalsIncludingNaN(float x, float y, float eps) {
        return equalsIncludingNaN(x, y) || (Math.abs(y - x) <= eps);
    }

    /**
     * Returns true if the arguments are equal or within the range of allowed
     * error (inclusive).
     * Two float numbers are considered equal if there are {@code (maxUlps - 1)}
     * (or fewer) floating point numbers between them, i.e. two adjacent floating
     * point numbers are considered equal.
     * Adapted from <a
     * href="http://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/">
     * Bruce Dawson</a>.  Returns {@code false} if either of the arguments is NaN.
     *
     * @param x first value
     * @param y second value
     * @param maxUlps {@code (maxUlps - 1)} is the number of floating point values between {@code x} and {@code y}.
     * @return {@code true} if there are fewer than {@code maxUlps} floating point values between {@code x} and {@code
     * y}.
     * @since 2.2
     */
    public static boolean equals(final float x, final float y, final int maxUlps) {

        final int xInt = Float.floatToRawIntBits(x);
        final int yInt = Float.floatToRawIntBits(y);

        final boolean isEqual;
        if (((xInt ^ yInt) & SGN_MASK_FLOAT) == 0) {
            // number have same sign, there is no risk of overflow
            isEqual = Math.abs(xInt - yInt) <= maxUlps;
        }
        else {
            // number have opposite signs, take care of overflow
            final int deltaPlus;
            final int deltaMinus;
            if (xInt < yInt) {
                deltaPlus = yInt - POSITIVE_ZERO_FLOAT_BITS;
                deltaMinus = xInt - NEGATIVE_ZERO_FLOAT_BITS;
            }
            else {
                deltaPlus = xInt - POSITIVE_ZERO_FLOAT_BITS;
                deltaMinus = yInt - NEGATIVE_ZERO_FLOAT_BITS;
            }

            if (deltaPlus > maxUlps)
                isEqual = false;
            else
                isEqual = deltaMinus <= (maxUlps - deltaPlus);

        }

        return isEqual && !Float.isNaN(x) && !Float.isNaN(y);

    }

    /**
     * Returns true if the arguments are both NaN or if they are equal as defined
     * by {@link #equals(float, float, int) equals(x, y, maxUlps)}.
     *
     * @param x first value
     * @param y second value
     * @param maxUlps {@code (maxUlps - 1)} is the number of floating point values between {@code x} and {@code y}.
     * @return {@code true} if both arguments are NaN or if there are less than {@code maxUlps} floating point values
     * between {@code x} and {@code y}.
     * @since 2.2
     */
    public static boolean equalsIncludingNaN(float x, float y, int maxUlps) {
        return (x != x || y != y) ? !(x != x ^ y != y) : equals(x, y, maxUlps);
    }

    /**
     * Returns true iff they are equal as defined by
     * {@link #equals(double, double, int) equals(x, y, 1)}.
     *
     * @param x first value
     * @param y second value
     * @return {@code true} if the values are equal.
     */
    public static boolean equals(double x, double y) {
        return equals(x, y, 1);
    }

    /**
     * Returns true if the arguments are both NaN or they are
     * equal as defined by {@link #equals(double, double) equals(x, y, 1)}.
     *
     * @param x first value
     * @param y second value
     * @return {@code true} if the values are equal or both are NaN.
     * @since 2.2
     */
    public static boolean equalsIncludingNaN(double x, double y) {
        return (x != x || y != y) ? !(x != x ^ y != y) : equals(x, y, 1);
    }

    /**
     * Returns {@code true} if there is no double value strictly between the
     * arguments or the difference between them is within the range of allowed
     * error (inclusive). Returns {@code false} if either of the arguments
     * is NaN.
     *
     * @param x First value.
     * @param y Second value.
     * @param eps Amount of allowed absolute error.
     * @return {@code true} if the values are two adjacent floating point numbers or they are within range of each
     * other.
     */
    public static boolean equals(double x, double y, double eps) {
        return equals(x, y, 1) || Math.abs(y - x) <= eps;
    }

    /**
     * Returns {@code true} if there is no double value strictly between the
     * arguments or the relative difference between them is less than or equal
     * to the given tolerance. Returns {@code false} if either of the arguments
     * is NaN.
     *
     * @param x First value.
     * @param y Second value.
     * @param eps Amount of allowed relative error.
     * @return {@code true} if the values are two adjacent floating point numbers or they are within range of each
     * other.
     * @since 3.1
     */
    public static boolean equalsWithRelativeTolerance(double x, double y, double eps) {
        if (equals(x, y, 1))
            return true;

        final double absMax = Math.max(Math.abs(x), Math.abs(y));
        final double relativeDifference = Math.abs((x - y) / absMax);

        return relativeDifference <= eps;
    }

    /**
     * Returns true if the arguments are both NaN, are equal or are within the range
     * of allowed error (inclusive).
     *
     * @param x first value
     * @param y second value
     * @param eps the amount of absolute error to allow.
     * @return {@code true} if the values are equal or within range of each other, or both are NaN.
     * @since 2.2
     */
    public static boolean equalsIncludingNaN(double x, double y, double eps) {
        return equalsIncludingNaN(x, y) || (Math.abs(y - x) <= eps);
    }

    /**
     * Returns true if the arguments are equal or within the range of allowed
     * error (inclusive).
     * <p>
     * Two float numbers are considered equal if there are {@code (maxUlps - 1)}
     * (or fewer) floating point numbers between them, i.e. two adjacent
     * floating point numbers are considered equal.
     * </p>
     * <p>
     * Adapted from <a
     * href="http://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/">
     * Bruce Dawson</a>. Returns {@code false} if either of the arguments is NaN.
     * </p>
     *
     * @param x first value
     * @param y second value
     * @param maxUlps {@code (maxUlps - 1)} is the number of floating point values between {@code x} and {@code y}.
     * @return {@code true} if there are fewer than {@code maxUlps} floating point values between {@code x} and {@code
     * y}.
     */
    public static boolean equals(final double x, final double y, final int maxUlps) {

        final long xInt = Double.doubleToRawLongBits(x);
        final long yInt = Double.doubleToRawLongBits(y);

        final boolean isEqual;
        if (((xInt ^ yInt) & SGN_MASK) == 0L) {
            // number have same sign, there is no risk of overflow
            isEqual = Math.abs(xInt - yInt) <= maxUlps;
        }
        else {
            // number have opposite signs, take care of overflow
            final long deltaPlus;
            final long deltaMinus;
            if (xInt < yInt) {
                deltaPlus = yInt - POSITIVE_ZERO_DOUBLE_BITS;
                deltaMinus = xInt - NEGATIVE_ZERO_DOUBLE_BITS;
            }
            else {
                deltaPlus = xInt - POSITIVE_ZERO_DOUBLE_BITS;
                deltaMinus = yInt - NEGATIVE_ZERO_DOUBLE_BITS;
            }

            if (deltaPlus > maxUlps)
                isEqual = false;
            else
                isEqual = deltaMinus <= (maxUlps - deltaPlus);

        }

        return isEqual && !Double.isNaN(x) && !Double.isNaN(y);

    }

    /**
     * Returns true if both arguments are NaN or if they are equal as defined
     * by {@link #equals(double, double, int) equals(x, y, maxUlps)}.
     *
     * @param x first value
     * @param y second value
     * @param maxUlps {@code (maxUlps - 1)} is the number of floating point values between {@code x} and {@code y}.
     * @return {@code true} if both arguments are NaN or if there are less than {@code maxUlps} floating point values
     * between {@code x} and {@code y}.
     * @since 2.2
     */
    public static boolean equalsIncludingNaN(double x, double y, int maxUlps) {
        return (x != x || y != y) ? !(x != x ^ y != y) : equals(x, y, maxUlps);
    }

    /**
     * Rounds the given value to the specified number of decimal places.
     * The value is rounded using the {@link BigDecimal#ROUND_HALF_UP} method.
     *
     * @param x Value to round.
     * @param scale Number of digits to the right of the decimal point.
     * @return the rounded value.
     * @since 1.1 (previously in {@code MathUtils}, moved as of version 3.0)
     */
    public static double round(double x, int scale) {
        return round(x, scale, BigDecimal.ROUND_HALF_UP);
    }

    /**
     * Rounds the given value to the specified number of decimal places.
     * The value is rounded using the given method which is any method defined
     * in {@link BigDecimal}.
     * If {@code x} is infinite or {@code NaN}, then the value of {@code x} is
     * returned unchanged, regardless of the other parameters.
     *
     * @param x Value to round.
     * @param scale Number of digits to the right of the decimal point.
     * @param roundingMtd Rounding method as defined in {@link BigDecimal}.
     * @return the rounded value.
     * @throws ArithmeticException if {@code roundingMethod == ROUND_UNNECESSARY} and the specified scaling operation
     * would require rounding.
     * @throws IllegalArgumentException if {@code roundingMethod} does not represent a valid rounding mode.
     * @since 1.1 (previously in {@code MathUtils}, moved as of version 3.0)
     */
    public static double round(double x, int scale, int roundingMtd) {
        try {
            final double rounded = (new BigDecimal(Double.toString(x))
                .setScale(scale, roundingMtd))
                .doubleValue();
            // MATH-1089: negative values rounded to zero should result in negative zero
            return rounded == POSITIVE_ZERO ? POSITIVE_ZERO * x : rounded;
        }
        catch (NumberFormatException ex) {
            if (Double.isInfinite(x))
                return x;
            else
                return Double.NaN;
        }
    }

    /**
     * Rounds the given value to the specified number of decimal places.
     * The value is rounded using the {@link BigDecimal#ROUND_HALF_UP} method.
     *
     * @param x Value to round.
     * @param scale Number of digits to the right of the decimal point.
     * @return the rounded value.
     * @since 1.1 (previously in {@code MathUtils}, moved as of version 3.0)
     */
    public static float round(float x, int scale) {
        return round(x, scale, BigDecimal.ROUND_HALF_UP);
    }

    /**
     * Rounds the given value to the specified number of decimal places.
     * The value is rounded using the given method which is any method defined
     * in {@link BigDecimal}.
     *
     * @param x Value to round.
     * @param scale Number of digits to the right of the decimal point.
     * @param roundingMtd Rounding method as defined in {@link BigDecimal}.
     * @return the rounded value.
     * @throws MathArithmeticException if an exact operation is required but result is not exact
     * @throws MathIllegalArgumentException if {@code roundingMethod} is not a valid rounding method.
     * @since 1.1 (previously in {@code MathUtils}, moved as of version 3.0)
     */
    public static float round(float x, int scale, int roundingMtd)
        throws MathArithmeticException, MathIllegalArgumentException {
        final float sign = Math.copySign(1f, x);
        final float factor = (float)Math.pow(10.0f, scale) * sign;
        return (float)roundUnscaled(x * factor, sign, roundingMtd) / factor;
    }

    /**
     * Rounds the given non-negative value to the "nearest" integer. Nearest is
     * determined by the rounding method specified. Rounding methods are defined
     * in {@link BigDecimal}.
     *
     * @param unscaled Value to round.
     * @param sign Sign of the original, scaled value.
     * @param roundingMtd Rounding method, as defined in {@link BigDecimal}.
     * @return the rounded value.
     * @throws MathArithmeticException if an exact operation is required but result is not exact
     * @throws MathIllegalArgumentException if {@code roundingMethod} is not a valid rounding method.
     * @since 1.1 (previously in {@code MathUtils}, moved as of version 3.0)
     */
    private static double roundUnscaled(double unscaled, double sign, int roundingMtd)
        throws MathArithmeticException, MathIllegalArgumentException {
        switch (roundingMtd) {
            case BigDecimal.ROUND_CEILING:
                if (sign == -1)
                    unscaled = Math.floor(Math.nextAfter(unscaled, Double.NEGATIVE_INFINITY));
                else
                    unscaled = Math.ceil(Math.nextAfter(unscaled, Double.POSITIVE_INFINITY));
                break;
            case BigDecimal.ROUND_DOWN:
                unscaled = Math.floor(Math.nextAfter(unscaled, Double.NEGATIVE_INFINITY));
                break;
            case BigDecimal.ROUND_FLOOR:
                if (sign == -1)
                    unscaled = Math.ceil(Math.nextAfter(unscaled, Double.POSITIVE_INFINITY));
                else
                    unscaled = Math.floor(Math.nextAfter(unscaled, Double.NEGATIVE_INFINITY));
                break;
            case BigDecimal.ROUND_HALF_DOWN: {
                unscaled = Math.nextAfter(unscaled, Double.NEGATIVE_INFINITY);
                double fraction = unscaled - Math.floor(unscaled);
                if (fraction > 0.5)
                    unscaled = Math.ceil(unscaled);
                else
                    unscaled = Math.floor(unscaled);
                break;
            }
            case BigDecimal.ROUND_HALF_EVEN: {
                double fraction = unscaled - Math.floor(unscaled);
                if (fraction > 0.5)
                    unscaled = Math.ceil(unscaled);
                else if (fraction < 0.5)
                    unscaled = Math.floor(unscaled);
                else {
                    // The following equality test is intentional and needed for rounding purposes
                    if (Math.floor(unscaled) / 2.0 == Math.floor(Math.floor(unscaled) / 2.0)) { // even
                        unscaled = Math.floor(unscaled);
                    }
                    else { // odd
                        unscaled = Math.ceil(unscaled);
                    }
                }
                break;
            }
            case BigDecimal.ROUND_HALF_UP: {
                unscaled = Math.nextAfter(unscaled, Double.POSITIVE_INFINITY);
                double fraction = unscaled - Math.floor(unscaled);
                if (fraction >= 0.5)
                    unscaled = Math.ceil(unscaled);
                else
                    unscaled = Math.floor(unscaled);
                break;
            }
            case BigDecimal.ROUND_UNNECESSARY:
                if (unscaled != Math.floor(unscaled))
                    throw new MathArithmeticException();
                break;
            case BigDecimal.ROUND_UP:
                // do not round if the discarded fraction is equal to zero
                if (unscaled != Math.floor(unscaled))
                    unscaled = Math.ceil(Math.nextAfter(unscaled, Double.POSITIVE_INFINITY));
                break;
            default:
                throw new MathIllegalArgumentException(INVALID_ROUNDING_METHOD,
                    roundingMtd,
                    "ROUND_CEILING", BigDecimal.ROUND_CEILING,
                    "ROUND_DOWN", BigDecimal.ROUND_DOWN,
                    "ROUND_FLOOR", BigDecimal.ROUND_FLOOR,
                    "ROUND_HALF_DOWN", BigDecimal.ROUND_HALF_DOWN,
                    "ROUND_HALF_EVEN", BigDecimal.ROUND_HALF_EVEN,
                    "ROUND_HALF_UP", BigDecimal.ROUND_HALF_UP,
                    "ROUND_UNNECESSARY", BigDecimal.ROUND_UNNECESSARY,
                    "ROUND_UP", BigDecimal.ROUND_UP);
        }
        return unscaled;
    }

    /**
     * Computes a number {@code delta} close to {@code originalDelta} with
     * the property that <pre><code>
     *   x + delta - x
     * </code></pre>
     * is exactly machine-representable.
     * This is useful when computing numerical derivatives, in order to reduce
     * roundoff errors.
     *
     * @param x Value.
     * @param originalDelta Offset value.
     * @return a number {@code delta} so that {@code x + delta} and {@code x} differ by a representable floating number.
     */
    public static double representableDelta(double x, double originalDelta) {
        return x + originalDelta - x;
    }
}
