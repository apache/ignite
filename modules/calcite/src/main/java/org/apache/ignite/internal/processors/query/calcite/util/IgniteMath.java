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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;

/** Math operations with overflow checking. */
public class IgniteMath {
    /** */
    public static final RoundingMode NUMERIC_ROUNDING_MODE = RoundingMode.HALF_UP;

    /** Cached bounds of long type, min and max for Float, Double, BigDecimal. */
    private static final Number[] LONG_BOUNDS = new Number[] {Long.MIN_VALUE - 0.5f, Long.MAX_VALUE + 0.5f,
        Long.MIN_VALUE - 0.5d, Long.MAX_VALUE + 0.5d, BigDecimal.valueOf(Long.MIN_VALUE), BigDecimal.valueOf(Long.MAX_VALUE)};

    /** Cached bounds of integer type, min and max for Float, Double, BigDecimal. */
    private static final Number[] INT_BOUNDS = new Number[] {Integer.MIN_VALUE - 0.5f, Integer.MAX_VALUE + 0.5f,
        Integer.MIN_VALUE - 0.5d, Integer.MAX_VALUE + 0.5d, BigDecimal.valueOf(Integer.MIN_VALUE), BigDecimal.valueOf(Integer.MAX_VALUE)};

    /** Cached bounds of short type, min and max for Float, Double, BigDecimal. */
    private static final Number[] SHORT_BOUNDS = new Number[] {Short.MIN_VALUE - 0.5f, Short.MAX_VALUE + 0.5f,
        Short.MIN_VALUE - 0.5d, Short.MAX_VALUE + 0.5d, BigDecimal.valueOf(Short.MIN_VALUE), BigDecimal.valueOf(Short.MAX_VALUE)};

    /** Cached bounds of byte type, min and max for Float, Double, BigDecimal. */
    private static final Number[] BYTE_BOUNDS = new Number[] {Byte.MIN_VALUE - 0.5f, Byte.MAX_VALUE + 0.5f,
        Byte.MIN_VALUE - 0.5d, Byte.MAX_VALUE + 0.5d, BigDecimal.valueOf(Byte.MIN_VALUE), BigDecimal.valueOf(Byte.MAX_VALUE)};

    /** Returns the sum of its arguments, throwing an exception if the result overflows an {@code long}. */
    public static long addExact(long x, long y) {
        long r = x + y;

        if (((x ^ r) & (y ^ r)) < 0)
            throw new ArithmeticException(BIGINT.getName() + " overflow");

        return r;
    }

    /** Returns the sum of its arguments, throwing an exception if the result overflows an {@code int}. */
    public static int addExact(int x, int y) {
        int r = x + y;

        if (((x ^ r) & (y ^ r)) < 0)
            throw new ArithmeticException(INTEGER.getName() + " overflow");

        return r;
    }

    /** Returns the sum of its arguments, throwing an exception if the result overflows an {@code short}. */
    public static short addExact(short x, short y) {
        int r = x + y;

        if (r != (short)r)
            throw new ArithmeticException(SMALLINT.getName() + " overflow");

        return (short)r;
    }

    /** Returns the sum of its arguments, throwing an exception if the result overflows an {@code byte}. */
    public static byte addExact(byte x, byte y) {
        int r = x + y;

        if (r != (byte)r)
            throw new ArithmeticException(TINYINT.getName() + " overflow");

        return (byte)r;
    }

    /** Returns the negation of the argument, throwing an exception if the result overflows an {@code long}. */
    public static long negateExact(long x) {
        long res = -x;

        if (x != 0 && x == res)
            throw new ArithmeticException(BIGINT.getName() + " overflow");

        return res;
    }

    /** Returns the negation of the argument, throwing an exception if the result overflows an {@code int}. */
    public static int negateExact(int x) {
        int res = -x;

        if (x != 0 && x == res)
            throw new ArithmeticException(INTEGER.getName() + " overflow");

        return res;
    }

    /** Returns the negation of the argument, throwing an exception if the result overflows an {@code short}. */
    public static short negateExact(short x) {
        int res = -x;

        if (res > Short.MAX_VALUE)
            throw new ArithmeticException(SMALLINT.getName() + " overflow");

        return (short)res;
    }

    /** Returns the negation of the argument, throwing an exception if the result overflows an {@code byte}. */
    public static byte negateExact(byte x) {
        int res = -x;

        if (res > Byte.MAX_VALUE)
            throw new ArithmeticException(TINYINT.getName() + " overflow");

        return (byte)res;
    }

    /** Returns the difference of the arguments, throwing an exception if the result overflows an {@code long}.*/
    public static long subtractExact(long x, long y) {
        long r = x - y;

        if (((x ^ y) & (x ^ r)) < 0)
            throw new ArithmeticException(BIGINT.getName() + " overflow");

        return r;
    }

    /** Returns the difference of the arguments, throwing an exception if the result overflows an {@code int}.*/
    public static int subtractExact(int x, int y) {
        int r = x - y;

        if (((x ^ y) & (x ^ r)) < 0)
            throw new ArithmeticException(INTEGER.getName() + " overflow");

        return r;
    }

    /** Returns the difference of the arguments, throwing an exception if the result overflows an {@code short}.*/
    public static short subtractExact(short x, short y) {
        int r = x - y;

        if (r != (short)r)
            throw new ArithmeticException(SMALLINT.getName() + " overflow");

        return (short)r;
    }

    /** Returns the difference of the arguments, throwing an exception if the result overflows an {@code byte}.*/
    public static byte subtractExact(byte x, byte y) {
        int r = x - y;

        if (r != (byte)r)
            throw new ArithmeticException(TINYINT.getName() + " overflow");

        return (byte)r;
    }

    /** Returns the product of the arguments, throwing an exception if the result overflows an {@code long}. */
    public static long multiplyExact(long x, long y) {
        long r = x * y;
        long ax = Math.abs(x);
        long ay = Math.abs(y);

        if ((ax | ay) >>> 31 != 0 && ((y != 0 && r / y != x) || (x == Long.MIN_VALUE && y == -1)))
                throw new ArithmeticException(BIGINT.getName() + " overflow");

        return r;
    }

    /** Returns the product of the arguments, throwing an exception if the result overflows an {@code int}. */
    public static int multiplyExact(int x, int y) {
        long r = (long)x * (long)y;

        return convertToIntExact(r);
    }

    /** Returns the product of the arguments, throwing an exception if the result overflows an {@code short}. */
    public static short multiplyExact(short x, short y) {
        int r = x * y;

        if (r != (short)r)
            throw new ArithmeticException(SMALLINT.getName() + " overflow");

        return (short)r;
    }

    /** Returns the product of the arguments, throwing an exception if the result overflows an {@code byte}. */
    public static byte multiplyExact(byte x, byte y) {
        int r = x * y;

        if (r != (byte)r)
            throw new ArithmeticException(TINYINT.getName() + " overflow");

        return (byte)r;
    }

    /** Returns the quotient of the arguments, throwing an exception if the result overflows an {@code long}. */
    public static long divideExact(long x, long y) {
        if (y == -1)
            return negateExact(x);

        return x / y;
    }

    /** Returns the quotient of the arguments, throwing an exception if the result overflows an {@code int}. */
    public static int divideExact(int x, int y) {
        if (y == -1)
            return negateExact(x);

        return x / y;
    }

    /** Returns the quotient of the arguments, throwing an exception if the result overflows an {@code short}. */
    public static short divideExact(short x, short y) {
        if (y == -1)
            return negateExact(x);

        return (short)(x / y);
    }

    /** Returns the quotient of the arguments, throwing an exception if the result overflows an {@code byte}. */
    public static byte divideExact(byte x, byte y) {
        if (y == -1)
            return negateExact(x);

        return (byte)(x / y);
    }

    /** Cast value to {@code long}, throwing an exception if the result overflows an {@code long}. */
    public static long convertToLongExact(Number x) {
        BigDecimal rounded = checkLongBounds(x);

        if (rounded != null)
            return rounded.longValue();

        return convertToBigDecimal(x).setScale(0, NUMERIC_ROUNDING_MODE).longValue();
    }

    /** Cast value to {@code long}, throwing an exception if the result overflows an {@code long}. */
    public static long convertToLongExact(double x) {
        if (x < (Double)LONG_BOUNDS[2] || x > (Double)LONG_BOUNDS[3])
            throw new ArithmeticException(BIGINT.getName() + " overflow");

        return (long)round(x);
    }

    /** Cast value to {@code int}, throwing an exception if the result overflows an {@code int}. */
    public static int convertToIntExact(long x) {
        int res = (int)x;

        if (res != x)
            throw new ArithmeticException(INTEGER.getName() + " overflow");

        return res;
    }

    /** Cast value to {@code int}, throwing an exception if the result overflows an {@code int}. */
    public static int convertToIntExact(double x) {
        if (x < (Double)INT_BOUNDS[2] || x > (Double)INT_BOUNDS[3])
            throw new ArithmeticException(INTEGER.getName() + " overflow");

        return (int)round(x);
    }

    /** Cast value to {@code int}, throwing an exception if the result overflows an {@code int}. */
    public static int convertToIntExact(Number x) {
        BigDecimal rounded = checkIntegerBounds(x);

        if (rounded != null)
            return rounded.intValue();

        return convertToBigDecimal(x).setScale(0, NUMERIC_ROUNDING_MODE).intValue();
    }

    /** Cast value to {@code short}, throwing an exception if the result overflows an {@code short}. */
    public static short convertToShortExact(long x) {
        short res = (short)x;

        if (res != x)
            throw new ArithmeticException(SMALLINT.getName() + " overflow");

        return res;
    }

    /** Cast value to {@code short}, throwing an exception if the result overflows an {@code short}. */
    public static short convertToShortExact(double x) {
        if (x < (Double)SHORT_BOUNDS[2] || x > (Double)SHORT_BOUNDS[3])
            throw new ArithmeticException(SMALLINT.getName() + " overflow");

        return (short)round(x);
    }

    /** Cast value to {@code short}, throwing an exception if the result overflows an {@code short}. */
    public static short convertToShortExact(Number x) {
        BigDecimal rounded = checkShortBounds(x);

        if (rounded != null)
            return rounded.shortValue();

        return convertToBigDecimal(x).setScale(0, NUMERIC_ROUNDING_MODE).shortValue();
    }

    /** Cast value to {@code byte}, throwing an exception if the result overflows an {@code byte}. */
    public static byte convertToByteExact(long x) {
        byte res = (byte)x;

        if (res != x)
            throw new ArithmeticException(TINYINT.getName() + " overflow");

        return res;
    }

    /** Cast value to {@code byte}, throwing an exception if the result overflows an {@code byte}. */
    public static byte convertToByteExact(double x) {
        if (x < (Double)BYTE_BOUNDS[2] || x > (Double)BYTE_BOUNDS[3])
            throw new ArithmeticException(TINYINT.getName() + " overflow");

        return (byte)round(x);
    }

    /** Cast value to {@code byte}, throwing an exception if the result overflows an {@code byte}. */
    public static byte convertToByteExact(Number x) {
        BigDecimal rounded = checkByteBounds(x);

        if (rounded != null)
            return rounded.byteValue();

        return convertToBigDecimal(x).setScale(0, NUMERIC_ROUNDING_MODE).byteValue();
    }

    /** */
    public static BigDecimal convertToBigDecimal(Number val) {
        BigDecimal dec;

        if (val instanceof Float)
            dec = BigDecimal.valueOf(val.floatValue());
        else if (val instanceof Double)
            dec = BigDecimal.valueOf(val.doubleValue());
        else if (val instanceof BigDecimal)
            dec = (BigDecimal)val;
        else if (val instanceof BigInteger)
            dec = new BigDecimal((BigInteger)val);
        else
            dec = BigDecimal.valueOf(val.longValue());

        return dec;
    }

    /** */
    @Nullable private static BigDecimal checkLongBounds(Number x) {
        if (x instanceof BigDecimal) {
            BigDecimal rounded = ((BigDecimal)x).setScale(0, NUMERIC_ROUNDING_MODE);

            if (rounded.compareTo((BigDecimal)LONG_BOUNDS[4]) >= 0 && rounded.compareTo((BigDecimal)LONG_BOUNDS[5]) <= 0)
                return rounded;
        }
        else if (x instanceof Double) {
            if (((Double)x).compareTo((Double)LONG_BOUNDS[2]) < 0 && ((Double)x).compareTo((Double)LONG_BOUNDS[3]) > 0)
                return null;
        }
        else if (x instanceof Float) {
            if (((Float)x).compareTo((Float)LONG_BOUNDS[0]) < 0 && ((Float)x).compareTo((Float)LONG_BOUNDS[1]) > 0)
                return null;
        }
        else
            return null;

        throw new ArithmeticException(BIGINT.getName() + " overflow");
    }

    /** */
    @Nullable private static BigDecimal checkIntegerBounds(Number x) {
        if (x instanceof BigDecimal) {
            BigDecimal rounded = ((BigDecimal)x).setScale(0, NUMERIC_ROUNDING_MODE);

            if (rounded.compareTo((BigDecimal)INT_BOUNDS[4]) >= 0 && rounded.compareTo((BigDecimal)INT_BOUNDS[5]) <= 0)
                return rounded;
        }
        else if (x instanceof Double) {
            if (((Double)x).compareTo((Double)INT_BOUNDS[2]) < 0 && ((Double)x).compareTo((Double)INT_BOUNDS[3]) > 0)
                return null;
        }
        else if (x instanceof Float) {
            if (((Float)x).compareTo((Float)INT_BOUNDS[0]) < 0 && ((Float)x).compareTo((Float)INT_BOUNDS[1]) > 0)
                return null;
        }
        else {
            long longVal = x.longValue();

            if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE)
                return null;
        }

        throw new ArithmeticException(INTEGER.getName() + " overflow");
    }

    /** */
    @Nullable private static BigDecimal checkShortBounds(Number x) {
        if (x instanceof BigDecimal) {
            BigDecimal rounded = ((BigDecimal)x).setScale(0, NUMERIC_ROUNDING_MODE);

            if (rounded.compareTo((BigDecimal)SHORT_BOUNDS[4]) >= 0 && rounded.compareTo((BigDecimal)SHORT_BOUNDS[5]) <= 0)
                return rounded;
        }
        else if (x instanceof Double) {
            if (((Double)x).compareTo((Double)SHORT_BOUNDS[2]) < 0 && ((Double)x).compareTo((Double)SHORT_BOUNDS[3]) > 0)
                return null;
        }
        else if (x instanceof Float) {
            if (((Float)x).compareTo((Float)SHORT_BOUNDS[0]) < 0 && ((Float)x).compareTo((Float)SHORT_BOUNDS[1]) > 0)
                return null;
        }
        else {
            long longVal = x.longValue();

            if (longVal >= Short.MIN_VALUE && longVal <= Short.MAX_VALUE)
                return null;
        }

        throw new ArithmeticException(SMALLINT.getName() + " overflow");
    }

    /** */
    @Nullable private static BigDecimal checkByteBounds(Number x) {
        if (x instanceof BigDecimal) {
            BigDecimal rounded = ((BigDecimal)x).setScale(0, NUMERIC_ROUNDING_MODE);

            if (rounded.compareTo((BigDecimal)BYTE_BOUNDS[4]) >= 0 && rounded.compareTo((BigDecimal)BYTE_BOUNDS[5]) <= 0)
                return rounded;
        }
        else if (x instanceof Double) {
            if (((Double)x).compareTo((Double)BYTE_BOUNDS[2]) < 0 && ((Double)x).compareTo((Double)BYTE_BOUNDS[3]) > 0)
                return null;
        }
        else if (x instanceof Float) {
            if (((Float)x).compareTo((Float)BYTE_BOUNDS[0]) < 0 && ((Float)x).compareTo((Float)BYTE_BOUNDS[1]) > 0)
                return null;
        }
        else {
            long longVal = x.longValue();

            if (longVal >= Byte.MIN_VALUE && longVal <= Byte.MAX_VALUE)
                return null;
        }

        throw new ArithmeticException(TINYINT.getName() + " overflow");
    }

    /** */
    private static double round(double x) {
        return x < 0.0d ? x - 0.5d : x + 0.5d;
    }
}
