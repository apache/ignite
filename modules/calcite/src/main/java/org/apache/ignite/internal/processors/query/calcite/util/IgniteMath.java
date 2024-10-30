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
import org.apache.calcite.sql.type.SqlTypeName;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;

/** Math operations with overflow checking. */
public class IgniteMath {
    /** */
    private static final BigDecimal UPPER_LONG_BIG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);

    /** */
    private static final BigDecimal LOWER_LONG_BIG_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE);

    /** */
    private static final Double UPPER_LONG_DOUBLE = (double)Long.MAX_VALUE;

    /** */
    private static final Double LOWER_LONG_DOUBLE = (double)Long.MIN_VALUE;

    /** */
    private static final Float UPPER_LONG_FLOAT = (float)Long.MAX_VALUE;

    /** */
    private static final Float LOWER_LONG_FLOAT = (float)Long.MIN_VALUE;

    /** */
    public static final RoundingMode NUMERIC_ROUNDING_MODE = RoundingMode.HALF_UP;

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
    private static void checkNumberLongBounds(SqlTypeName type, Number x) {
        if (x instanceof BigDecimal) {
            if ((((BigDecimal)x).compareTo(UPPER_LONG_BIG_DECIMAL) < 0 && ((BigDecimal)x).compareTo(LOWER_LONG_BIG_DECIMAL) > 0))
                return;
        }
        else if (x instanceof Double) {
            if ((((Double)x).compareTo(UPPER_LONG_DOUBLE) <= 0 && ((Double)x).compareTo(LOWER_LONG_DOUBLE) >= 0))
                return;
        }
        else if (x instanceof Float) {
            if ((((Float)x).compareTo(UPPER_LONG_FLOAT) <= 0 && ((Float)x).compareTo(LOWER_LONG_FLOAT) >= 0))
                return;
        }
        else
            return;

        throw new ArithmeticException(type.getName() + " overflow");
    }

    /** Cast value to {@code long}, throwing an exception if the result overflows an {@code long}. */
    public static long convertToLongExact(Number x) {
        checkNumberLongBounds(BIGINT, x);

        return convertToBigDecimal(x).setScale(0, NUMERIC_ROUNDING_MODE).longValue();
    }

    /** Cast value to {@code long}, throwing an exception if the result overflows an {@code long}. */
    public static long convertToLongExact(double x) {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE)
            throw new ArithmeticException(BIGINT.getName() + " overflow");

        assert NUMERIC_ROUNDING_MODE == RoundingMode.HALF_UP
            : "Converting primitive double to long isn't implemented with global rounding " + RoundingMode.HALF_UP;

        return (long)(x < 0.0d ? x - 0.5d : x + 0.5d);
    }

    /** Cast value to {@code int}, throwing an exception if the result overflows an {@code int}. */
    public static int convertToIntExact(long x) {
        return checkIntPrimitiveLimit(x);
    }

    /** Cast value to {@code int}, throwing an exception if the result overflows an {@code int}. */
    public static int convertToIntExact(double x) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE)
            throw new ArithmeticException(INTEGER.getName() + " overflow");

        assert NUMERIC_ROUNDING_MODE == RoundingMode.HALF_UP
            : "Converting primitive double to int isn't implemented with global rounding " + RoundingMode.HALF_UP;

        return (int)(x < 0.0d ? x - 0.5d : x + 0.5d);
    }

    /** Cast value to {@code int}, throwing an exception if the result overflows an {@code int}. */
    public static int convertToIntExact(Number val) {
        checkNumberLongBounds(INTEGER, val);

        checkIntPrimitiveLimit(val.longValue());

        return convertToBigDecimal(val).setScale(0, NUMERIC_ROUNDING_MODE).intValue();
    }

    /** */
    private static int checkIntPrimitiveLimit(long val) {
        int res = (int)val;

        if (res != val)
            throw new ArithmeticException(INTEGER.getName() + " overflow");

        return res;
    }

    /** */
    private static short checkShortPrimitiveLimit(long val) {
        short res = (short)val;

        if (res != val)
            throw new ArithmeticException(SMALLINT.getName() + " overflow");

        return res;
    }

    /** */
    private static byte checkBytePrimitiveLimit(long val) {
        byte res = (byte)val;

        if (res != val)
            throw new ArithmeticException(TINYINT.getName() + " overflow");

        return res;
    }

    /** Cast value to {@code short}, throwing an exception if the result overflows an {@code short}. */
    public static short convertToShortExact(long val) {
        return checkShortPrimitiveLimit(val);
    }

    /** Cast value to {@code short}, throwing an exception if the result overflows an {@code short}. */
    public static short convertToShortExact(double x) {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE)
            throw new ArithmeticException(SMALLINT.getName() + " overflow");

        assert NUMERIC_ROUNDING_MODE == RoundingMode.HALF_UP
            : "Converting primitive double to short isn't implemented with global rounding " + RoundingMode.HALF_UP;

        return (short)(x < 0.0d ? x - 0.5d : x + 0.5d);
    }

    /** Cast value to {@code short}, throwing an exception if the result overflows an {@code short}. */
    public static short convertToShortExact(Number val) {
        checkNumberLongBounds(SMALLINT, val);

        checkShortPrimitiveLimit(val.longValue());

        return convertToBigDecimal(val).setScale(0, NUMERIC_ROUNDING_MODE).shortValue();
    }

    /** Cast value to {@code byte}, throwing an exception if the result overflows an {@code byte}. */
    public static byte convertToByteExact(long val) {
        return checkBytePrimitiveLimit(val);
    }

    /** Cast value to {@code byte}, throwing an exception if the result overflows an {@code byte}. */
    public static byte convertToByteExact(double x) {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE)
            throw new ArithmeticException(TINYINT.getName() + " overflow");

        assert NUMERIC_ROUNDING_MODE == RoundingMode.HALF_UP
            : "Converting primitive double to byte isn't implemented with global rounding " + RoundingMode.HALF_UP;

        return (byte)(x < 0.0d ? x - 0.5d : x + 0.5d);
    }

    /** Cast value to {@code byte}, throwing an exception if the result overflows an {@code byte}. */
    public static byte convertToByteExact(Number val) {
        checkNumberLongBounds(TINYINT, val);

        checkBytePrimitiveLimit(val.longValue());

        return convertToBigDecimal(val).setScale(0, NUMERIC_ROUNDING_MODE).byteValue();
    }
}
