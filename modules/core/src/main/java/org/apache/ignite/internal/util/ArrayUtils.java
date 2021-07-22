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

package org.apache.ignite.internal.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class provides various method for manipulating arrays.
 */
@SuppressWarnings("SwitchStatementWithTooFewBranches")
public final class ArrayUtils {
    /** Empty array of byte. */
    public static final byte[] BYTE_EMPTY_ARRAY = new byte[0];

    /** Empty array of short. */
    public static final short[] SHORT_EMPTY_ARRAY = new short[0];

    /** Empty array of int. */
    public static final int[] INT_EMPTY_ARRAY = new int[0];

    /** Empty array of long. */
    public static final long[] LONG_EMPTY_ARRAY = new long[0];

    /** Empty array of float. */
    public static final float[] FLOAT_EMPTY_ARRAY = new float[0];

    /** Empty array of double. */
    public static final double[] DOUBLE_EMPTY_ARRAY = new double[0];

    /** Empty array of char. */
    public static final char[] CHAR_EMPTY_ARRAY = new char[0];

    /** Empty array of boolean. */
    public static final boolean[] BOOLEAN_EMPTY_ARRAY = new boolean[0];

    /** Empty object array. */
    public static final Object[] OBJECT_EMPTY_ARRAY = new Object[0];

    /** */
    public static final ArrayFactory<byte[]> BYTE_ARRAY = new ArrayFactory<>() {
        @Override public byte[] of(int len) {
            if (len < 0)
                throw new IgniteInternalException("Read invalid byte array length: " + len);

            switch (len) {
                case 0:
                    return BYTE_EMPTY_ARRAY;

                default:
                    return new byte[len];
            }
        }
    };

    /** */
    public static final ArrayFactory<short[]> SHORT_ARRAY = new ArrayFactory<>() {
        @Override public short[] of(int len) {
            if (len < 0)
                throw new IgniteInternalException("Read invalid short array length: " + len);

            switch (len) {
                case 0:
                    return SHORT_EMPTY_ARRAY;

                default:
                    return new short[len];
            }
        }
    };

    /** */
    public static final ArrayFactory<int[]> INT_ARRAY = new ArrayFactory<>() {
        @Override public int[] of(int len) {
            if (len < 0)
                throw new IgniteInternalException("Read invalid int array length: " + len);

            switch (len) {
                case 0:
                    return INT_EMPTY_ARRAY;

                default:
                    return new int[len];
            }
        }
    };

    /** */
    public static final ArrayFactory<long[]> LONG_ARRAY = new ArrayFactory<>() {
        @Override public long[] of(int len) {
            if (len < 0)
                throw new IgniteInternalException("Read invalid long array length: " + len);

            switch (len) {
                case 0:
                    return LONG_EMPTY_ARRAY;

                default:
                    return new long[len];
            }
        }
    };

    /** */
    public static final ArrayFactory<float[]> FLOAT_ARRAY = new ArrayFactory<>() {
        @Override public float[] of(int len) {
            if (len < 0)
                throw new IgniteInternalException("Read invalid float array length: " + len);

            switch (len) {
                case 0:
                    return FLOAT_EMPTY_ARRAY;

                default:
                    return new float[len];
            }
        }
    };

    /** */
    public static final ArrayFactory<double[]> DOUBLE_ARRAY = new ArrayFactory<>() {
        @Override public double[] of(int len) {
            if (len < 0)
                throw new IgniteInternalException("Read invalid double array length: " + len);

            switch (len) {
                case 0:
                    return DOUBLE_EMPTY_ARRAY;

                default:
                    return new double[len];
            }
        }
    };

    /** */
    public static final ArrayFactory<char[]> CHAR_ARRAY = new ArrayFactory<>() {
        @Override public char[] of(int len) {
            if (len < 0)
                throw new IgniteInternalException("Read invalid char array length: " + len);

            switch (len) {
                case 0:
                    return CHAR_EMPTY_ARRAY;

                default:
                    return new char[len];
            }
        }
    };

    /** */
    public static final ArrayFactory<boolean[]> BOOLEAN_ARRAY = new ArrayFactory<>() {
        @Override public boolean[] of(int len) {
            if (len < 0)
                throw new IgniteInternalException("Read invalid boolean array length: " + len);

            switch (len) {
                case 0:
                    return BOOLEAN_EMPTY_ARRAY;

                default:
                    return new boolean[len];
            }
        }
    };

    /**
     * @param arr Array to check.
     * @param <T> Array element type.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static <T> boolean nullOrEmpty(T[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(byte[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(short[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(int[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(long[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(float[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(double[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * @param arr Array to check.
     * @return {@code true} if {@code null} or an empty array is provided, {@code false} otherwise.
     */
    public static boolean nullOrEmpty(boolean[] arr) {
        return arr == null || arr.length == 0;
    }

    /**
     * Converts array to {@link List}. Note that resulting list cannot
     * be altered in size, as it it based on the passed in array -
     * only current elements can be changed.
     * <p>
     * Note that unlike {@link Arrays#asList(Object[])}, this method is
     * {@code null}-safe. If {@code null} is passed in, then empty list
     * will be returned.
     *
     * @param vals Array of values
     * @param <T> Array type.
     * @return {@link List} instance for array.
     */
    @SafeVarargs
    public static <T> List<T> asList(@Nullable T... vals) {
        return nullOrEmpty(vals) ? Collections.emptyList() : Arrays.asList(vals);
    }

    /**
     * Concatenates an elements to an array.
     *
     * @param arr Array.
     * @param obj One or more elements.
     * @param <T> Type of the elements of the array.
     * @return Concatenated array.
     */
    @SafeVarargs
    public static <T> T[] concat(@Nullable T[] arr, T... obj) {
        T[] newArr;

        if (arr == null || arr.length == 0)
            newArr = obj;
        else {
            newArr = Arrays.copyOf(arr, arr.length + obj.length);

            System.arraycopy(obj, 0, newArr, arr.length, obj.length);
        }

        return newArr;
    }

    /**
     * Stub.
     */
    private ArrayUtils() {
        // No op.
    }
}
