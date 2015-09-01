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

import java.util.Collection;
import org.jetbrains.annotations.Nullable;

/**
 * This class encapsulates argument check (null and range) for public facing APIs. Unlike asserts
 * it throws "normal" exceptions with standardized messages.
 */
public class GridArgumentCheck {
    /** Null pointer error message prefix. */
    public static final String NULL_MSG_PREFIX = "Ouch! Argument cannot be null: ";

    /** Invalid argument error message prefix. */
    private static final String INVALID_ARG_MSG_PREFIX = "Ouch! Argument is invalid: ";

    /** Not empty argument error message suffix. */
    private static final String NOT_EMPTY_SUFFIX = " must not be empty.";

    /** Not null or empty error message suffix. */
    private static final String NOT_NULL_OR_EMPTY_SUFFIX = " must not be null or empty.";

    /**
     * Checks if given argument value is not {@code null}. Otherwise - throws {@link NullPointerException}.
     *
     * @param val Argument value to check.
     * @param name Name of the argument in the code (used in error message).
     */
    public static void notNull(@Nullable Object val, String name) {
        if (val == null)
            throw new NullPointerException(NULL_MSG_PREFIX + name);
    }

    /**
     * Checks that none of the given values are {@code null}. Otherwise - throws {@link NullPointerException}.
     *
     * @param val1 1st argument value to check.
     * @param name1 Name of the 1st argument in the code (used in error message).
     * @param val2 2nd argument value to check.
     * @param name2 Name of the 2nd argument in the code (used in error message).
     */
    public static void notNull(Object val1, String name1, Object val2, String name2) {
        notNull(val1, name1);
        notNull(val2, name2);
    }

    /**
     * Checks that none of the given values are {@code null}. Otherwise - throws {@link NullPointerException}.
     *
     * @param val1 1st argument value to check.
     * @param name1 Name of the 1st argument in the code (used in error message).
     * @param val2 2nd argument value to check.
     * @param name2 Name of the 2nd argument in the code (used in error message).
     * @param val3 3rd argument value to check.
     * @param name3 Name of the 3rd argument in the code (used in error message).
     */
    public static void notNull(Object val1, String name1, Object val2, String name2, Object val3, String name3) {
        notNull(val1, name1);
        notNull(val2, name2);
        notNull(val3, name3);
    }

    /**
     * Checks that none of the given values are {@code null}. Otherwise - throws {@link NullPointerException}.
     *
     * @param val1 1st argument value to check.
     * @param name1 Name of the 1st argument in the code (used in error message).
     * @param val2 2nd argument value to check.
     * @param name2 Name of the 2nd argument in the code (used in error message).
     * @param val3 3rd argument value to check.
     * @param name3 Name of the 3rd argument in the code (used in error message).
     * @param val4 4th argument value to check.
     * @param name4 Name of the 4th argument in the code (used in error message).
     */
    public static void notNull(Object val1, String name1, Object val2, String name2, Object val3, String name3,
        Object val4, String name4) {
        notNull(val1, name1);
        notNull(val2, name2);
        notNull(val3, name3);
        notNull(val4, name4);
    }

    /**
     * Checks if given argument's condition is equal to {@code true}, otherwise
     * throws {@link IllegalArgumentException} exception.
     *
     * @param cond Argument's value condition to check.
     * @param desc Description of the condition to be used in error message.
     */
    public static void ensure(boolean cond, String desc) {
        if (!cond)
            throw new IllegalArgumentException(INVALID_ARG_MSG_PREFIX + desc);
    }

    /**
     * Checks that given collection is not empty.
     *
     * @param c Collection.
     * @param name Argument name.
     */
    public static void notEmpty(Collection<?> c, String name) {
        notNull(c, name);

        if (c.isEmpty())
            throw new IllegalArgumentException(INVALID_ARG_MSG_PREFIX + name + NOT_EMPTY_SUFFIX);
    }

    /**
     * Checks that given array is not empty.
     *
     * @param arr Array.
     * @param name Argument name.
     */
    public static void notEmpty(Object[] arr, String name) {
        notNull(arr, name);

        if (arr.length == 0)
            throw new IllegalArgumentException(INVALID_ARG_MSG_PREFIX + name + NOT_EMPTY_SUFFIX);
    }

    /**
     * Checks that given array is not empty.
     *
     * @param arr Array.
     * @param name Argument name.
     */
    public static void notEmpty(int[] arr, String name) {
        notNull(arr, name);

        if (arr.length == 0)
            throw new IllegalArgumentException(INVALID_ARG_MSG_PREFIX + name + NOT_EMPTY_SUFFIX);
    }

    /**
     * Checks that a String is not null or empty.
     *
     * @param value Value to check.
     * @param name Argument name.
     */
    public static void notNullOrEmpty(String value, String name) {
        notNull(value, name);

        if (value.trim().length() == 0) {
            throw new IllegalArgumentException(INVALID_ARG_MSG_PREFIX + name + NOT_NULL_OR_EMPTY_SUFFIX);
        }
    }
}