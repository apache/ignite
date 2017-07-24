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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;

/**
 * Provides conversion functions for JSR-310 Java 8 Date and Time API types
 * based on reflection.
 *
 * <p>This class can be used to provide support for
 * Java 8 Date and Time API types in the code compiled by the compiler versions
 * prior to Java 8.</p>
 */
public final class Java8DateTimeApiUtils {
    /** Class<java.time.LocalDateTime>. */
    private static final Class<?> LOCAL_DATE_TIME_CLASS = tryGetClass("java.time.LocalDateTime");

    /** Is Java 8 Date and Time API is available. */
    private static final boolean JAVA8_DATE_TIME_API_AVAILABLE = LOCAL_DATE_TIME_CLASS != null;

    /** java.sql.Timestamp#valueOf(LocalDateTime). */
    private static final Method TIMESTAMP_FROM_LOCAL_DATE_TIME =
        getMethodIfJava8DateTimeApiAvailable(Timestamp.class, "valueOf", LOCAL_DATE_TIME_CLASS);

    /**
     * Default private constructor.
     *
     * <p>Prevents creation of instances of this class.</p>
     */
    private Java8DateTimeApiUtils() {
    }

    /**
     * Returns a class by its name if possible.
     *
     * @param clsName Class name.
     * @return Class for the specified class name or {@code null} if the class cannot be found.
     */
    private static Class<?> tryGetClass(String clsName) {
        try {
            return Class.forName(clsName);
        } catch (ClassNotFoundException ignored) {
            return null;
        }
    }

    /**
     * Returns a Method for the specified class and method signature
     * if Java 8 Date and Time API is available.
     *
     * @param clazz Class.
     * @param mtdName Method name.
     * @param paramTypes Parameter types.
     * @return Method for the specified class and method signature if
     * Java 8 Date and time API is available, {@code null} otherwise .
     * @throws IllegalStateException If a method with the specified name
     * cannot be found.
     */
    private static Method getMethodIfJava8DateTimeApiAvailable(
        Class<?> clazz, String mtdName, Class<?>... paramTypes
    ) {
        if (!JAVA8_DATE_TIME_API_AVAILABLE)
            return null;

        try {
            return clazz.getMethod(mtdName, paramTypes);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Java 8 or later but method " + clazz.getName() + "#" + mtdName + "(" +
                Arrays.toString(paramTypes) + ") is missing", e);
        }
    }

    /**
     * Returns whether Java 8 Date and Time API is available.
     *
     * @return Whether Java 8 Date and Time API is available.
     */
    public static boolean java8DateTimeApiAvailable() {
        return JAVA8_DATE_TIME_API_AVAILABLE;
    }

    /**
     * Returns whether the specified class is {@link java.time.LocalDateTime}.
     *
     * @param clazz Class to check.
     * @return whether the specified class is {@link java.time.LocalDateTime}.
     */
    public static boolean isLocalDateTime(Class<?> clazz) {
        return clazz == LOCAL_DATE_TIME_CLASS;
    }

    /**
     * Converts the specified {@link java.time.LocalDateTime} object to {@link java.sql.Timestamp}.
     *
     * <p>This method should be called only if Java 8 Date and Time API is available.</p>
     *
     * @param locDateTime {@link java.time.LocalDateTime} object.
     * @return Converted object.
     * @throws IllegalStateException If Java 8 Date and Time API is not available.
     * @throws IgniteCheckedException If the conversion cannot be done.
     */
    public static Timestamp localDateTimeToTimestamp(Object locDateTime) throws IgniteCheckedException {
        if (!JAVA8_DATE_TIME_API_AVAILABLE)
            throw new IllegalStateException("Java 8 Date and Time API is not available");

        try {
            return (Timestamp)TIMESTAMP_FROM_LOCAL_DATE_TIME.invoke(null, locDateTime);
        }
        catch (IllegalAccessException | InvocationTargetException ignored) {
            throw new IgniteCheckedException("Failed to convert to " + Timestamp.class);
        }
    }
}
