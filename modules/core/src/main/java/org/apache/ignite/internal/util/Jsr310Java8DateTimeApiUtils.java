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

import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Provides utility functions for JSR-310 Java 8 Date and Time API types
 * based on reflection.
 */
public final class Jsr310Java8DateTimeApiUtils {
    /** Class<java.time.LocalTime>. */
    private static final Class<?> LOCAL_TIME_CLASS = U.classForName("java.time.LocalTime", null);

    /** Class<java.time.LocalDate>. */
    private static final Class<?> LOCAL_DATE_CLASS = U.classForName("java.time.LocalDate", null);

    /** Class<java.time.LocalDateTime>. */
    private static final Class<?> LOCAL_DATE_TIME_CLASS = U.classForName("java.time.LocalDateTime", null);

    /** JSR-310 API classes. */
    private static final Collection<Class<?>> JSR_310_API_CLASSES = createJsr310ApiClassesCollection();

    /**
     * Creates a collection of the available JSR-310 classes.
     *
     * @return Collection of the available JSR-310 classes.
     */
    @NotNull private static Collection<Class<?>> createJsr310ApiClassesCollection() {
        Collection<Class<?>> res = new ArrayList<>(3);

        if (LOCAL_DATE_CLASS != null)
            res.add(LOCAL_DATE_CLASS);

        if (LOCAL_TIME_CLASS != null)
            res.add(LOCAL_TIME_CLASS);

        if (LOCAL_DATE_TIME_CLASS != null)
            res.add(LOCAL_DATE_TIME_CLASS);

        return res;
    }

    /**
     * Default private constructor.
     *
     * <p>Prevents creation of instances of this class.</p>
     */
    private Jsr310Java8DateTimeApiUtils() {
        // No-op
    }

    /**
     * Returns the available JSR-310 classes.
     *
     * @return Available JSR-310 classes.
     */
    @NotNull public static Collection<Class<?>> jsr310ApiClasses() {
        return JSR_310_API_CLASSES;
    }
}
