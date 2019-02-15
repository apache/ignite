/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
