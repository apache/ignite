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
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

/**
 * Utility class provides various method to work with collections.
 */
public final class CollectionUtils {
    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param col Collection to check.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean nullOrEmpty(@Nullable Collection<?> col) {
        return col == null || col.isEmpty();
    }

    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param c Collection to test.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean nullOrEmpty(@Nullable Iterable<?> c) {
        return c == null || (c instanceof Collection<?> ? ((Collection<?>) c).isEmpty() : !c.iterator().hasNext());
    }

    /**
     * Tests if the given collection is either {@code null} or empty.
     *
     * @param col Collection to check.
     * @return Whether or not the given collection is {@code null} or empty.
     */
    public static boolean nullOrEmpty(@Nullable Map<?, ?> col) {
        return col == null || col.isEmpty();
    }

    /**
     * Gets first element from given list or returns {@code null} if list is empty.
     *
     * @param list List to retrieve the first element.
     * @return The first element of the given list or {@code null} in case the list is empty.
     */
    public static <T> T first(List<? extends T> list) {
        if (nullOrEmpty(list))
            return null;

        return list.get(0);
    }

    /** Stub. */
    private CollectionUtils() {
        // No op.
    }
}
