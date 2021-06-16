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

package org.apache.ignite.internal.configuration;

import org.jetbrains.annotations.Nullable;

/**
 * Helper methods for working with reflection API.
 */
public class TypeUtils {
    /**
     * Returns the boxed version of the given class, if it is a primitive.
     *
     * @param clazz type to get the boxed version for
     * @return {@code Class} of the boxed type or {@code null} if the given {@code clazz} does not represent a
     * primitive type
     */
    @Nullable
    public static Class<?> boxed(Class<?> clazz) {
        if (clazz == boolean.class)
            return Boolean.class;
        else if (clazz == byte.class)
            return Byte.class;
        else if (clazz == short.class)
            return Short.class;
        else if (clazz == int.class)
            return Integer.class;
        else if (clazz == long.class)
            return Long.class;
        else if (clazz == char.class)
            return Character.class;
        else if (clazz == float.class)
            return Float.class;
        else if (clazz == double.class)
            return Double.class;
        else
            return null;
    }

    /**
     * Returns the {@code Class} that represents the primitive type that has been wrapped by the given {@code clazz}.
     *
     * @param clazz class to get the unboxed version for
     * @return {@code Class} of the primitive type or {@code null} if the given {@code clazz} does not represent a
     * boxed primitive type
     */
    @Nullable
    public static Class<?> unboxed(Class<?> clazz) {
        if (clazz == Boolean.class)
            return boolean.class;
        else if (clazz == Byte.class)
            return byte.class;
        else if (clazz == Short.class)
            return short.class;
        else if (clazz == Integer.class)
            return int.class;
        else if (clazz == Long.class)
            return long.class;
        else if (clazz == Character.class)
            return char.class;
        else if (clazz == Float.class)
            return float.class;
        else if (clazz == Double.class)
            return double.class;
        else
            return null;
    }
}
