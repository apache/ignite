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

package org.apache.ignite.marshaller;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;

/**
 * Controls what classes should be excluded from marshalling by default.
 */
public final class MarshallerExclusions {
    /**
     * Classes that must be included in serialization. All marshallers must
     * included these classes.
     * <p>
     * Note that this list supersedes {@link #EXCL_CLASSES}.
     */
    private static final Set<Class<?>> INCL_CLASSES = new HashSet<>();

    /** */
    private static volatile Map<Class<?>, Boolean> cache = new GridBoundedConcurrentLinkedHashMap<>(
        512, 512, 0.75f, 16);

    /**
     * Excluded grid classes from serialization. All marshallers must omit
     * these classes. Fields of these types should be serialized as {@code null}.
     * <p>
     * Note that {@link #INCL_CLASSES} supersedes this list.
     */
    private static final Set<Class<?>> EXCL_CLASSES = new HashSet<>();

    /**
     * Ensures singleton.
     */
    private MarshallerExclusions() {
        // No-op.
    }

    /**
     * Checks given class against predefined set of excluded types.
     *
     * @param cls Class to check.
     * @return {@code true} if class should be excluded, {@code false} otherwise.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private static boolean isExcluded0(Class<?> cls) {
        assert cls != null;

        for (Class<?> inclCls : INCL_CLASSES)
            if (inclCls.isAssignableFrom(cls))
                return false;

        for (Class<?> exclCls : EXCL_CLASSES)
            if (exclCls.isAssignableFrom(cls))
                return true;

        return false;
    }

    /**
     * Checks whether or not given class should be excluded from marshalling.
     *
     * @param cls Class to check.
     * @return {@code true} if class should be excluded, {@code false} otherwise.
     */
    public static boolean isExcluded(Class<?> cls) {
        Boolean res = cache.get(cls);

        if (res == null) {
            res = isExcluded0(cls);

            cache.put(cls, res);
        }

        return res;
    }

    /**
     * Intended for test purposes only.
     */
    public static void clearCache() {
        cache = new GridBoundedConcurrentLinkedHashMap<>(512, 512, 0.75f, 16);
    }

    /**
     * Adds Ignite class included in serialization.
     * @param cls Class to add.
     */
    public static void exclude(Class<?> cls) {
        EXCL_CLASSES.add(cls);
    }

    /**
     * Adds Ignite class included in serialization.
     * @param cls Class to add.
     */
    public static void include(Class<?> cls) {
        INCL_CLASSES.add(cls);
    }
}
