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

package org.apache.ignite.internal.processors.license;

import org.apache.ignite.internal.util.*;
import org.jdk8.backport.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Usage registry.
 */
public class GridLicenseUseRegistry {
    /** Usage map. */
    private static final ConcurrentMap<GridLicenseSubsystem, Collection<Class<?>>> useMap =
        new ConcurrentHashMap8<>();

    /**
     * Ensure singleton.
     */
    private GridLicenseUseRegistry() {
        // No-op.
    }

    /**
     * Callback for whenever component gets used.
     *
     * @param ed Subsystem.
     * @param cls Component.
     */
    public static void onUsage(GridLicenseSubsystem ed, Class<?> cls) {
        Collection<Class<?>> c = useMap.get(ed);

        if (c == null) {
            Collection<Class<?>> old = useMap.putIfAbsent(ed, c = new GridConcurrentHashSet<>());

            if (old != null)
                c = old;
        }

        c.add(cls);
    }

    /**
     * Gets used subsystems for given subsystem.
     *
     * @param ed Subsystem.
     * @return Component.
     */
    public static Collection<Class<?>> usedClasses(GridLicenseSubsystem ed) {
        Collection<Class<?>> c = useMap.get(ed);

        return c == null ? Collections.<Class<?>>emptySet() : c;
    }

    /**
     * Checks if subsystem is used.
     *
     * @param ed Subsystem to check.
     * @return {@code True} if used.
     */
    public static boolean used(GridLicenseSubsystem ed) {
        return !usedClasses(ed).isEmpty();
    }

    /**
     * Clears usages.
     *
     * FOR TESTING PURPOSES ONLY!
     */
    public static void clear() {
        useMap.clear();
    }
}
