/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.resource;

import java.util.Map;

/**
 * Resource test utilities.
 */
final class GridResourceTestUtils {
    /**
     * Ensures singleton.
     */
    private GridResourceTestUtils() {
        // No-op.
    }

    /**
     * @param usage Usage map.
     * @param cls Class to check usage for.
     * @param cnt Expected count.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    public static void checkUsageCount(Map<Class<?>, Integer> usage, Class<?> cls, int cnt) {
        Integer used;

        synchronized (usage) {
            used = usage.get(cls);
        }

        if (used == null)
            used = 0;

        assert used == cnt : "Invalid count [expected=" + cnt + ", actual=" + used + ", usageMap=" + usage + ']';
    }
}