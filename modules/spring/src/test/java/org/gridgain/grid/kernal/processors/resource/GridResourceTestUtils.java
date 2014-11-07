/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import java.util.*;

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

        if (used == null) {
            used = 0;
        }

        assert used == cnt : "Invalid count [expected=" + cnt + ", actual=" + used + ", usageMap=" + usage + ']';
    }
}
