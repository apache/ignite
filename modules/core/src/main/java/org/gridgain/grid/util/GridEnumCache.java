/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.jdk8.backport.*;

import java.util.concurrent.*;

/**
 * Cache for enum constants.
 */
public class GridEnumCache {
    /** Cache for enum constants. */
    private static final ConcurrentMap<Class<?>, Object[]> ENUM_CACHE = new ConcurrentHashMap8<>();

    /**
     * Gets enum constants for provided class.
     *
     * @param cls Class.
     * @return Enum constants.
     */
    public static Object[] get(Class<?> cls) {
        assert cls != null;

        Object[] vals = ENUM_CACHE.get(cls);

        if (vals == null) {
            vals = cls.getEnumConstants();

            ENUM_CACHE.putIfAbsent(cls, vals);
        }

        return vals;
    }

    /**
     * Clears cache.
     */
    public static void clear() {
        ENUM_CACHE.clear();
    }
}
