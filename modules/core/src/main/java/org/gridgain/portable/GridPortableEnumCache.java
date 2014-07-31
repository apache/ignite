/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.portable;

import org.jdk8.backport.*;

import java.util.concurrent.*;

/**
 * Portable reader's cache for enum constants.
 */
public class GridPortableEnumCache {
    /** Cache for enum constants. */
    private static final ConcurrentMap<Class<?>, Object[]> ENUM_CACHE = new ConcurrentHashMap8<>();

    /**
     * Gets enum constants for specified class object.
     * @param cls Class.
     * @return Cached enum constants for specified class object.
     */
    public static Object[] get(Class<?> cls) {
        Object[] consts = ENUM_CACHE.get(cls);

        if (consts == null) {
            consts = cls.getEnumConstants();

            ENUM_CACHE.putIfAbsent(cls, consts);
        }

        return consts;
    }

    /**
     * Clears enum cache.
     */
    public static void clearCache() {
        ENUM_CACHE.clear();
    }
}
