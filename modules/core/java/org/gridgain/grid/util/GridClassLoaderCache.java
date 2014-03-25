/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Caches class loaders for classes.
 */
public final class GridClassLoaderCache {
    /** Fields cache. */
    private static final ConcurrentMap<Class<?>, ClassLoader> cache =
        new ConcurrentHashMap8<>();

    /**
     * Gets cached ClassLoader for efficiency since class loader detection has proven to be slow.
     *
     * @param cls Class.
     * @return ClassLoader for the class.
     */
    public static ClassLoader classLoader(Class<?> cls) {
        ClassLoader cached = cache.get(cls);

        if (cached == null) {
            ClassLoader old = cache.putIfAbsent(cls, cached = detectClassLoader(cls));

            if (old != null)
                cached = old;
        }

        return cached;
    }

    /**
     * @param ldr Undeployed class loader.
     */
    public static void onUndeployed(ClassLoader ldr) {
        assert ldr != null;

        for (Map.Entry<Class<?>, ClassLoader> e : cache.entrySet()) {
            if (e.getValue().equals(ldr))
                cache.remove(e.getKey(), ldr);
        }
    }

    /**
     * Detects class loader for given class.
     * <p>
     * This method will first check if {@link Thread#getContextClassLoader()} is appropriate.
     * If yes, then context class loader will be returned, otherwise
     * the {@link Class#getClassLoader()} will be returned.
     *
     * @param cls Class to find class loader for.
     * @return Class loader for given class (never {@code null}).
     */
    private static ClassLoader detectClassLoader(Class<?> cls) {
        ClassLoader ctxClsLdr = Thread.currentThread().getContextClassLoader();

        ClassLoader clsLdr = cls.getClassLoader();

        if (clsLdr == null)
            clsLdr = U.gridClassLoader();

        if (U.p2pLoader(ctxClsLdr))
            return clsLdr;

        if (ctxClsLdr != null) {
            if (ctxClsLdr == clsLdr)
                return ctxClsLdr;

            try {
                // Check if context loader is wider than direct object class loader.
                Class<?> c = Class.forName(cls.getName(), true, ctxClsLdr);

                if (c == cls)
                    return ctxClsLdr;
            }
            catch (ClassNotFoundException ignored) {
                // No-op.
            }
        }

        ctxClsLdr = clsLdr;

        return ctxClsLdr;
    }

    /**
     *
     */
    public static void printMemoryStats() {
        X.println(">>>");
        X.println(">>> GridClassLoaderCache memory stats:");
        X.println(" Cache size: " + cache.size());

        for (Map.Entry<Class<?>, ClassLoader> e : cache.entrySet())
            X.println(" " + e.getKey() + " : " + e.getValue());
    }

    /**
     * Intended for test purposes only.
     */
    public static void clear() {
        cache.clear();
    }

    /**
     * Ensure singleton.
     */
    private GridClassLoaderCache() {
        // No-op.
    }
}
