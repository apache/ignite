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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Caches class loaders for classes.
 */
public final class GridClassLoaderCache {
    /** Fields cache. */
    private static final ConcurrentMap<Class<?>, ClassLoader> cache =
        new ConcurrentHashMap<>();

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

        return clsLdr;
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