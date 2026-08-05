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
package org.apache.ignite.internal.marshaller;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.processors.cache.CacheClassLoaderMarker;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.Marshallers;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ClassLoaderUtils {
    /** Class loader used to load Ignite. */
    private static final ClassLoader gridClassLoader = ClassLoaderUtils.class.getClassLoader();

    /** Primitive class map. */
    private static final Map<String, Class<?>> primitiveMap = new HashMap<>(16, .5f);

    /** */
    private static final ConcurrentMap<ClassLoader, ConcurrentMap<String, Class>> classCache =
        new ConcurrentHashMap<>();

    static {
        primitiveMap.put("byte", byte.class);
        primitiveMap.put("short", short.class);
        primitiveMap.put("int", int.class);
        primitiveMap.put("long", long.class);
        primitiveMap.put("float", float.class);
        primitiveMap.put("double", double.class);
        primitiveMap.put("char", char.class);
        primitiveMap.put("boolean", boolean.class);
        primitiveMap.put("void", void.class);
    }

    /**
     * @return Class loader used to load Ignite itself.
     */
    public static ClassLoader gridClassLoader() {
        return gridClassLoader;
    }

    /**
     * @param ldr Custom class loader.
     * @param cfgLdr Class loader from config.
     * @return ClassLoader passed as param in case it is not null or cfgLdr  in case it is not null or ClassLoader used to start Ignite.
     */
    public static ClassLoader resolveClassLoader(@Nullable ClassLoader ldr, @Nullable ClassLoader cfgLdr) {
        return (ldr != null && ldr != gridClassLoader)
            ? ldr
            : cfgLdr != null
                ? cfgLdr
                : gridClassLoader;
    }

    /**
     * Tests whether given class is loadable with provided class loader.
     *
     * @param clsName Class name to test.
     * @param ldr Class loader to test with. If {@code null} - we'll use system class loader instead.
     *      If System class loader is not set - this method will return {@code false}.
     * @return {@code True} if class is loadable, {@code false} otherwise.
     */
    public static boolean isLoadableBy(String clsName, @Nullable ClassLoader ldr) {
        assert clsName != null;

        if (ldr == null)
            ldr = gridClassLoader;

        String lambdaParent = lambdaEnclosingClassName(clsName);

        try {
            ldr.loadClass(lambdaParent == null ? clsName : lambdaParent);

            return true;
        }
        catch (ClassNotFoundException ignore) {
            return false;
        }
    }

    /**
     * Gets class for the given name if it can be loaded or default given class.
     *
     * @param cls Class.
     * @param dflt Default class to return.
     * @return Class or default given class if it can't be found.
     */
    @Nullable public static Class<?> classForName(@Nullable String cls, @Nullable Class<?> dflt) {
        return classForName(cls, dflt, false);
    }

    /**
     * Gets class for the given name if it can be loaded or default given class.
     *
     * @param cls Class.
     * @param dflt Default class to return.
     * @param includePrimitiveTypes Whether class resolution should include primitive types
     *                              (i.e. "int" will resolve to int.class if flag is set)
     * @return Class or default given class if it can't be found.
     */
    @Nullable public static Class<?> classForName(
        @Nullable String cls,
        @Nullable Class<?> dflt,
        boolean includePrimitiveTypes
    ) {
        Class<?> clazz;
        if (cls == null)
            clazz = dflt;
        else if (!includePrimitiveTypes || cls.length() > 7 || (clazz = primitiveMap.get(cls)) == null) {
            try {
                clazz = Class.forName(cls);
            }
            catch (ClassNotFoundException ignore) {
                clazz = dflt;
            }
        }
        return clazz;
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @param useCache If true class loader and result should be cached internally, false otherwise.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(
        String clsName,
        @Nullable ClassLoader ldr,
        boolean useCache
    ) throws ClassNotFoundException {
        assert clsName != null;

        Class<?> cls = primitiveMap.get(clsName);

        if (cls != null)
            return cls;

        if (ldr != null) {
            if (!useCache) {
                cls = Class.forName(clsName, true, ldr);

                return cls;
            }
        }
        else
            ldr = gridClassLoader;

        if (!useCache) {
            cls = Class.forName(clsName, true, ldr);

            return cls;
        }

        ConcurrentMap<String, Class> ldrMap = classCache.get(ldr);

        if (ldrMap == null) {
            ConcurrentMap<String, Class> old = classCache.putIfAbsent(ldr, ldrMap = new ConcurrentHashMap<>());

            if (old != null)
                ldrMap = old;
        }

        cls = ldrMap.get(clsName);

        if (cls == null) {
            if (!MarshallerUtils.classNameFilter().apply(clsName))
                throw new ClassNotFoundException("Deserialization of class " + clsName + " is disallowed.");

            // Avoid class caching inside Class.forName
            if (ldr instanceof CacheClassLoaderMarker)
                cls = ldr.loadClass(clsName);
            else
                cls = Class.forName(clsName, true, ldr);

            Class old = ldrMap.putIfAbsent(clsName, cls);

            if (old != null)
                cls = old;
        }

        return cls;
    }

    /**
     * Clears class associated with provided class loader from class cache.
     *
     * @param ldr Class loader.
     * @param clsName Class name of clearing class.
     */
    public static void clearClassFromClassCache(ClassLoader ldr, String clsName) {
        ConcurrentMap<String, Class> map = classCache.get(ldr);

        if (map != null)
            map.remove(clsName);
    }

    /**
     * Clears class cache for provided loader.
     *
     * @param ldr Class loader.
     */
    public static void clearClassCache(ClassLoader ldr) {
        classCache.remove(ldr);
    }

    /**
     * Completely clears class cache.
     */
    public static void clearClassCache() {
        classCache.clear();
    }

    /**
     * Extracts full name of enclosing class from JDK8 lambda class name.
     *
     * @param clsName JDK8 lambda class name.
     * @return Full name of enclosing class for JDK8 lambda class name or
     *      {@code null} if passed in name is not related to lambda.
     */
    @Nullable public static String lambdaEnclosingClassName(String clsName) {
        int idx0 = clsName.indexOf("$$Lambda$"); // Java 8+
        int idx1 = clsName.indexOf("$$Lambda/"); // Java 21+

        if (idx0 == idx1)
            return null;

        return clsName.substring(0, idx0 >= 0 ? idx0 : idx1);
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(String clsName, @Nullable ClassLoader ldr) throws ClassNotFoundException {
        return forName(clsName, ldr, Marshallers.USE_CACHE.get());
    }
}
