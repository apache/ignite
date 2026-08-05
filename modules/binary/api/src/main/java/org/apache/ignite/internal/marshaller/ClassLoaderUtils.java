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
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.Marshallers;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ClassLoaderUtils {
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

        ldr = ldr != null ? ldr : CommonUtils.gridClassLoader();

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

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(String clsName) throws ClassNotFoundException {
        return forName(clsName, null, Marshallers.USE_CACHE.get());
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
}
