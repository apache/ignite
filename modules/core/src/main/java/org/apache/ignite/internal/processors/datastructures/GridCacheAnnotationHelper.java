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

package org.apache.ignite.internal.processors.datastructures;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridBoundedLinkedHashMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Utility class for getting annotated values from classes.
 * Contains local cache of annotated methods and fields by classes for best performance.
 */
public class GridCacheAnnotationHelper<A extends Annotation> {
    /** Number of entries to keep in annotation cache. */
    private static final int DFLT_CLASS_CACHE_SIZE = 1000;

    /** Field cache. */
    private final GridBoundedLinkedHashMap<Class<?>, List<Field>> fieldCache;

    /** Method cache. */
    private final GridBoundedLinkedHashMap<Class<?>, List<Method>> mtdCache;

    /** Annotation class. */
    private final Class<A> annCls;

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * Constructor.
     *
     * @param annCls Annotation class.
     */
    public GridCacheAnnotationHelper(Class<A> annCls) {
        this(annCls, DFLT_CLASS_CACHE_SIZE);
    }

    /**
     * Constructor.
     *
     * @param annCls Annotation class.
     * @param capacity Capacity of local caches.
     */
    public GridCacheAnnotationHelper(Class<A> annCls, int capacity) {
        assert annCls != null : "Annotated class mustn't be null.";
        assert capacity > 0 : "Capacity must be more then zero.";

        this.annCls = annCls;

        fieldCache = new GridBoundedLinkedHashMap<>(capacity);

        mtdCache = new GridBoundedLinkedHashMap<>(capacity);
    }

    /**
     * Returns annotated value.
     *
     * @param target Object to find a value in.
     * @return Value of annotated field or method.
     * @throws IgniteCheckedException If failed to find.
     */
    public Object annotatedValue(Object target) throws IgniteCheckedException {
        IgniteBiTuple<Object, Boolean> res = annotatedValue(target, new HashSet<>(), false);

        assert res != null;

        return res.get1();
    }

    /**
     * Returns annotated value.
     *
     * @param target Object to find a value in.
     * @param visited Set of visited objects to avoid cycling.
     * @param annFound Flag indicating if value has already been found.
     * @return Value of annotated field or method.
     * @throws IgniteCheckedException If failed to find.
     */
    private IgniteBiTuple<Object, Boolean> annotatedValue(Object target, Set<Object> visited, boolean annFound)
        throws IgniteCheckedException {
        assert target != null;

        // To avoid infinite recursion.
        if (visited.contains(target))
            return F.t(null, annFound);

        visited.add(target);

        Object val = null;

        for (Class<?> cls = target.getClass(); !cls.equals(Object.class); cls = cls.getSuperclass()) {
            // Fields.
            for (Field f : fieldsWithAnnotation(cls)) {
                f.setAccessible(true);

                Object fieldVal;

                try {
                    fieldVal = f.get(target);
                }
                catch (IllegalAccessException e) {
                    throw new IgniteCheckedException("Failed to get annotated field value [cls=" + cls.getName() +
                        ", ann=" + annCls.getSimpleName()+']', e);
                }

                if (needsRecursion(f)) {
                    if (fieldVal != null) {
                        // Recursion.
                        IgniteBiTuple<Object, Boolean> tup = annotatedValue(fieldVal, visited, annFound);

                        if (!annFound && tup.get2())
                            // Update value only if annotation was found in recursive call.
                            val = tup.get1();

                        annFound = tup.get2();
                    }
                }
                else {
                    if (annFound)
                        throw new IgniteCheckedException("Multiple annotations has been found [cls=" + cls.getName() +
                            ", ann=" + annCls.getSimpleName() + ']');

                    val = fieldVal;

                    annFound = true;
                }
            }

            // Methods.
            for (Method m : methodsWithAnnotation(cls)) {
                if (annFound)
                    throw new IgniteCheckedException("Multiple annotations has been found [cls=" + cls.getName() +
                        ", ann=" + annCls.getSimpleName() + ']');

                m.setAccessible(true);

                try {
                    val = m.invoke(target);
                }
                catch (Exception e) {
                    throw new IgniteCheckedException("Failed to get annotated method value [cls=" + cls.getName() +
                        ", ann=" + annCls.getSimpleName()+']', e);
                }

                annFound = true;
            }
        }

        return F.t(val, annFound);
    }

    /**
     * @param f Field.
     * @return {@code true} if recursive inspection is required.
     */
    private boolean needsRecursion(Field f) {
        assert f != null;

        // Need to inspect anonymous classes, callable and runnable instances.
        return f.getName().startsWith("this$") || f.getName().startsWith("val$") ||
            Callable.class.isAssignableFrom(f.getType()) || Runnable.class.isAssignableFrom(f.getType());
    }

    /**
     * Gets all entries from the specified class or its super-classes that have
     * been annotated with annotation provided.
     *
     * @param cls Class in which search for fields.
     * @return Set of entries with given annotations.
     */
    private Iterable<Field> fieldsWithAnnotation(Class<?> cls) {
        synchronized (mux) {
            List<Field> fields = fieldCache.get(cls);
            if (fields == null) {
                fields = new ArrayList<>();

                for (Field field : cls.getDeclaredFields()) {
                    Annotation ann = field.getAnnotation(annCls);

                    if (ann != null || needsRecursion(field))
                        fields.add(field);
                }

                if (!fields.isEmpty())
                    fieldCache.put(cls, fields);
            }

            return fields;
        }
    }

    /**
     * Gets set of methods with given annotation.
     *
     * @param cls Class in which search for methods.
     * @return Set of methods with given annotations.
     */
    private Iterable<Method> methodsWithAnnotation(Class<?> cls) {
        synchronized (mux) {
            List<Method> mtds = mtdCache.get(cls);

            if (mtds == null) {
                mtds = new ArrayList<>();

                for (Method mtd : cls.getDeclaredMethods()) {
                    Annotation ann = mtd.getAnnotation(annCls);

                    if (ann != null)
                        mtds.add(mtd);
                }

                if (!mtds.isEmpty())
                    mtdCache.put(cls, mtds);
            }

            return mtds;
        }
    }
}