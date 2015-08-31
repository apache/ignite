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

package org.apache.ignite.internal.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_REFLECTION_CACHE_SIZE;

/**
 * Reflection field and method cache for classes.
 */
public class GridReflectionCache implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Compares fields by name. */
    private static final Comparator<Field> FIELD_NAME_COMPARATOR = new Comparator<Field>() {
        @Override public int compare(Field f1, Field f2) {
            return f1.getName().compareTo(f2.getName());
        }
    };

    /** Compares methods by name. */
    private static final Comparator<Method> METHOD_NAME_COMPARATOR = new Comparator<Method>() {
        @Override public int compare(Method m1, Method m2) {
            return m1.getName().compareTo(m2.getName());
        }
    };

    /** Cache size. */
    private static final int CACHE_SIZE = Integer.getInteger(IGNITE_REFLECTION_CACHE_SIZE, 128);

    /** Fields cache. */
    private ConcurrentMap<Class, List<Field>> fields = new GridBoundedConcurrentLinkedHashMap<>(
        CACHE_SIZE, CACHE_SIZE);

    /** Methods cache. */
    private ConcurrentMap<Class, List<Method>> mtds = new GridBoundedConcurrentLinkedHashMap<>(
        CACHE_SIZE, CACHE_SIZE);

    /** Field predicate. */
    private IgnitePredicate<Field> fp;

    /** Method predicate. */
    private IgnitePredicate<Method> mp;

    /**
     * Reflection cache without any method or field predicates.
     */
    public GridReflectionCache() {
        // No-op.
    }

    /**
     * Reflection cache with specified field and method predicates.
     * @param fp Field predicate.
     * @param mp Method predicate.
     */
    public GridReflectionCache(@Nullable IgnitePredicate<Field> fp, @Nullable IgnitePredicate<Method> mp) {
        this.fp = fp;
        this.mp = mp;
    }

    /**
     * Gets field value for object.
     *
     * @param o Key to get affinity key for.
     * @return Value of the field for given object or {@code null} if field was not found.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public Object firstFieldValue(Object o) throws IgniteCheckedException {
        assert o != null;

        Field f = firstField(o.getClass());

        if (f != null) {
            try {
                return f.get(o);
            }
            catch (IllegalAccessException e) {
                throw new IgniteCheckedException("Failed to access field for object [field=" + f + ", obj=" + o + ']', e);
            }
        }

        return null;
    }

    /**
     * Gets method return value for object.
     *
     * @param o Key to get affinity key for.
     * @return Method return value for given object or {@code null} if method was not found.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public Object firstMethodValue(Object o) throws IgniteCheckedException {
        assert o != null;

        Method m = firstMethod(o.getClass());

        if (m != null) {
            try {
                return m.invoke(o);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new IgniteCheckedException("Failed to invoke method for object [mtd=" + m + ", obj=" + o + ']', e);
            }
        }

        return null;
    }

    /**
     * Gets first field in the class list of fields.
     *
     * @param cls Class.
     * @return First field.
     */
    @Nullable public Field firstField(Class<?> cls) {
        assert cls != null;

        List<Field> l = fields(cls);

        return l.isEmpty() ? null : l.get(0);
    }

    /**
     * Gets first method in the class list of methods.
     *
     * @param cls Class.
     * @return First method.
     */
    @Nullable public Method firstMethod(Class<?> cls) {
        assert cls != null;

        List<Method> l = methods(cls);

        return l.isEmpty() ? null : l.get(0);
    }

    /**
     * Gets fields.
     *
     * @param cls Class.
     * @return Annotated field.
     */
    public List<Field> fields(Class<?> cls) {
        assert cls != null;

        List<Field> fieldsList = fields.get(cls);

        if (fieldsList == null) {
            fieldsList = new ArrayList<>();

            for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                List<Field> l = new ArrayList<>();

                for (Field f : c.getDeclaredFields()) {
                    if (fp == null || fp.apply(f)) {
                        f.setAccessible(true);

                        l.add(f);
                    }
                }

                if (!l.isEmpty()) {
                    Collections.sort(l, FIELD_NAME_COMPARATOR);

                    fieldsList.addAll(l);
                }
            }

            fields.putIfAbsent(cls, fieldsList);
        }

        return fieldsList;
    }


    /**
     * Gets methods.
     *
     * @param cls Class.
     * @return Annotated method.
     */
    public List<Method> methods(Class<?> cls) {
        assert cls != null;

        List<Method> mtdsList = mtds.get(cls);

        if (mtdsList == null) {
            mtdsList = new ArrayList<>();

            for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                List<Method> l = new ArrayList<>();

                for (Method m : c.getDeclaredMethods()) {
                    if (mp == null || mp.apply(m)) {
                        m.setAccessible(true);

                        l.add(m);
                    }
                }

                if (!l.isEmpty()) {
                    Collections.sort(l, METHOD_NAME_COMPARATOR);

                    mtdsList.addAll(l);
                }
            }

            mtds.putIfAbsent(cls, mtdsList);
        }

        return mtdsList;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(fp);
        out.writeObject(mp);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fp = (IgnitePredicate<Field>)in.readObject();
        mp = (IgnitePredicate<Method>)in.readObject();
    }
}