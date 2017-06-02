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

package org.apache.ignite.internal.util.tostring;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.EventListener;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;

/**
 * Provides auto-generation framework for {@code toString()} output.
 * <p>
 * Default exclusion policy (can be overridden with {@link GridToStringInclude}
 * annotation):
 * <ul>
 * <li>fields with {@link GridToStringExclude} annotations
 * <li>classes that have {@link GridToStringExclude} annotation (current list):
 *      <ul>
 *      <li>GridManager
 *      <li>GridManagerRegistry
 *      <li>GridProcessor
 *      <li>GridProcessorRegistry
 *      <li>IgniteLogger
 *      <li>GridDiscoveryMetricsProvider
 *      <li>GridByteArrayList
 *      </ul>
 * <li>static fields
 * <li>non-private fields
 * <li>arrays
 * <li>fields of type {@link Object}
 * <li>fields of type {@link Thread}
 * <li>fields of type {@link Runnable}
 * <li>fields of type {@link Serializable}
 * <li>fields of type {@link Externalizable}
 * <li>{@link InputStream} implementations
 * <li>{@link OutputStream} implementations
 * <li>{@link EventListener} implementations
 * <li>{@link Lock} implementations
 * <li>{@link ReadWriteLock} implementations
 * <li>{@link Condition} implementations
 * <li>{@link Map} implementations
 * <li>{@link Collection} implementations
 * </ul>
 */
public class GridToStringBuilder {
    /** */
    private static final Map<String, GridToStringClassDescriptor> classCache = new HashMap<>();

    /** */
    private static final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** Maximum number of collection (map) entries to print. */
    public static final int MAX_COL_SIZE = 100;

    /** {@link IgniteSystemProperties#IGNITE_TO_STRING_INCLUDE_SENSITIVE} */
    public static final boolean INCLUDE_SENSITIVE =
        IgniteSystemProperties.getBoolean(IGNITE_TO_STRING_INCLUDE_SENSITIVE, true);

    /** */
    private static ThreadLocal<Queue<GridToStringThreadLocal>> threadCache = new ThreadLocal<Queue<GridToStringThreadLocal>>() {
        @Override protected Queue<GridToStringThreadLocal> initialValue() {
            Queue<GridToStringThreadLocal> queue = new LinkedList<>();

            queue.offer(new GridToStringThreadLocal());

            return queue;
        }
    };

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @param name0 Additional parameter name.
     * @param val0 Additional parameter value.
     * @param name1 Additional parameter name.
     * @param val1 Additional parameter value.
     * @param name2 Additional parameter name.
     * @param val2 Additional parameter value.
     * @param name3 Additional parameter name.
     * @param val3 Additional parameter value.
     * @param name4 Additional parameter name.
     * @param val4 Additional parameter value.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0,
        String name1, Object val1,
        String name2, Object val2,
        String name3, Object val3,
        String name4, Object val4) {
        return toString(cls,
            obj,
            name0, val0, false,
            name1, val1, false,
            name2, val2, false,
            name3, val3, false,
            name4, val4, false);
    }

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @param name0 Additional parameter name.
     * @param val0 Additional parameter value.
     * @param name1 Additional parameter name.
     * @param val1 Additional parameter value.
     * @param name2 Additional parameter name.
     * @param val2 Additional parameter value.
     * @param name3 Additional parameter name.
     * @param val3 Additional parameter value.
     * @param name4 Additional parameter name.
     * @param val4 Additional parameter value.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0, boolean sens0,
        String name1, Object val1, boolean sens1,
        String name2, Object val2, boolean sens2,
        String name3, Object val3, boolean sens3,
        String name4, Object val4, boolean sens4) {
        assert cls != null;
        assert obj != null;
        assert name0 != null;
        assert name1 != null;
        assert name2 != null;
        assert name3 != null;
        assert name4 != null;

        Queue<GridToStringThreadLocal> queue = threadCache.get();

        assert queue != null;

        // Since string() methods can be chain-called from the same thread we
        // have to keep a list of thread-local objects and remove/add them
        // in each string() apply.
        GridToStringThreadLocal tmp = queue.isEmpty() ? new GridToStringThreadLocal() : queue.remove();

        Object[] addNames = tmp.getAdditionalNames();
        Object[] addVals = tmp.getAdditionalValues();
        boolean[] addSens = tmp.getAdditionalSensitives();

        addNames[0] = name0;
        addVals[0] = val0;
        addSens[0] = sens0;
        addNames[1] = name1;
        addVals[1] = val1;
        addSens[1] = sens1;
        addNames[2] = name2;
        addVals[2] = val2;
        addSens[2] = sens2;
        addNames[3] = name3;
        addVals[3] = val3;
        addSens[3] = sens3;
        addNames[4] = name4;
        addVals[4] = val4;
        addSens[4] = sens4;

        try {
            return toStringImpl(cls, tmp.getStringBuilder(), obj, addNames, addVals, addSens, 5);
        }
        finally {
            queue.offer(tmp);
        }
    }

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @param name0 Additional parameter name.
     * @param val0 Additional parameter value.
     * @param name1 Additional parameter name.
     * @param val1 Additional parameter value.
     * @param name2 Additional parameter name.
     * @param val2 Additional parameter value.
     * @param name3 Additional parameter name.
     * @param val3 Additional parameter value.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0,
        String name1, Object val1,
        String name2, Object val2,
        String name3, Object val3) {
        return toString(cls, obj,
            name0, val0, false,
            name1, val1, false,
            name2, val2, false,
            name3, val3, false);
    }

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @param name0 Additional parameter name.
     * @param val0 Additional parameter value.
     * @param sens0 Property sensitive flag.
     * @param name1 Additional parameter name.
     * @param val1 Additional parameter value.
     * @param sens1 Property sensitive flag.
     * @param name2 Additional parameter name.
     * @param val2 Additional parameter value.
     * @param sens2 Property sensitive flag.
     * @param name3 Additional parameter name.
     * @param val3 Additional parameter value.
     * @param sens3 Property sensitive flag.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0, boolean sens0,
        String name1, Object val1, boolean sens1,
        String name2, Object val2, boolean sens2,
        String name3, Object val3, boolean sens3) {
        assert cls != null;
        assert obj != null;
        assert name0 != null;
        assert name1 != null;
        assert name2 != null;
        assert name3 != null;

        Queue<GridToStringThreadLocal> queue = threadCache.get();

        assert queue != null;

        // Since string() methods can be chain-called from the same thread we
        // have to keep a list of thread-local objects and remove/add them
        // in each string() apply.
        GridToStringThreadLocal tmp = queue.isEmpty() ? new GridToStringThreadLocal() : queue.remove();

        Object[] addNames = tmp.getAdditionalNames();
        Object[] addVals = tmp.getAdditionalValues();
        boolean[] addSens = tmp.getAdditionalSensitives();

        addNames[0] = name0;
        addVals[0] = val0;
        addSens[0] = sens0;
        addNames[1] = name1;
        addVals[1] = val1;
        addSens[1] = sens1;
        addNames[2] = name2;
        addVals[2] = val2;
        addSens[2] = sens2;
        addNames[3] = name3;
        addVals[3] = val3;
        addSens[3] = sens3;

        try {
            return toStringImpl(cls, tmp.getStringBuilder(), obj, addNames, addVals, addSens, 4);
        }
        finally {
            queue.offer(tmp);
        }
    }

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @param name0 Additional parameter name.
     * @param val0 Additional parameter value.
     * @param name1 Additional parameter name.
     * @param val1 Additional parameter value.
     * @param name2 Additional parameter name.
     * @param val2 Additional parameter value.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0,
        String name1, Object val1,
        String name2, Object val2) {
        return toString(cls,
            obj,
            name0, val0, false,
            name1, val1, false,
            name2, val2, false);
    }

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @param name0 Additional parameter name.
     * @param val0 Additional parameter value.
     * @param sens0 Property sensitive flag.
     * @param name1 Additional parameter name.
     * @param val1 Additional parameter value.
     * @param sens1 Property sensitive flag.
     * @param name2 Additional parameter name.
     * @param val2 Additional parameter value.
     * @param sens2 Property sensitive flag.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0, boolean sens0,
        String name1, Object val1, boolean sens1,
        String name2, Object val2, boolean sens2) {
        assert cls != null;
        assert obj != null;
        assert name0 != null;
        assert name1 != null;
        assert name2 != null;

        Queue<GridToStringThreadLocal> queue = threadCache.get();

        assert queue != null;

        // Since string() methods can be chain-called from the same thread we
        // have to keep a list of thread-local objects and remove/add them
        // in each string() apply.
        GridToStringThreadLocal tmp = queue.isEmpty() ? new GridToStringThreadLocal() : queue.remove();

        Object[] addNames = tmp.getAdditionalNames();
        Object[] addVals = tmp.getAdditionalValues();
        boolean[] addSens = tmp.getAdditionalSensitives();

        addNames[0] = name0;
        addVals[0] = val0;
        addSens[0] = sens0;
        addNames[1] = name1;
        addVals[1] = val1;
        addSens[1] = sens1;
        addNames[2] = name2;
        addVals[2] = val2;
        addSens[2] = sens2;

        try {
            return toStringImpl(cls, tmp.getStringBuilder(), obj, addNames, addVals, addSens, 3);
        }
        finally {
            queue.offer(tmp);
        }
    }

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @param name0 Additional parameter name.
     * @param val0 Additional parameter value.
     * @param name1 Additional parameter name.
     * @param val1 Additional parameter value.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0,
        String name1, Object val1) {
        return toString(cls, obj, name0, val0, false, name1, val1, false);
    }

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @param name0 Additional parameter name.
     * @param val0 Additional parameter value.
     * @param sens0 Property sensitive flag.
     * @param name1 Additional parameter name.
     * @param val1 Additional parameter value.
     * @param sens1 Property sensitive flag.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0, boolean sens0,
        String name1, Object val1, boolean sens1) {
        assert cls != null;
        assert obj != null;
        assert name0 != null;
        assert name1 != null;

        Queue<GridToStringThreadLocal> queue = threadCache.get();

        assert queue != null;

        // Since string() methods can be chain-called from the same thread we
        // have to keep a list of thread-local objects and remove/add them
        // in each string() apply.
        GridToStringThreadLocal tmp = queue.isEmpty() ? new GridToStringThreadLocal() : queue.remove();

        Object[] addNames = tmp.getAdditionalNames();
        Object[] addVals = tmp.getAdditionalValues();
        boolean[] addSens = tmp.getAdditionalSensitives();

        addNames[0] = name0;
        addVals[0] = val0;
        addSens[0] = sens0;
        addNames[1] = name1;
        addVals[1] = val1;
        addSens[1] = sens1;

        try {
            return toStringImpl(cls, tmp.getStringBuilder(), obj, addNames, addVals, addSens, 2);
        }
        finally {
            queue.offer(tmp);
        }
    }

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @param name Additional parameter name.
     * @param val Additional parameter value.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj, String name, @Nullable Object val) {
        return toString(cls, obj, name, val, false);
    }

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @param name Additional parameter name.
     * @param val Additional parameter value.
     * @param sens Property sensitive flag.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj, String name, @Nullable Object val, boolean sens) {
        assert cls != null;
        assert obj != null;
        assert name != null;

        Queue<GridToStringThreadLocal> queue = threadCache.get();

        assert queue != null;

        // Since string() methods can be chain-called from the same thread we
        // have to keep a list of thread-local objects and remove/add them
        // in each string() apply.
        GridToStringThreadLocal tmp = queue.isEmpty() ? new GridToStringThreadLocal() : queue.remove();

        Object[] addNames = tmp.getAdditionalNames();
        Object[] addVals = tmp.getAdditionalValues();
        boolean[] addSens = tmp.getAdditionalSensitives();

        addNames[0] = name;
        addVals[0] = val;
        addSens[0] = sens;

        try {
            return toStringImpl(cls, tmp.getStringBuilder(), obj, addNames, addVals, addSens, 1);
        }
        finally {
            queue.offer(tmp);
        }
    }

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj) {
        assert cls != null;
        assert obj != null;

        Queue<GridToStringThreadLocal> queue = threadCache.get();

        assert queue != null;

        // Since string() methods can be chain-called from the same thread we
        // have to keep a list of thread-local objects and remove/add them
        // in each string() apply.
        GridToStringThreadLocal tmp = queue.isEmpty() ? new GridToStringThreadLocal() : queue.remove();

        try {
            return toStringImpl(cls, tmp.getStringBuilder(), obj, tmp.getAdditionalNames(),
                tmp.getAdditionalValues(), null, 0);
        }
        finally {
            queue.offer(tmp);
        }
    }

    /**
     * Produces auto-generated output of string presentation for given object and its declaration class.
     *
     * @param <T> Type of the object.
     * @param cls Declaration class of the object. Note that this should not be a runtime class.
     * @param obj Object to get a string presentation for.
     * @param parent String representation of parent.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj, String parent) {
        return parent != null ? toString(cls, obj, "super", parent) : toString(cls, obj);
    }

    /**
     * Creates an uniformed string presentation for the given object.
     *
     * @param cls Class of the object.
     * @param buf String builder buffer.
     * @param obj Object for which to get string presentation.
     * @param addNames Names of additional values to be included.
     * @param addVals Additional values to be included.
     * @param addSens Sensitive flag of values or {@code null} if all values are not sensitive.
     * @param addLen How many additional values will be included.
     * @return String presentation of the given object.
     * @param <T> Type of object.
     */
    @SuppressWarnings({"unchecked"})
    private static <T> String toStringImpl(Class<T> cls, SB buf, T obj,
        Object[] addNames,
        Object[] addVals,
        @Nullable boolean[] addSens,
        int addLen) {
        assert cls != null;
        assert buf != null;
        assert obj != null;
        assert addNames != null;
        assert addVals != null;
        assert addNames.length == addVals.length;
        assert addLen <= addNames.length;

        try {
            GridToStringClassDescriptor cd = getClassDescriptor(cls);

            assert cd != null;

            buf.setLength(0);

            buf.a(cd.getSimpleClassName()).a(" [");

            boolean first = true;

            for (GridToStringFieldDescriptor fd : cd.getFields()) {
                if (!first)
                   buf.a(", ");
                else
                    first = false;

                String name = fd.getName();

                Field field = cls.getDeclaredField(name);

                field.setAccessible(true);

                buf.a(name).a('=');

                Class<?> fieldType = field.getType();

                if (fieldType.isArray())
                    buf.a(arrayToString(fieldType, field.get(obj)));
                else {
                    Object val = field.get(obj);

                    if (val instanceof Collection && ((Collection)val).size() > MAX_COL_SIZE)
                        val = F.retain((Collection)val, true, MAX_COL_SIZE);
                    else if (val instanceof Map && ((Map)val).size() > MAX_COL_SIZE) {
                        Map tmp = U.newHashMap(MAX_COL_SIZE);
                        int cntr = 0;

                        for (Object o : ((Map)val).entrySet()) {
                            Map.Entry e = (Map.Entry)o;

                            tmp.put(e.getKey(), e.getValue());

                            if (++cntr >= MAX_COL_SIZE)
                                break;
                        }

                        val = tmp;
                    }

                    buf.a(val);
                }
            }

            appendVals(buf, first, addNames, addVals, addSens, addLen);

            buf.a(']');

            return buf.toString();
        }
        // Specifically catching all exceptions.
        catch (Exception e) {
            rwLock.writeLock().lock();

            // Remove entry from cache to avoid potential memory leak
            // in case new class loader got loaded under the same identity hash.
            try {
                classCache.remove(cls.getName() + System.identityHashCode(cls.getClassLoader()));
            }
            finally {
                rwLock.writeLock().unlock();
            }

            // No other option here.
            throw new IgniteException(e);
        }
    }

    /**
     * @param arrType Type of the array.
     * @param arr Array object.
     * @return String representation of an array.
     */
    private static String arrayToString(Class arrType, Object arr) {
        if (arrType.equals(byte[].class))
            return Arrays.toString((byte[])arr);
        if (arrType.equals(boolean[].class))
            return Arrays.toString((boolean[])arr);
        if (arrType.equals(short[].class))
            return Arrays.toString((short[])arr);
        if (arrType.equals(int[].class))
            return Arrays.toString((int[])arr);
        if (arrType.equals(long[].class))
            return Arrays.toString((long[])arr);
        if (arrType.equals(float[].class))
            return Arrays.toString((float[])arr);
        if (arrType.equals(double[].class))
            return Arrays.toString((double[])arr);
        if (arrType.equals(char[].class))
            return Arrays.toString((char[])arr);

        return Arrays.toString((Object[])arr);
    }

    /**
     * Produces uniformed output of string with context properties
     *
     * @param str Output prefix or {@code null} if empty.
     * @param name Property name.
     * @param val Property value.
     * @return String presentation.
     */
    public static String toString(String str, String name, @Nullable Object val) {
        return toString(str, name, val, false);
    }

    /**
     * Produces uniformed output of string with context properties
     *
     * @param str Output prefix or {@code null} if empty.
     * @param name Property name.
     * @param val Property value.
     * @param sens Property sensitive flag.
     * @return String presentation.
     */
    public static String toString(String str, String name, @Nullable Object val, boolean sens) {
        assert name != null;

        Queue<GridToStringThreadLocal> queue = threadCache.get();

        assert queue != null;

        // Since string() methods can be chain-called from the same thread we
        // have to keep a list of thread-local objects and remove/add them
        // in each string() apply.
        GridToStringThreadLocal tmp = queue.isEmpty() ? new GridToStringThreadLocal() : queue.remove();

        Object[] propNames = tmp.getAdditionalNames();
        Object[] propVals = tmp.getAdditionalValues();
        boolean[] propSens = tmp.getAdditionalSensitives();

        propNames[0] = name;
        propVals[0] = val;
        propSens[0] = sens;

        try {
            return toStringImpl(str, tmp.getStringBuilder(), propNames, propVals, propSens, 1);
        }
        finally {
            queue.offer(tmp);
        }
    }

    /**
     * Produces uniformed output of string with context properties
     *
     * @param str Output prefix or {@code null} if empty.
     * @param name0 Property name.
     * @param val0 Property value.
     * @param name1 Property name.
     * @param val1 Property value.
     * @return String presentation.
     */
    public static String toString(String str, String name0, @Nullable Object val0, String name1,
        @Nullable Object val1) {
        return toString(str, name0, val0, false, name1, val1, false);
    }

    /**
     * Produces uniformed output of string with context properties
     *
     * @param str Output prefix or {@code null} if empty.
     * @param name0 Property name.
     * @param val0 Property value.
     * @param sens0 Property sensitive flag.
     * @param name1 Property name.
     * @param val1 Property value.
     * @param sens1 Property sensitive flag.
     * @return String presentation.
     */
    public static String toString(String str,
        String name0, @Nullable Object val0, boolean sens0,
        String name1, @Nullable Object val1, boolean sens1) {
        assert name0 != null;
        assert name1 != null;

        Queue<GridToStringThreadLocal> queue = threadCache.get();

        assert queue != null;

        // Since string() methods can be chain-called from the same thread we
        // have to keep a list of thread-local objects and remove/add them
        // in each string() apply.
        GridToStringThreadLocal tmp = queue.isEmpty() ? new GridToStringThreadLocal() : queue.remove();

        Object[] propNames = tmp.getAdditionalNames();
        Object[] propVals = tmp.getAdditionalValues();
        boolean[] propSens = tmp.getAdditionalSensitives();

        propNames[0] = name0;
        propVals[0] = val0;
        propSens[0] = sens0;
        propNames[1] = name1;
        propVals[1] = val1;
        propSens[1] = sens1;

        try {
            return toStringImpl(str, tmp.getStringBuilder(), propNames, propVals, propSens, 2);
        }
        finally {
            queue.offer(tmp);
        }
    }

    /**
     * Produces uniformed output of string with context properties
     *
     * @param str Output prefix or {@code null} if empty.
     * @param name0 Property name.
     * @param val0 Property value.
     * @param sens0 Property sensitive flag.
     * @param name1 Property name.
     * @param val1 Property value.
     * @param sens1 Property sensitive flag.
     * @param name2 Property name.
     * @param val2 Property value.
     * @param sens2 Property sensitive flag.
     * @return String presentation.
     */
    public static String toString(String str,
        String name0, @Nullable Object val0, boolean sens0,
        String name1, @Nullable Object val1, boolean sens1,
        String name2, @Nullable Object val2, boolean sens2) {
        assert name0 != null;
        assert name1 != null;
        assert name2 != null;

        Queue<GridToStringThreadLocal> queue = threadCache.get();

        assert queue != null;

        // Since string() methods can be chain-called from the same thread we
        // have to keep a list of thread-local objects and remove/add them
        // in each string() apply.
        GridToStringThreadLocal tmp = queue.isEmpty() ? new GridToStringThreadLocal() : queue.remove();

        Object[] propNames = tmp.getAdditionalNames();
        Object[] propVals = tmp.getAdditionalValues();
        boolean[] propSens = tmp.getAdditionalSensitives();

        propNames[0] = name0;
        propVals[0] = val0;
        propSens[0] = sens0;
        propNames[1] = name1;
        propVals[1] = val1;
        propSens[1] = sens1;
        propNames[2] = name2;
        propVals[2] = val2;
        propSens[2] = sens2;

        try {
            return toStringImpl(str, tmp.getStringBuilder(), propNames, propVals, propSens, 3);
        }
        finally {
            queue.offer(tmp);
        }
    }

    /**
     * Produces uniformed output of string with context properties
     *
     * @param str Output prefix or {@code null} if empty.
     * @param name0 Property name.
     * @param val0 Property value.
     * @param sens0 Property sensitive flag.
     * @param name1 Property name.
     * @param val1 Property value.
     * @param sens1 Property sensitive flag.
     * @param name2 Property name.
     * @param val2 Property value.
     * @param sens2 Property sensitive flag.
     * @param name3 Property name.
     * @param val3 Property value.
     * @param sens3 Property sensitive flag.
     * @return String presentation.
     */
    public static String toString(String str,
        String name0, @Nullable Object val0, boolean sens0,
        String name1, @Nullable Object val1, boolean sens1,
        String name2, @Nullable Object val2, boolean sens2,
        String name3, @Nullable Object val3, boolean sens3) {
        assert name0 != null;
        assert name1 != null;
        assert name2 != null;
        assert name3 != null;

        Queue<GridToStringThreadLocal> queue = threadCache.get();

        assert queue != null;

        // Since string() methods can be chain-called from the same thread we
        // have to keep a list of thread-local objects and remove/add them
        // in each string() apply.
        GridToStringThreadLocal tmp = queue.isEmpty() ? new GridToStringThreadLocal() : queue.remove();

        Object[] propNames = tmp.getAdditionalNames();
        Object[] propVals = tmp.getAdditionalValues();
        boolean[] propSens = tmp.getAdditionalSensitives();

        propNames[0] = name0;
        propVals[0] = val0;
        propSens[0] = sens0;
        propNames[1] = name1;
        propVals[1] = val1;
        propSens[1] = sens1;
        propNames[2] = name2;
        propVals[2] = val2;
        propSens[2] = sens2;
        propNames[3] = name3;
        propVals[3] = val3;
        propSens[3] = sens3;

        try {
            return toStringImpl(str, tmp.getStringBuilder(), propNames, propVals, propSens, 4);
        }
        finally {
            queue.offer(tmp);
        }
    }

    /**
     * Produces uniformed output of string with context properties
     *
     * @param str Output prefix or {@code null} if empty.
     * @param name0 Property name.
     * @param val0 Property value.
     * @param sens0 Property sensitive flag.
     * @param name1 Property name.
     * @param val1 Property value.
     * @param sens1 Property sensitive flag.
     * @param name2 Property name.
     * @param val2 Property value.
     * @param sens2 Property sensitive flag.
     * @param name3 Property name.
     * @param val3 Property value.
     * @param sens3 Property sensitive flag.
     * @param name4 Property name.
     * @param val4 Property value.
     * @param sens4 Property sensitive flag.
     * @return String presentation.
     */
    public static String toString(String str,
        String name0, @Nullable Object val0, boolean sens0,
        String name1, @Nullable Object val1, boolean sens1,
        String name2, @Nullable Object val2, boolean sens2,
        String name3, @Nullable Object val3, boolean sens3,
        String name4, @Nullable Object val4, boolean sens4) {
        assert name0 != null;
        assert name1 != null;
        assert name2 != null;
        assert name3 != null;
        assert name4 != null;

        Queue<GridToStringThreadLocal> queue = threadCache.get();

        assert queue != null;

        // Since string() methods can be chain-called from the same thread we
        // have to keep a list of thread-local objects and remove/add them
        // in each string() apply.
        GridToStringThreadLocal tmp = queue.isEmpty() ? new GridToStringThreadLocal() : queue.remove();

        Object[] propNames = tmp.getAdditionalNames();
        Object[] propVals = tmp.getAdditionalValues();
        boolean[] propSens = tmp.getAdditionalSensitives();

        propNames[0] = name0;
        propVals[0] = val0;
        propSens[0] = sens0;
        propNames[1] = name1;
        propVals[1] = val1;
        propSens[1] = sens1;
        propNames[2] = name2;
        propVals[2] = val2;
        propSens[2] = sens2;
        propNames[3] = name3;
        propVals[3] = val3;
        propSens[3] = sens3;
        propNames[4] = name4;
        propVals[4] = val4;
        propSens[4] = sens4;

        try {
            return toStringImpl(str, tmp.getStringBuilder(), propNames, propVals, propSens, 5);
        }
        finally {
            queue.offer(tmp);
        }
    }

    /**
     * Creates an uniformed string presentation for the binary-like object.
     *
     * @param str Output prefix or {@code null} if empty.
     * @param buf String builder buffer.
     * @param propNames Names of object properties.
     * @param propVals Property values.
     * @param propSens Sensitive flag of values or {@code null} if all values is not sensitive.
     * @param propCnt Properties count.
     * @return String presentation of the object.
     */
    private static String toStringImpl(String str, SB buf, Object[] propNames, Object[] propVals,
        boolean[] propSens, int propCnt) {

        buf.setLength(0);

        if (str != null)
            buf.a(str).a(" ");

        buf.a("[");

        appendVals(buf, true, propNames, propVals, propSens, propCnt);

        buf.a(']');

        return buf.toString();
    }

    /**
     * Append additional values to the buffer.
     *
     * @param buf Buffer.
     * @param first First value flag.
     * @param addNames Names of additional values to be included.
     * @param addVals Additional values to be included.
     * @param addSens Sensitive flag of values or {@code null} if all values are not sensitive.
     * @param addLen How many additional values will be included.
     */
    private static void appendVals(SB buf,
        boolean first,
        Object[] addNames,
        Object[] addVals,
        boolean[] addSens,
        int addLen)
    {
        if (addLen > 0) {
            for (int i = 0; i < addLen; i++) {
                Object addVal = addVals[i];

                if (addVal != null) {
                    if (addSens != null && addSens[i] && !INCLUDE_SENSITIVE)
                        continue;

                    GridToStringInclude incAnn = addVal.getClass().getAnnotation(GridToStringInclude.class);

                    if (incAnn != null && incAnn.sensitive() && !INCLUDE_SENSITIVE)
                        continue;

                    Class<?> cls = addVal.getClass();

                    if (cls.isArray())
                        addVal = arrayToString(cls, addVal);
                }

                if (!first)
                    buf.a(", ");
                else
                    first = false;

                buf.a(addNames[i]).a('=').a(addVal);
            }
        }
    }

    /**
     * @param cls Class.
     * @param <T> Type of the object.
     * @return Descriptor for the class.
     */
    @SuppressWarnings({"TooBroadScope"})
    private static <T> GridToStringClassDescriptor getClassDescriptor(Class<T> cls) {
        assert cls != null;

        String key = cls.getName() + System.identityHashCode(cls.getClassLoader());

        GridToStringClassDescriptor cd;

        rwLock.readLock().lock();

        try {
            cd = classCache.get(key);
        }
        finally {
            rwLock.readLock().unlock();
        }

        if (cd == null) {
            cd = new GridToStringClassDescriptor(cls);

            for (Field f : cls.getDeclaredFields()) {
                boolean add = false;

                Class<?> type = f.getType();

                final GridToStringInclude incFld = f.getAnnotation(GridToStringInclude.class);
                final GridToStringInclude incType = type.getAnnotation(GridToStringInclude.class);

                if (incFld != null || incType != null) {
                    // Information is not sensitive when both the field and the field type are not sensitive.
                    // When @GridToStringInclude is not present then the flag is false by default for that attribute.
                    final boolean notSens = (incFld == null || !incFld.sensitive()) && (incType == null || !incType.sensitive());
                    add = notSens || INCLUDE_SENSITIVE;
                }
                else if (!f.isAnnotationPresent(GridToStringExclude.class) &&
                    !f.getType().isAnnotationPresent(GridToStringExclude.class)) {
                    if (
                        // Include only private non-static
                        Modifier.isPrivate(f.getModifiers()) && !Modifier.isStatic(f.getModifiers()) &&

                        // No direct objects & serializable.
                        Object.class != type &&
                        Serializable.class != type &&
                        Externalizable.class != type &&

                        // No arrays.
                        !type.isArray() &&

                        // Exclude collections, IO, etc.
                        !EventListener.class.isAssignableFrom(type) &&
                        !Map.class.isAssignableFrom(type) &&
                        !Collection.class.isAssignableFrom(type) &&
                        !InputStream.class.isAssignableFrom(type) &&
                        !OutputStream.class.isAssignableFrom(type) &&
                        !Thread.class.isAssignableFrom(type) &&
                        !Runnable.class.isAssignableFrom(type) &&
                        !Lock.class.isAssignableFrom(type) &&
                        !ReadWriteLock.class.isAssignableFrom(type) &&
                        !Condition.class.isAssignableFrom(type)
                    )
                        add = true;
                }

                if (add) {
                    GridToStringFieldDescriptor fd = new GridToStringFieldDescriptor(f.getName());

                    // Get order, if any.
                    final GridToStringOrder annOrder = f.getAnnotation(GridToStringOrder.class);
                    if (annOrder != null)
                        fd.setOrder(annOrder.value());

                    cd.addField(fd);
                }
            }

            cd.sortFields();

            /*
             * Allow multiple puts for the same class - they will simply override.
             */

            rwLock.writeLock().lock();

            try {
                classCache.put(key, cd);
            }
            finally {
                rwLock.writeLock().unlock();
            }
        }

        return cd;
    }
}
