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

import java.io.Externalizable;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EventListener;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCommonsSystemProperties;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteCommonsSystemProperties.IGNITE_TO_STRING_COLLECTION_LIMIT;
import static org.apache.ignite.IgniteCommonsSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;
import static org.apache.ignite.IgniteCommonsSystemProperties.getBoolean;

/**
 * Provides auto-generation framework for {@code toString()} output.
 * <p>
 * In case of recursion, repeatable objects will be shown as "ClassName@hash".
 * But fields will be printed only for the first entry to prevent recursion.
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
    private static final Object[] EMPTY_ARRAY = new Object[0];

    /** */
    private static final Map<String, GridToStringClassDescriptor> classCache = new ConcurrentHashMap<>();

    /** */
    public static volatile Function<Field, GridToStringFieldDescriptor> fldDescFactory = ReflectionToStringFieldDescriptor::new;

    /** @see IgniteCommonsSystemProperties#IGNITE_TO_STRING_MAX_LENGTH */
    public static final int DFLT_TO_STRING_MAX_LENGTH = 10_000;

    /** @see IgniteCommonsSystemProperties#IGNITE_TO_STRING_INCLUDE_SENSITIVE */
    public static final boolean DFLT_TO_STRING_INCLUDE_SENSITIVE = true;

    /** Supplier for {@link #includeSensitive} with default behavior. */
    private static final AtomicReference<Supplier<Boolean>> INCL_SENS_SUP_REF =
        new AtomicReference<>(new Supplier<Boolean>() {
            /** Value of "IGNITE_TO_STRING_INCLUDE_SENSITIVE". */
            final boolean INCLUDE_SENSITIVE =
                getBoolean(IGNITE_TO_STRING_INCLUDE_SENSITIVE, DFLT_TO_STRING_INCLUDE_SENSITIVE);

            /** {@inheritDoc} */
            @Override public Boolean get() {
                return INCLUDE_SENSITIVE;
            }
        });

    /** @see IgniteCommonsSystemProperties#IGNITE_TO_STRING_COLLECTION_LIMIT */
    public static final int DFLT_TO_STRING_COLLECTION_LIMIT = 100;

    /** */
    public static final int COLLECTION_LIMIT =
        IgniteCommonsSystemProperties.getInteger(IGNITE_TO_STRING_COLLECTION_LIMIT, DFLT_TO_STRING_COLLECTION_LIMIT);

    /**
     * Implementation of the <a href=
     * "https://en.wikipedia.org/wiki/Initialization-on-demand_holder_idiom">
     * "Initialization-on-demand holder idiom"</a>.
     */
    private static class Holder {
        /** Supplier holder for {@link #includeSensitive}. */
        static final Supplier<Boolean> INCL_SENS_SUP = INCL_SENS_SUP_REF.get();
    }

    /**
     * Setting the logic of the {@link #includeSensitive} method. <br/>
     * By default, it take the value of
     * {@link IgniteCommonsSystemProperties#IGNITE_TO_STRING_INCLUDE_SENSITIVE
     * IGNITE_TO_STRING_INCLUDE_SENSITIVE} system property. <br/>
     * <b>Important!</b> Changing the logic is possible only until the first
     * call of  {@link #includeSensitive} method. <br/>
     *
     * @param sup
     */
    public static void setIncludeSensitiveSupplier(Supplier<Boolean> sup) {
        assert nonNull(sup);

        INCL_SENS_SUP_REF.set(sup);
    }

    /**
     * Return {@code true} if need to include sensitive data otherwise
     * {@code false}.
     *
     * @return {@code true} if need to include sensitive data otherwise
     *      {@code false}.
     * @see GridToStringBuilder#setIncludeSensitiveSupplier(Supplier)
     */
    public static boolean includeSensitive() {
        return Holder.INCL_SENS_SUP.get();
    }

    /**
     * @param obj Object.
     * @return Hexed identity hashcode.
     */
    public static String identity(Object obj) {
        return '@' + Integer.toHexString(System.identityHashCode(obj));
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
     * @param name5 Additional parameter name.
     * @param val5 Additional parameter value.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0,
        String name1, Object val1,
        String name2, Object val2,
        String name3, Object val3,
        String name4, Object val4,
        String name5, Object val5) {
        return toString(cls,
            obj,
            name0, val0, false,
            name1, val1, false,
            name2, val2, false,
            name3, val3, false,
            name4, val4, false,
            name5, val5, false);
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
     * @param name5 Additional parameter name.
     * @param val5 Additional parameter value.
     * @param name6 Additional parameter name.
     * @param val6 Additional parameter value.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0,
        String name1, Object val1,
        String name2, Object val2,
        String name3, Object val3,
        String name4, Object val4,
        String name5, Object val5,
        String name6, Object val6) {
        return toString(cls,
            obj,
            name0, val0, false,
            name1, val1, false,
            name2, val2, false,
            name3, val3, false,
            name4, val4, false,
            name5, val5, false,
            name6, val6, false);
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
     * @param name4 Additional parameter name.
     * @param val4 Additional parameter value.
     * @param sens4 Property sensitive flag.
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

        Object[] addNames = new Object[5];
        Object[] addVals = new Object[5];
        boolean[] addSens = new boolean[5];

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

        return toStringImpl(cls, obj, addNames, addVals, addSens, 5);
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
     * @param name4 Additional parameter name.
     * @param val4 Additional parameter value.
     * @param sens4 Property sensitive flag.
     * @param name5 Additional parameter name.
     * @param val5 Additional parameter value.
     * @param sens5 Property sensitive flag.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0, boolean sens0,
        String name1, Object val1, boolean sens1,
        String name2, Object val2, boolean sens2,
        String name3, Object val3, boolean sens3,
        String name4, Object val4, boolean sens4,
        String name5, Object val5, boolean sens5) {
        assert cls != null;
        assert obj != null;
        assert name0 != null;
        assert name1 != null;
        assert name2 != null;
        assert name3 != null;
        assert name4 != null;
        assert name5 != null;

        Object[] addNames = new Object[6];
        Object[] addVals = new Object[6];
        boolean[] addSens = new boolean[6];

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
        addNames[5] = name5;
        addVals[5] = val5;
        addSens[5] = sens5;

        return toStringImpl(cls, obj, addNames, addVals, addSens, 6);
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
     * @param name4 Additional parameter name.
     * @param val4 Additional parameter value.
     * @param sens4 Property sensitive flag.
     * @param name5 Additional parameter name.
     * @param val5 Additional parameter value.
     * @param sens5 Property sensitive flag.
     * @param name6 Additional parameter name.
     * @param val6 Additional parameter value.
     * @param sens6 Property sensitive flag.
     * @return String presentation of the given object.
     */
    public static <T> String toString(Class<T> cls, T obj,
        String name0, Object val0, boolean sens0,
        String name1, Object val1, boolean sens1,
        String name2, Object val2, boolean sens2,
        String name3, Object val3, boolean sens3,
        String name4, Object val4, boolean sens4,
        String name5, Object val5, boolean sens5,
        String name6, Object val6, boolean sens6) {
        assert cls != null;
        assert obj != null;
        assert name0 != null;
        assert name1 != null;
        assert name2 != null;
        assert name3 != null;
        assert name4 != null;
        assert name5 != null;
        assert name6 != null;

        Object[] addNames = new Object[7];
        Object[] addVals = new Object[7];
        boolean[] addSens = new boolean[7];

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
        addNames[5] = name5;
        addVals[5] = val5;
        addSens[5] = sens5;
        addNames[6] = name6;
        addVals[6] = val6;
        addSens[6] = sens6;

        return toStringImpl(cls, obj, addNames, addVals, addSens, 7);
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

        Object[] addNames = new Object[4];
        Object[] addVals = new Object[4];
        boolean[] addSens = new boolean[4];

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

        return toStringImpl(cls, obj, addNames, addVals, addSens, 4);
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

        Object[] addNames = new Object[3];
        Object[] addVals = new Object[3];
        boolean[] addSens = new boolean[3];

        addNames[0] = name0;
        addVals[0] = val0;
        addSens[0] = sens0;
        addNames[1] = name1;
        addVals[1] = val1;
        addSens[1] = sens1;
        addNames[2] = name2;
        addVals[2] = val2;
        addSens[2] = sens2;

        return toStringImpl(cls, obj, addNames, addVals, addSens, 3);
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

        Object[] addNames = new Object[2];
        Object[] addVals = new Object[2];
        boolean[] addSens = new boolean[2];

        addNames[0] = name0;
        addVals[0] = val0;
        addSens[0] = sens0;
        addNames[1] = name1;
        addVals[1] = val1;
        addSens[1] = sens1;

        return toStringImpl(cls, obj, addNames, addVals, addSens, 2);
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

        Object[] addNames = new Object[] {name};
        Object[] addVals = new Object[] {val};
        boolean[] addSens = new boolean[] {sens};

        return toStringImpl(cls, obj, addNames, addVals, addSens, 1);
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

        return toStringImpl(cls, obj, EMPTY_ARRAY, EMPTY_ARRAY, null, 0);
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
     * @param <T> Type of object.
     * @param cls Class of the object.
     * @param obj Object for which to get string presentation.
     * @param addNames Names of additional values to be included.
     * @param addVals Additional values to be included.
     * @param addSens Sensitive flag of values or {@code null} if all values are not sensitive.
     * @param addLen How many additional values will be included.
     * @return String presentation of the given object.
     */
    private static <T> String toStringImpl(
        Class<T> cls,
        T obj,
        Object[] addNames,
        Object[] addVals,
        @Nullable boolean[] addSens,
        int addLen) {
        assert cls != null;
        assert obj != null;
        assert addNames != null;
        assert addVals != null;
        assert addNames.length == addVals.length;
        assert addLen <= addNames.length;
        boolean isNew = GridToStringNode.isNew();
        try {
            List<GridToStringNode> addNodes =
                    GridToStringNodeFactory.getNodes(addNames, addVals, addSens, addLen);
            GridToStringNode node = GridToStringNode.getRootNode(obj, cls, addNodes);
            return isNew ? node.toString() : GridToStringNode.markNode(node);
        }
        catch (RuntimeException | StackOverflowError throwable) {
            if (isNew)
                return handleThrowable(throwable);
            else
                throw throwable;
        }
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
     * Returns limited string representation of array.
     *
     * @param arr Array object. Each value is automatically wrapped if it has a primitive type.
     * @return String representation of an array.
     */
    public static String arrayToString(Object arr) {
        if (arr == null)
            return "null";

        String res;

        int arrLen;

        if (arr instanceof Object[]) {
            Object[] objArr = (Object[])arr;

            arrLen = objArr.length;

            if (arrLen > COLLECTION_LIMIT)
                objArr = Arrays.copyOf(objArr, COLLECTION_LIMIT);

            res = Arrays.toString(objArr);
        }
        else {
            res = toStringWithLimit(arr, COLLECTION_LIMIT);

            arrLen = Array.getLength(arr);
        }

        if (arrLen > COLLECTION_LIMIT) {
            StringBuilder resSB = new StringBuilder(res);

            resSB.deleteCharAt(resSB.length() - 1);

            resSB.append("... and ").append(arrLen - COLLECTION_LIMIT).append(" more]");

            res = resSB.toString();
        }

        return res;
    }

    /**
     * Returns limited string representation of array.
     *
     * @param arr Input array. Each value is automatically wrapped if it has a primitive type.
     * @param limit max array items to string limit.
     * @return String representation of an array.
     */
    private static String toStringWithLimit(Object arr, int limit) {
        int arrIdxMax = Array.getLength(arr) - 1;

        if (arrIdxMax == -1)
            return "[]";

        int idxMax = Math.min(arrIdxMax, limit);

        StringBuilder b = new StringBuilder();

        b.append('[');

        for (int i = 0; i <= idxMax; ++i) {
            b.append(Array.get(arr, i));

            if (i == idxMax)
                return b.append(']').toString();

            b.append(", ");
        }

        return b.toString();
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

        Object[] propNames = new Object[1];
        Object[] propVals = new Object[1];
        boolean[] propSens = new boolean[1];

        propNames[0] = name;
        propVals[0] = val;
        propSens[0] = sens;

        return toStringImpl(str, propNames, propVals, propSens, 1);
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

        Object[] propNames = new Object[2];
        Object[] propVals = new Object[2];
        boolean[] propSens = new boolean[2];

        propNames[0] = name0;
        propVals[0] = val0;
        propSens[0] = sens0;
        propNames[1] = name1;
        propVals[1] = val1;
        propSens[1] = sens1;

        return toStringImpl(str, propNames, propVals, propSens, 2);
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

        Object[] propNames = new Object[3];
        Object[] propVals = new Object[3];
        boolean[] propSens = new boolean[3];

        propNames[0] = name0;
        propVals[0] = val0;
        propSens[0] = sens0;
        propNames[1] = name1;
        propVals[1] = val1;
        propSens[1] = sens1;
        propNames[2] = name2;
        propVals[2] = val2;
        propSens[2] = sens2;

        return toStringImpl(str, propNames, propVals, propSens, 3);
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

        Object[] propNames = new Object[4];
        Object[] propVals = new Object[4];
        boolean[] propSens = new boolean[4];

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

        return toStringImpl(str, propNames, propVals, propSens, 4);
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

        Object[] propNames = new Object[5];
        Object[] propVals = new Object[5];
        boolean[] propSens = new boolean[5];

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

        return toStringImpl(str, propNames, propVals, propSens, 5);
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
     * @param name5 Property name.
     * @param val5 Property value.
     * @param sens5 Property sensitive flag.
     * @return String presentation.
     */
    public static String toString(String str,
        String name0, @Nullable Object val0, boolean sens0,
        String name1, @Nullable Object val1, boolean sens1,
        String name2, @Nullable Object val2, boolean sens2,
        String name3, @Nullable Object val3, boolean sens3,
        String name4, @Nullable Object val4, boolean sens4,
        String name5, @Nullable Object val5, boolean sens5) {
        assert name0 != null;
        assert name1 != null;
        assert name2 != null;
        assert name3 != null;
        assert name4 != null;
        assert name5 != null;

        Object[] propNames = new Object[6];
        Object[] propVals = new Object[6];
        boolean[] propSens = new boolean[6];

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
        propNames[5] = name5;
        propVals[5] = val5;
        propSens[5] = sens5;

        return toStringImpl(str, propNames, propVals, propSens, 6);
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
     * @param name5 Property name.
     * @param val5 Property value.
     * @param sens5 Property sensitive flag.
     * @param name6 Property name.
     * @param val6 Property value.
     * @param sens6 Property sensitive flag.
     * @return String presentation.
     */
    public static String toString(String str,
        String name0, @Nullable Object val0, boolean sens0,
        String name1, @Nullable Object val1, boolean sens1,
        String name2, @Nullable Object val2, boolean sens2,
        String name3, @Nullable Object val3, boolean sens3,
        String name4, @Nullable Object val4, boolean sens4,
        String name5, @Nullable Object val5, boolean sens5,
        String name6, @Nullable Object val6, boolean sens6) {
        assert name0 != null;
        assert name1 != null;
        assert name2 != null;
        assert name3 != null;
        assert name4 != null;
        assert name5 != null;
        assert name6 != null;

        Object[] propNames = new Object[7];
        Object[] propVals = new Object[7];
        boolean[] propSens = new boolean[7];

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
        propNames[5] = name5;
        propVals[5] = val5;
        propSens[5] = sens5;
        propNames[6] = name6;
        propVals[6] = val6;
        propSens[6] = sens6;

        return toStringImpl(str, propNames, propVals, propSens, 7);
    }

    /**
     * Produces uniformed output of string with context properties
     *
     * @param str Output prefix or {@code null} if empty.
     * @param triplets Triplets {@code {name, value, sencitivity}}.
     * @return String presentation.
     */
    public static String toString(String str, Object... triplets) {
        if (triplets.length % 3 != 0)
            throw new IllegalArgumentException("Array length must be a multiple of 3");

        int propCnt = triplets.length / 3;

        Object[] propNames = new Object[propCnt];
        Object[] propVals = new Object[propCnt];
        boolean[] propSens = new boolean[propCnt];

        for (int i = 0; i < propCnt; i++) {
            Object name = triplets[i * 3];

            assert name != null;

            propNames[i] = name;

            propVals[i] = triplets[i * 3 + 1];

            Object sens = triplets[i * 3 + 2];

            assert sens instanceof Boolean;

            propSens[i] = (Boolean)sens;
        }
        return toStringImpl(str, propNames, propVals, propSens, propCnt);
    }

    /**
     * Creates an uniformed string presentation for the binary-like object.
     *
     * @param str Output prefix or {@code null} if empty.
     * @param propNames Names of object properties.
     * @param propVals Property values.
     * @param propSens Sensitive flag of values or {@code null} if all values is not sensitive.
     * @param propCnt Properties count.
     * @return String presentation of the object.
     */
    private static String toStringImpl(String str, Object[] propNames, Object[] propVals,
        boolean[] propSens, int propCnt) {
        boolean isNew = GridToStringNode.isNew();
        try {
            List<GridToStringNode> addNodes =
                    GridToStringNodeFactory.getNodes(propNames, propVals, propSens, propCnt);
            GridToStringNode node = GridToStringNode.getRootNode(str, addNodes);
            return isNew ? node.toString() : GridToStringNode.markNode(node);
        }
        catch (RuntimeException | StackOverflowError throwable) {
            if (isNew)
                return handleThrowable(throwable);
            else
                throw throwable;
        }
    }

    /**
     * @param cls Class.
     * @param <T> Type of the object.
     * @return Descriptor for the class.
     */
    @SuppressWarnings({"TooBroadScope"})
    static <T> GridToStringClassDescriptor getClassDescriptor(Class<T> cls) {
        assert cls != null;

        String key = cls.getName() + System.identityHashCode(cls.getClassLoader());

        GridToStringClassDescriptor cd;

        cd = classCache.get(key);

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
                    add = notSens || includeSensitive();
                }
                else if (!f.isAnnotationPresent(GridToStringExclude.class) &&
                    !type.isAnnotationPresent(GridToStringExclude.class)
                ) {
                    add = addField(f, type);
                }

                if (add) {
                    GridToStringFieldDescriptor fd = fldDescFactory.apply(f);

                    // Get order, if any.
                    final GridToStringOrder annOrder = f.getAnnotation(GridToStringOrder.class);
                    if (annOrder != null)
                        fd.setOrder(annOrder.value());

                    cd.addField(fd);
                }
            }

            cd.sortFields();

            classCache.putIfAbsent(key, cd);
        }

        return cd;
    }

    /** @return {@code True} if field should be added. */
    private static boolean addField(Field f, Class<?> type) {
        // Include only private non-static
        return !Modifier.isStatic(f.getModifiers()) &&
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
            !Condition.class.isAssignableFrom(type);
    }

    /**
     * Creates string representation of a specified collection with preliminary sorting and duplicates removing.
     *
     * @param c Input collection.
     * @return String representation of collection.
     */
    public static String toStringSortedDistinct(Collection<? extends Comparable<?>> c) {
        if (c.isEmpty())
            return "[]";

        return '[' + F.concat(new TreeSet<>(c), ",") + ']';
    }

    /**
     * Returns sorted and compacted string representation of given {@code col}.
     * Two nearby numbers are compacted to one continuous segment.
     * E.g. collection of [1, 2, 3, 5, 6, 7, 10] with
     * {@code nextValFun = i -> i + 1} will be compacted to [1-3, 5-7, 10].
     *
     * @param col Collection of numbers.
     * @param nextValFun Function to get nearby number.
     * @return Compacted string representation of given collections.
     */
    public static <T extends Number & Comparable<? super T>> String compact(
        Collection<T> col,
        Function<T, T> nextValFun
    ) {
        assert nonNull(col);
        assert nonNull(nextValFun);

        if (col.isEmpty())
            return "[]";

        SB sb = new SB();
        sb.a('[');

        List<T> l = new ArrayList<>(col);
        Collections.sort(l);

        T left = l.get(0), right = left;
        for (int i = 1; i < l.size(); i++) {
            T val = l.get(i);

            if (right.compareTo(val) == 0 || nextValFun.apply(right).compareTo(val) == 0) {
                right = val;
                continue;
            }

            if (left.compareTo(right) == 0)
                sb.a(left);
            else
                sb.a(left).a('-').a(right);

            sb.a(',').a(' ');

            left = right = val;
        }

        if (left.compareTo(right) == 0)
            sb.a(left);
        else
            sb.a(left).a('-').a(right);

        sb.a(']');

        return sb.toString();
    }

    /**
     * Creates a string from the elements separated using <code>separator</code>.
     *
     * @param list Elements.
     * @param separator Separator.
     * @param truncSuffix String suffix in case of string truncation.
     * @param maxLen Max length of the output string at which it will be truncated (<code>0</code> - unlimited).
     * @param maxCnt Max number of added elements at which the string will be truncated (<code>0</code> - unlimited).
     * @return Result string.
     */
    public static <T> String joinToString(
        @Nullable Iterable<T> list,
        @Nullable String separator,
        @Nullable String truncSuffix,
        int maxLen,
        int maxCnt
    ) {
        if (F.isEmpty(list))
            return "";

        SB buf = new SB();

        for (Iterator<T> itr = list.iterator(); itr.hasNext(); ) {
            T obj = itr.next();

            if (separator != null && buf.length() > 0)
                buf.a(separator);

            buf.a(obj);

            if (itr.hasNext() && (--maxCnt == 0 || maxLen > 0 && buf.length() >= maxLen)) {
                if (truncSuffix != null)
                    buf.a(truncSuffix);

                break;
            }
        }

        return buf.toString();
    }

    /** */
    private static <T extends Throwable> String handleThrowable(T throwable) {
        StringWriter strWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(strWriter);
        throwable.printStackTrace(printWriter);
        printWriter.flush();
        printWriter.close();
        return strWriter.toString();
    }
}
