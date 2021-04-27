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

package org.apache.ignite.internal.tostring;

import java.io.Externalizable;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.EventListener;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringBuilder;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.nonNull;
import static org.apache.ignite.lang.IgniteSystemProperties.IGNITE_SENSITIVE_DATA_LOGGING;
import static org.apache.ignite.lang.IgniteSystemProperties.IGNITE_TO_STRING_COLLECTION_LIMIT;
import static org.apache.ignite.lang.IgniteSystemProperties.IGNITE_TO_STRING_IGNORE_RUNTIME_EXCEPTION;
import static org.apache.ignite.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.lang.IgniteSystemProperties.getInteger;
import static org.apache.ignite.lang.IgniteSystemProperties.getString;

/**
 * Provides auto-generation framework for {@code toString()} output.
 * <p>
 * In case of recursion, object fields will be printed only for the first entry to prevent recursion,
 * and all the next repeated entrances will be shown as "ClassName@hash".
 * <p>
 * Default exclusion policy (can be overridden with {@link IgniteToStringInclude} annotation):
 * <ul>
 * <li>fields with {@link IgniteToStringExclude} annotation
 * <li>classes with {@link IgniteToStringExclude} annotation
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
@SuppressWarnings({"BooleanParameter", "NonFinalUtilityClass"})
public class IgniteToStringBuilder {
    /** Empty array instance. */
    private static final Object[] EMPTY_ARRAY = new Object[0];

    /** Max collection elements to be written. */
    private static final int COLLECTION_LIMIT = getInteger(IGNITE_TO_STRING_COLLECTION_LIMIT, 100);

    /** Ignore flag for runtime exceptions while building string. */
    private static final boolean IGNORE_RUNTIME_EXCEPTION = !getBoolean(IGNITE_TO_STRING_IGNORE_RUNTIME_EXCEPTION, false);

    /** Supplier for {@link #includeSensitive} with default behavior. */
    private static final AtomicReference<Supplier<SensitiveDataLoggingPolicy>> SENS_DATA_LOG_SUP_REF =
        new AtomicReference<>(new Supplier<>() {
            /** Sensitive data logging policy. */
            final SensitiveDataLoggingPolicy sensitiveDataLoggingPolicy =
                SensitiveDataLoggingPolicy.valueOf(getString(IGNITE_SENSITIVE_DATA_LOGGING, "hash").toUpperCase());

            /** {@inheritDoc} */
            @Override public SensitiveDataLoggingPolicy get() {
                return sensitiveDataLoggingPolicy;
            }
        });

    /** Every thread has its own string builder. */
    private static final ThreadLocal<SBLimitedLength> threadLocSB = ThreadLocal.withInitial(() ->
        new SBLimitedLength(256));

    /**
     * Tracks the objects currently printing in the string builder.
     * <p>
     * Since {@code toString()} methods can be chain-called from the same thread we
     * have to keep a map of this objects pointed to the position of previous occurrence
     * and remove/add them in each {@code toString(...)} apply.
     */
    private static final ThreadLocal<IdentityHashMap<Object, EntryReference>> savedObjects = ThreadLocal.withInitial(IdentityHashMap::new);

    /**
     *
     */
    private static final Map<String, ClassDescriptor> classCache = new ConcurrentHashMap<>();

    /**
     * Initialization-on-demand holder.
     *
     * @see <a href= "https://en.wikipedia.org/wiki/Initialization-on-demand_holder_idiom">
     * "Initialization-on-demand holder idiom"</a>.
     */
    private static final class Holder {
        /** Supplier holder for {@link #includeSensitive} and {@link #getSensitiveDataLogging}. */
        static final Supplier<SensitiveDataLoggingPolicy> SENS_DATA_LOG_SUP = SENS_DATA_LOG_SUP_REF.get();
    }

    /**
     * @return {@link SensitiveDataLoggingPolicy} Log levels for sensitive data
     */
    public static SensitiveDataLoggingPolicy getSensitiveDataLogging() {
        return Holder.SENS_DATA_LOG_SUP.get();
    }

    /**
     * Setting the logic of the {@link #includeSensitive} and {@link #getSensitiveDataLogging} methods.
     * <p>
     * Overrides default supplier that uses {@link IgniteSystemProperties#IGNITE_SENSITIVE_DATA_LOGGING}
     * system property.
     *
     * <b>Important!</b> Changing the logic is possible only until the first
     * call of {@link #includeSensitive} or {@link #getSensitiveDataLogging} methods.
     *
     * @param sup {@link SensitiveDataLoggingPolicy} supplier.
     */
    public static void setSensitiveDataLoggingPolicySupplier(Supplier<SensitiveDataLoggingPolicy> sup) {
        assert nonNull(sup);

        SENS_DATA_LOG_SUP_REF.set(sup);
    }

    /**
     * Return include sensitive data flag.
     *
     * @return {@code true} if need to include sensitive data, {@code false} otherwise.
     * @see IgniteToStringBuilder#setSensitiveDataLoggingPolicySupplier(Supplier)
     */
    public static boolean includeSensitive() {
        return Holder.SENS_DATA_LOG_SUP.get() == SensitiveDataLoggingPolicy.PLAIN;
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(cls, sb, obj, addNames, addVals, addSens, 5);
        }
        finally {
            if (newStr)
                sb.reset();
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(cls, sb, obj, addNames, addVals, addSens, 6);
        }
        finally {
            if (newStr)
                sb.reset();
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(cls, sb, obj, addNames, addVals, addSens, 7);
        }
        finally {
            if (newStr)
                sb.reset();
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(cls, sb, obj, addNames, addVals, addSens, 4);
        }
        finally {
            if (newStr)
                sb.reset();
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(cls, sb, obj, addNames, addVals, addSens, 3);
        }
        finally {
            if (newStr)
                sb.reset();
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

        Object[] addNames = new Object[2];
        Object[] addVals = new Object[2];
        boolean[] addSens = new boolean[2];

        addNames[0] = name0;
        addVals[0] = val0;
        addSens[0] = sens0;
        addNames[1] = name1;
        addVals[1] = val1;
        addSens[1] = sens1;

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(cls, sb, obj, addNames, addVals, addSens, 2);
        }
        finally {
            if (newStr)
                sb.reset();
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

        Object[] addNames = new Object[1];
        Object[] addVals = new Object[1];
        boolean[] addSens = new boolean[1];

        addNames[0] = name;
        addVals[0] = val;
        addSens[0] = sens;

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(cls, sb, obj, addNames, addVals, addSens, 1);
        }
        finally {
            if (newStr)
                sb.reset();
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(cls, sb, obj, EMPTY_ARRAY, EMPTY_ARRAY, null, 0);
        }
        finally {
            if (newStr)
                sb.reset();
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
     * Print value with length limitation.
     *
     * @param buf buffer to print to.
     * @param val value to print, can be {@code null}.
     */
    private static void toString(SBLimitedLength buf, Object val) {
        toString(buf, null, val);
    }

    /**
     * Print value with length limitation.
     *
     * @param buf buffer to print to.
     * @param cls value class.
     * @param val value to print.
     */
    private static void toString(SBLimitedLength buf, Class<?> cls, Object val) {
        if (val == null) {
            buf.a("null");

            return;
        }

        if (cls == null)
            cls = val.getClass();

        if (cls.isPrimitive()) {
            buf.a(val);

            return;
        }

        IdentityHashMap<Object, EntryReference> svdObjs = savedObjects.get();

        if (handleRecursion(buf, val, cls, svdObjs))
            return;

        svdObjs.put(val, new EntryReference(buf.length()));

        try {
            if (cls.isArray())
                addArray(buf, cls, val);
            else if (val instanceof Collection)
                addCollection(buf, (Collection<?>)val);
            else if (val instanceof Map)
                addMap(buf, (Map<?, ?>)val);
            else
                buf.a(val);
        }
        finally {
            svdObjs.remove(val);
        }
    }

    /**
     * Writes array to buffer.
     *
     * @param buf String builder buffer.
     * @param arrType Type of the array.
     * @param obj Array object.
     */
    private static void addArray(SBLimitedLength buf, Class arrType, Object obj) {
        if (arrType.getComponentType().isPrimitive()) {
            buf.a(arrayToString(obj));

            return;
        }

        Object[] arr = (Object[])obj;

        buf.a(arrType.getSimpleName()).a(" [");

        for (int i = 0; i < arr.length; i++) {
            toString(buf, arr[i]);

            if (i == COLLECTION_LIMIT - 1 || i == arr.length - 1)
                break;

            buf.a(", ");
        }

        handleOverflow(buf, arr.length);

        buf.a(']');
    }

    /**
     * Writes collection to buffer.
     *
     * @param buf String builder buffer.
     * @param col Collection object.
     */
    private static void addCollection(SBLimitedLength buf, Collection<?> col) {
        buf.a(col.getClass().getSimpleName()).a(" [");

        int cnt = 0;
        boolean needHandleOverflow = true;

        Iterator<?> iter = col.iterator();
        int colSize = col.size();

        while (iter.hasNext()) {
            Object obj;

            try {
                obj = iter.next();
            }
            catch (ConcurrentModificationException e) {
                handleConcurrentModification(buf, cnt, colSize);

                needHandleOverflow = false;
                break;
            }

            toString(buf, obj);

            if (++cnt == COLLECTION_LIMIT || cnt == colSize)
                break;

            buf.a(", ");
        }

        if (needHandleOverflow)
            handleOverflow(buf, colSize);

        buf.a(']');
    }

    /**
     * Writes map to buffer.
     *
     * @param buf String builder buffer.
     * @param map Map object.
     */
    private static <K, V> void addMap(SBLimitedLength buf, Map<K, V> map) {
        buf.a(map.getClass().getSimpleName()).a(" {");

        int cnt = 0;
        boolean needHandleOverflow = true;

        Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();
        int mapSize = map.size();

        while (iter.hasNext()) {
            Object key;
            Object value;

            try {
                Map.Entry<K, V> entry = iter.next();

                key = entry.getKey();
                value = entry.getValue();
            }
            catch (ConcurrentModificationException e) {
                handleConcurrentModification(buf, cnt, mapSize);

                needHandleOverflow = false;
                break;
            }

            toString(buf, key);

            buf.a('=');

            toString(buf, value);

            if (++cnt == COLLECTION_LIMIT || cnt == mapSize)
                break;

            buf.a(", ");
        }

        if (needHandleOverflow)
            handleOverflow(buf, mapSize);

        buf.a('}');
    }

    /**
     * Writes overflow message to buffer if needed.
     *
     * @param buf String builder buffer.
     * @param size Size to compare with limit.
     */
    private static void handleOverflow(SBLimitedLength buf, int size) {
        int overflow = size - COLLECTION_LIMIT;

        if (overflow > 0)
            buf.a("... and ").a(overflow).a(" more");
    }

    /**
     * Writes message about situation of ConcurrentModificationException caught when iterating over collection.
     *
     * @param buf String builder buffer.
     * @param writtenElements Number of elements successfully written to output.
     * @param size Overall size of collection.
     */
    private static void handleConcurrentModification(SBLimitedLength buf, int writtenElements, int size) {
        buf.a("... concurrent modification was detected, ").a(writtenElements).a(" out of ").a(size)
            .a(" were written");
    }

    /**
     * Creates an uniformed string presentation for the given object.
     *
     * @param <T> Type of object.
     * @param cls Class of the object.
     * @param buf String builder buffer.
     * @param obj Object for which to get string presentation.
     * @param addNames Names of additional values to be included.
     * @param addVals Additional values to be included.
     * @param addSens Sensitive flag of values or {@code null} if all values are not sensitive.
     * @param addLen How many additional values will be included.
     * @return String presentation of the given object.
     */
    private static <T> String toStringImpl(
        Class<T> cls,
        SBLimitedLength buf,
        T obj,
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

        boolean newStr = buf.length() == 0;

        IdentityHashMap<Object, EntryReference> svdObjs = savedObjects.get();

        if (newStr)
            svdObjs.put(obj, new EntryReference(buf.length()));

        try {
            int len = buf.length();

            String s = toStringImpl0(cls, buf, obj, addNames, addVals, addSens, addLen);

            if (newStr)
                return s;

            buf.setLength(len);

            return s.substring(len);
        }
        finally {
            if (newStr)
                svdObjs.remove(obj);
        }
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
     * @param <T> Type of object.
     * @return String presentation of the given object.
     */
    private static <T> String toStringImpl0(
        Class<T> cls,
        SBLimitedLength buf,
        T obj,
        Object[] addNames,
        Object[] addVals,
        @Nullable boolean[] addSens,
        int addLen
    ) {
        try {
            ClassDescriptor cd = getClassDescriptor(cls);

            assert cd != null;

            buf.a(cd.getSimpleClassName());

            EntryReference ref = savedObjects.get().get(obj);

            if (ref != null && ref.hashNeeded) {
                buf.a(identity(obj));

                ref.hashNeeded = false;
            }

            buf.a(" [");

            boolean first = true;

            for (FieldDescriptor fd : cd.getFields()) {
                if (!first)
                    buf.a(", ");
                else
                    first = false;

                buf.a(fd.getName()).a('=');

                final VarHandle fH = fd.varHandle();

                switch (fd.type()) {
                    case FieldDescriptor.FIELD_TYPE_OBJECT:
                        try {
                            toString(buf, fd.fieldClass(), fH.get(obj));
                        }
                        catch (RuntimeException e) {
                            if (IGNORE_RUNTIME_EXCEPTION) {
                                buf.a("Runtime exception was caught when building string representation: " +
                                    e.getMessage());
                            }
                            else
                                throw e;
                        }

                        break;
                    case FieldDescriptor.FIELD_TYPE_BYTE:
                        buf.a((byte)fH.get(obj));

                        break;
                    case FieldDescriptor.FIELD_TYPE_BOOLEAN:
                        buf.a((boolean)fH.get(obj));

                        break;
                    case FieldDescriptor.FIELD_TYPE_CHAR:
                        buf.a((char)fH.get(obj));

                        break;
                    case FieldDescriptor.FIELD_TYPE_SHORT:
                        buf.a((short)fH.get(obj));

                        break;
                    case FieldDescriptor.FIELD_TYPE_INT:
                        buf.a((int)fH.get(obj));

                        break;
                    case FieldDescriptor.FIELD_TYPE_FLOAT:
                        buf.a((float)fH.get(obj));

                        break;
                    case FieldDescriptor.FIELD_TYPE_LONG:
                        buf.a((long)fH.get(obj));

                        break;
                    case FieldDescriptor.FIELD_TYPE_DOUBLE:
                        buf.a((double)fH.get(obj));

                        break;
                }
            }

            appendVals(buf, first, addNames, addVals, addSens, addLen);

            buf.a(']');

            return buf.toString();
        }
        // Specifically catching all exceptions.
        catch (Exception e) {
            // Remove entry from cache to avoid potential memory leak
            // in case new class loader got loaded under the same identity hash.
            classCache.remove(cls.getName() + System.identityHashCode(cls.getClassLoader()));

            // No other option here.
            throw new IgniteInternalException(e);
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(str, sb, propNames, propVals, propSens, 1);
        }
        finally {
            if (newStr)
                sb.reset();
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
     * @param name1 Property name.
     * @param val1 Property value.
     * @param name2 Property name.
     * @param val2 Property value.
     * @return String presentation.
     */
    public static String toString(String str, String name0, @Nullable Object val0, String name1,
        @Nullable Object val1, String name2, @Nullable Object val2) {
        return toString(str, name0, val0, false, name1, val1, false, name2, val2, false);
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(str, sb, propNames, propVals, propSens, 2);
        }
        finally {
            if (newStr)
                sb.reset();
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(str, sb, propNames, propVals, propSens, 3);
        }
        finally {
            if (newStr)
                sb.reset();
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(str, sb, propNames, propVals, propSens, 4);
        }
        finally {
            if (newStr)
                sb.reset();
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(str, sb, propNames, propVals, propSens, 5);
        }
        finally {
            if (newStr)
                sb.reset();
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(str, sb, propNames, propVals, propSens, 6);
        }
        finally {
            if (newStr)
                sb.reset();
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(str, sb, propNames, propVals, propSens, 7);
        }
        finally {
            if (newStr)
                sb.reset();
        }
    }

    /**
     * Produces uniformed output of string with context properties
     *
     * @param str Output prefix or {@code null} if empty.
     * @param triplets Triplets {@code {name, value, sensitivity}}.
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

        SBLimitedLength sb = threadLocSB.get();

        boolean newStr = sb.length() == 0;

        try {
            return toStringImpl(str, sb, propNames, propVals, propSens, propCnt);
        }
        finally {
            if (newStr)
                sb.reset();
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
    private static String toStringImpl(String str, SBLimitedLength buf, Object[] propNames, Object[] propVals,
        boolean[] propSens, int propCnt) {

        boolean newStr = buf.length() == 0;

        if (str != null)
            buf.a(str).a(" ");

        buf.a("[");

        appendVals(buf, true, propNames, propVals, propSens, propCnt);

        buf.a(']');

        if (newStr)
            return buf.toString();

        // Called from another ITSB.toString(), so this string is already in the buffer and shouldn't be returned.
        return "";
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
    private static void appendVals(SBLimitedLength buf,
        boolean first,
        Object[] addNames,
        Object[] addVals,
        boolean[] addSens,
        int addLen) {
        if (addLen > 0) {
            for (int i = 0; i < addLen; i++) {
                Object addVal = addVals[i];

                if (addVal != null) {
                    if (addSens != null && addSens[i] && !includeSensitive())
                        continue;

                    IgniteToStringInclude incAnn = addVal.getClass().getAnnotation(IgniteToStringInclude.class);

                    if (incAnn != null && incAnn.sensitive() && !includeSensitive())
                        continue;
                }

                if (!first)
                    buf.a(", ");
                else
                    first = false;

                buf.a(addNames[i]).a('=');

                toString(buf, addVal);
            }
        }
    }

    /**
     * @param cls Class.
     * @param <T> Type of the object.
     * @return Descriptor for the class.
     * @throws IllegalAccessException If failed.
     */
    private static <T> ClassDescriptor getClassDescriptor(Class<T> cls) throws IllegalAccessException {
        assert cls != null;

        String key = cls.getName() + System.identityHashCode(cls.getClassLoader());

        ClassDescriptor cd = classCache.get(key);

        if (cd != null)
            return cd;

        cd = new ClassDescriptor(cls);

        final MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(cls, MethodHandles.lookup());

        for (Field f : cls.getDeclaredFields()) {
            boolean add = false;

            Class<?> type = f.getType();

            final IgniteToStringInclude incFld = f.getAnnotation(IgniteToStringInclude.class);
            final IgniteToStringInclude incType = type.getAnnotation(IgniteToStringInclude.class);

            if (incFld != null || incType != null) {
                // Information is not sensitive when both the field and the field type are not sensitive.
                // When @IgniteToStringInclude is not present then the flag is false by default for that attribute.
                final boolean notSens = (incFld == null || !incFld.sensitive()) && (incType == null || !incType.sensitive());
                add = notSens || includeSensitive();
            }
            else if (!f.isAnnotationPresent(IgniteToStringExclude.class) &&
                !type.isAnnotationPresent(IgniteToStringExclude.class)
            ) {
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
                FieldDescriptor fd = new FieldDescriptor(f, lookup.unreflectVarHandle(f));

                // Get order, if any.
                final IgniteToStringOrder annOrder = f.getAnnotation(IgniteToStringOrder.class);
                if (annOrder != null)
                    fd.setOrder(annOrder.value());

                cd.addField(fd);
            }
        }

        cd.sortFields();

        classCache.putIfAbsent(key, cd);

        return cd;
    }

    /**
     * Returns sorted and compacted string representation of given {@code col}.
     * Two nearby numbers with difference at most 1 are compacted to one continuous segment.
     * E.g. collection of [1, 2, 3, 5, 6, 7, 10] will be compacted to [1-3, 5-7, 10].
     *
     * @param col Collection of integers.
     * @return Compacted string representation of given collections.
     */
    public static String compact(Collection<Integer> col) {
        return compact(col, i -> i + 1);
    }

    /**
     * Returns sorted and compacted string representation of given {@code col}.
     * Two nearby numbers are compacted to one continuous segment.
     * E.g. collection of [1, 2, 3, 5, 6, 7, 10] with
     * {@code nextValFun = i -> i + 1} will be compacted to [1-3, 5-7, 10].
     *
     * @param col Collection of numbers.
     * @param nextValFun Function to get nearby number.
     * @param <T> Comparable number type.
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

        IgniteStringBuilder sb = new IgniteStringBuilder();
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
     * Checks that object is already saved.
     * In positive case this method inserts hash to the saved object entry (if needed) and name@hash for current entry.
     * Further toString operations are not needed for current object.
     *
     * @param buf String builder buffer.
     * @param obj Object.
     * @param cls Class.
     * @param svdObjs Map with saved objects to handle recursion.
     * @return {@code True} if object is already saved and name@hash was added to buffer.
     * {@code False} if it wasn't saved previously and it should be saved.
     */
    private static boolean handleRecursion(
        SBLimitedLength buf,
        Object obj,
        @NotNull Class<?> cls,
        IdentityHashMap<Object, EntryReference> svdObjs
    ) {
        EntryReference ref = svdObjs.get(obj);

        if (ref == null)
            return false;

        int pos = ref.pos;

        String name = cls.getSimpleName();
        String hash = identity(obj);
        String savedName = name + hash;
        String charsAtPos = buf.impl().substring(pos, pos + savedName.length());

        if (!buf.isOverflowed() && !savedName.equals(charsAtPos)) {
            if (charsAtPos.startsWith(cls.getSimpleName())) {
                buf.i(pos + name.length(), hash);

                incValues(svdObjs, obj, hash.length());
            }
            else
                ref.hashNeeded = true;
        }

        buf.a(savedName);

        return true;
    }

    /**
     * Increment positions of already presented objects afterward given object.
     *
     * @param svdObjs Map with objects already presented in the buffer.
     * @param obj Object.
     * @param hashLen Length of the object's hash.
     */
    private static void incValues(IdentityHashMap<Object, EntryReference> svdObjs, Object obj, int hashLen) {
        int baseline = svdObjs.get(obj).pos;

        for (IdentityHashMap.Entry<Object, EntryReference> entry : svdObjs.entrySet()) {
            EntryReference ref = entry.getValue();

            int pos = ref.pos;

            if (pos > baseline)
                ref.pos = pos + hashLen;
        }
    }

    /**
     * Stub.
     */
    protected IgniteToStringBuilder() {
        // No op.
    }

    /**
     *
     */
    private static class EntryReference {
        /** Position. */
        int pos;

        /** First object entry needs hash to be written. */
        boolean hashNeeded;

        /**
         * @param pos Position.
         */
        private EntryReference(int pos) {
            this.pos = pos;
            hashNeeded = false;
        }
    }
}
