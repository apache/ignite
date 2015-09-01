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

package org.apache.ignite.internal.util.typedef;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_HOME;

/**
 * Defines global scope.
 * <p>
 * Contains often used utility functions allowing to cut down on code bloat. This
 * is somewhat analogous to {@code Predef} in Scala. Note that this should only be used
 * when this typedef <b>does not sacrifice</b> the code readability.
 */
public final class X {
    /** An empty immutable {@code Object} array. */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    /** Time span dividers. */
    private static final long[] SPAN_DIVS = new long[] {1000L, 60L, 60L, 60L};

    /** The names of methods commonly used to access a wrapped exception. */
    private static final String[] CAUSE_MTD_NAMES = new String[] {
        "getCause",
        "getNextException",
        "getTargetException",
        "getException",
        "getSourceException",
        "getRootCause",
        "getCausedByException",
        "getNested",
        "getLinkedException",
        "getNestedException",
        "getLinkedCause",
        "getThrowable"
    };

    /** The Method object for Java 1.4 getCause. */
    private static final Method THROWABLE_CAUSE_METHOD;

    /**
     *
     */
    static {
        Method causeMtd;

        try {
            causeMtd = Throwable.class.getMethod("getCause", null);
        }
        catch (Exception ignored) {
            causeMtd = null;
        }

        THROWABLE_CAUSE_METHOD = causeMtd;
    }

    /**
     * Ensures singleton.
     */
    private X() {
        // No-op.
    }

    /**
     * Alias for {@code System.out.println()}.
     */
    public static void println() {
        System.out.println();
    }

    /**
     * Alias for {@code System.err.println()}.
     */
    public static void printerrln() {
        System.err.println();
    }

    /**
     * Alias for {@code System.out.println}.
     *
     * @param s1 First string to print.
     * @param rest Optional list of objects to print as well.
     */
    public static void println(@Nullable String s1, @Nullable Object... rest) {
        System.out.println(s1);

        if (rest != null && rest.length > 0)
            for (Object obj : rest)
                System.out.println(obj);
    }

    /**
     * Alias for {@code System.err.println}.
     *
     * @param s1 First string to print.
     * @param rest Optional list of objects to print as well.
     */
    public static void printerrln(@Nullable String s1, @Nullable Object... rest) {
        error(s1, rest);
    }

    /**
     * Alias for {@code System.err.println}.
     *
     * @param s1 First string to print.
     * @param rest Optional list of objects to print as well.
     */
    public static void error(@Nullable String s1, @Nullable Object... rest) {
        System.err.println(s1);

        if (rest != null && rest.length > 0)
            for (Object obj : rest)
                System.err.println(obj);
    }

    /**
     * Alias for {@code System.out.print}.
     *
     * @param s1 First string to print.
     * @param rest Optional list of objects to print as well.
     */
    public static void print(@Nullable String s1, @Nullable Object... rest) {
        System.out.print(s1);

        if (rest != null && rest.length > 0)
            for (Object obj : rest)
                System.out.print(obj);
    }

    /**
     * Alias for {@code System.err.print}.
     *
     * @param s1 First string to print.
     * @param rest Optional list of objects to print as well.
     */
    public static void printerr(@Nullable String s1, @Nullable Object... rest) {
        System.err.print(s1);

        if (rest != null && rest.length > 0)
            for (Object obj : rest)
                System.err.print(obj);
    }

    /**
     * Gets either system property or environment variable with given name.
     *
     * @param name Name of the system property or environment variable.
     * @return Value of the system property or environment variable. Returns
     *      {@code null} if neither can be found for given name.
     */
    @Nullable public static String getSystemOrEnv(String name) {
        assert name != null;

        String v = System.getProperty(name);

        if (v == null)
            v = System.getenv(name);

        return v;
    }

    /**
     * Creates string presentation of given time {@code span} in hh:mm:ss:msec {@code HMSM} format.
     *
     * @param span Time span.
     * @return String presentation.
     */
    public static String timeSpan2HMSM(long span) {
        long[] t = new long[4];

        long sp = span;

        for (int i = 0; i < SPAN_DIVS.length && sp > 0; sp /= SPAN_DIVS[i++])
            t[i] = sp % SPAN_DIVS[i];

        return (t[3] < 10 ? "0" + t[3] : Long.toString(t[3])) + ':' +
            (t[2] < 10 ? "0" + t[2] : Long.toString(t[2])) + ':' +
            (t[1] < 10 ? "0" + t[1] : Long.toString(t[1])) + ':' +
            (t[0] < 10 ? "0" + t[0] : Long.toString(t[0]));
    }

    /**
     * Creates string presentation of given time {@code span} in hh:mm:ss {@code HMS} format.
     *
     * @param span Time span.
     * @return String presentation.
     */
    public static String timeSpan2HMS(long span) {
        long[] t = new long[4];

        long sp = span;

        for (int i = 0; i < SPAN_DIVS.length && sp > 0; sp /= SPAN_DIVS[i++])
            t[i] = sp % SPAN_DIVS[i];

        return (t[3] < 10 ? "0" + t[3] : Long.toString(t[3])) + ':' +
            (t[2] < 10 ? "0" + t[2] : Long.toString(t[2])) + ':' +
            (t[1] < 10 ? "0" + t[1] : Long.toString(t[1]));
    }

    /**
     * Clones a passed in object. If parameter {@code deep} is set to {@code true}
     * then this method will use deep cloning algorithm based on deep reflection
     * ignoring {@link Cloneable} interface unless parameter {@code honorCloneable}
     * is set to false.
     * <p>
     * If {@code deep} is {@code false} then this method will check the object for
     * {@link Cloneable} interface and use {@link Object#clone()} to make a copy,
     * otherwise the object itself will be returned.
     *
     * @param obj Object to create a clone from.
     * @param deep {@code true} to use algorithm of deep cloning. If {@code false}
     *      then this method will always be checking whether a passed in object
     *      implements interface {@link Cloneable} and if it does then method
     *      {@link Object#clone()} will be used to clone object, if does not
     *      then the object itself will be returned.
     * @param honorCloneable Flag indicating whether {@link Cloneable} interface
     *      should be honored or not when cloning. This parameter makes sense only if
     *      parameter {@code deep} is set to {@code true}.
     * @param <T> Type of cloning object.
     * @return Copy of a passed in object.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public static <T> T cloneObject(@Nullable T obj, boolean deep, boolean honorCloneable) {
        if (obj == null)
            return null;

        try {
            return !deep ? shallowClone(obj) : (T)deepClone(new GridLeanMap<Integer, Integer>(),
                new ArrayList<>(), obj, honorCloneable);
        }
        catch (Exception e) {
            throw new IgniteException("Unable to clone instance of class: " + obj.getClass(), e);
        }
    }

    /**
     * @param obj Object to make a clone for.
     * @param <T> Type of cloning object.
     * @return Copy of a passed in object.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable private static <T> T shallowClone(@Nullable T obj) {
        if (obj == null)
            return null;

        if (!(obj instanceof Cloneable))
            return obj;

        if (obj.getClass().isArray())
            return obj instanceof byte[] ? (T)(((byte[])obj).clone()) :
                obj instanceof short[] ? (T)(((short[])obj).clone()) :
                    obj instanceof char[] ? (T)(((char[])obj).clone()) :
                        obj instanceof int[] ? (T)(((int[])obj).clone()) :
                            obj instanceof long[] ? (T)(((long[])obj).clone()) :
                                obj instanceof float[] ? (T)(((float[])obj).clone()) :
                                    obj instanceof double[] ? (T)(((double[])obj).clone()) :
                                        obj instanceof boolean[] ? (T)(((boolean[])obj).clone()) :
                                            (T)(((Object[])obj).clone());

        try {
            // 'getDeclaredMethods' searches for ALL methods, 'getMethods' - only public methods.
            Method mtd = obj.getClass().getDeclaredMethod("clone");

            boolean set = false;

            if (!mtd.isAccessible())
                mtd.setAccessible(set = true);

            T clone = (T)mtd.invoke(obj);

            if (set)
                mtd.setAccessible(false);

            return clone;
        }
        catch (Exception e) {
            throw new IgniteException("Unable to clone instance of class: " + obj.getClass(), e);
        }
    }

    /**
     * Recursively clones the object.
     *
     * @param identityIdxs Map of object identities to indexes in {@code clones} parameter.
     * @param clones List of already cloned objects.
     * @param obj The object to deep-clone.
     * @param honorCloneable {@code true} if method should account {@link Cloneable} interface.
     * @return Clone of the input object.
     * @throws Exception If deep-cloning fails.
     */
    @Nullable private static Object deepClone(Map<Integer, Integer> identityIdxs, List<Object> clones, @Nullable Object obj,
        boolean honorCloneable) throws Exception {
        if (obj == null)
            return null;

        if (honorCloneable && obj instanceof Cloneable)
            return shallowClone(obj);

        Integer idx = identityIdxs.get(System.identityHashCode(obj));

        Object clone = null;

        if (idx != null)
            clone = clones.get(idx);

        if (clone != null)
            return clone;

        if (obj instanceof Class)
            // No clone needed for java.lang.Class instance.
            return obj;

        Class cls = obj.getClass();

        if (cls.isArray()) {
            Class<?> arrType = cls.getComponentType();

            int len = Array.getLength(obj);

            clone = Array.newInstance(arrType, len);

            for (int i = 0; i < len; i++)
                Array.set(clone, i, deepClone(identityIdxs, clones, Array.get(obj, i), honorCloneable));

            clones.add(clone);

            identityIdxs.put(System.identityHashCode(obj), clones.size() - 1);

            return clone;
        }

        clone = U.forceNewInstance(cls);

        if (clone == null)
            throw new IgniteException("Failed to clone object (empty constructor could not be assigned): " + obj);

        clones.add(clone);

        identityIdxs.put(System.identityHashCode(obj), clones.size() - 1);

        for (Class<?> c = cls; c != Object.class; c = c.getSuperclass())
            for (Field f : c.getDeclaredFields())
                cloneField(identityIdxs, clones, obj, clone, f, honorCloneable);

        return clone;
    }

    /**
     * @param identityIdxs Map of object identities to indexes in {@code clones} parameter.
     * @param clones List of already cloned objects.
     * @param obj Object to clone.
     * @param clone Clone.
     * @param f Field to clone.
     * @param honorCloneable {@code true} if method should account {@link Cloneable} interface.
     * @throws Exception If failed.
     */
    private static void cloneField(Map<Integer, Integer> identityIdxs, List<Object> clones, Object obj, Object clone,
        Field f, boolean honorCloneable) throws Exception {
        int modifiers = f.getModifiers();

        // Skip over static fields.
        if (Modifier.isStatic(modifiers))
            return;

        boolean set = false;

        if (!f.isAccessible()) {
            f.setAccessible(true);

            set = true;
        }

        try {
            if (f.getType().isPrimitive())
                f.set(clone, f.get(obj));
            else
                f.set(clone, deepClone(identityIdxs, clones, f.get(obj), honorCloneable));
        }
        finally {
            if (set)
                f.setAccessible(false);
        }
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     * <p>
     * Note that this method follows includes {@link Throwable#getSuppressed()}
     * into check.
     *
     * @param t Throwable to check (if {@code null}, {@code false} is returned).
     * @param cls Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code True} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    @SafeVarargs
    public static boolean hasCause(@Nullable Throwable t, @Nullable Class<? extends Throwable>... cls) {
        if (t == null || F.isEmpty(cls))
            return false;

        assert cls != null;

        for (Throwable th = t; th != null; th = th.getCause()) {
            for (Class<? extends Throwable> c : cls) {
                if (c.isAssignableFrom(th.getClass()))
                    return true;
            }

            for (Throwable n : th.getSuppressed()) {
                if (hasCause(n, cls))
                    return true;
            }

            if (th.getCause() == th)
                break;
        }

        return false;
    }

    /**
     * Checks if passed throwable has given class in one of the suppressed exceptions.
     *
     * @param t Throwable to check (if {@code null}, {@code false} is returned).
     * @param cls Class to check.
     * @return {@code True} if one of the suppressed exceptions is an instance of passed class,
     *      {@code false} otherwise.
     */
    public static boolean hasSuppressed(@Nullable Throwable t, @Nullable Class<? extends Throwable> cls) {
        if (t == null || cls == null)
            return false;

        if (t.getSuppressed() != null) {
            for (Throwable th : t.getSuppressed()) {
                if (cls.isAssignableFrom(th.getClass()))
                    return true;

                if (hasSuppressed(th, cls))
                    return true;
            }
        }

        return false;
    }

    /**
     * Gets first cause if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy.
     * <p>
     * Note that this method follows includes {@link Throwable#getSuppressed()}
     * into check.
     *
     * @param t Throwable to check (if {@code null}, {@code null} is returned).
     * @param cls Cause class to get cause (if {@code null}, {@code null} is returned).
     * @return First causing exception of passed in class, {@code null} otherwise.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public static <T extends Throwable> T cause(@Nullable Throwable t, @Nullable Class<T> cls) {
        if (t == null || cls == null)
            return null;

        for (Throwable th = t; th != null; th = th.getCause()) {
            if (cls.isAssignableFrom(th.getClass()))
                return (T)th;

            for (Throwable n : th.getSuppressed()) {
                T found = cause(n, cls);

                if (found != null)
                    return found;
            }

            if (th.getCause() == th)
                break;
        }

        return null;
    }

    /**
     * @param throwable The exception to examine.
     * @return The wrapped exception, or {@code null} if not found.
     */
    private static Throwable getCauseUsingWellKnownTypes(Throwable throwable) {
        if (throwable instanceof SQLException)
            return ((SQLException)throwable).getNextException();

        if (throwable instanceof InvocationTargetException)
            return ((InvocationTargetException)throwable).getTargetException();

        return null;
    }

    /**
     * Finds a {@code Throwable} by method name.
     *
     * @param throwable The exception to examine.
     * @param mtdName The name of the method to find and invoke.
     * @return The wrapped exception, or {@code null} if not found.
     */
    private static Throwable getCauseUsingMethodName(Throwable throwable, String mtdName) {
        Method mtd = null;

        try {
            mtd = throwable.getClass().getMethod(mtdName, null);
        }
        catch (NoSuchMethodException | SecurityException ignored) {
            // exception ignored
        }

        if (mtd != null && Throwable.class.isAssignableFrom(mtd.getReturnType())) {
            try {
                return (Throwable)mtd.invoke(throwable, EMPTY_OBJECT_ARRAY);
            }
            catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ignored) {
                // exception ignored
            }
        }

        return null;
    }

    /**
     * Finds a {@code Throwable} by field name.
     *
     * @param throwable The exception to examine.
     * @param fieldName The name of the attribute to examine.
     * @return The wrapped exception, or {@code null} if not found.
     */
    private static Throwable getCauseUsingFieldName(Throwable throwable, String fieldName) {
        Field field = null;

        try {
            field = throwable.getClass().getField(fieldName);
        }
        catch (NoSuchFieldException | SecurityException ignored) {
            // exception ignored
        }

        if (field != null && Throwable.class.isAssignableFrom(field.getType())) {
            try {
                return (Throwable)field.get(throwable);
            }
            catch (IllegalAccessException | IllegalArgumentException ignored) {
                // exception ignored
            }
        }

        return null;
    }

    /**
     * Checks if the Throwable class has a {@code getCause} method.
     *
     * This is true for JDK 1.4 and above.
     *
     * @return True if Throwable is nestable.
     */
    public static boolean isThrowableNested() {
        return THROWABLE_CAUSE_METHOD != null;
    }

    /**
     * Checks whether this {@code Throwable} class can store a cause.
     *
     * This method does not check whether it actually does store a cause.
     *
     * @param throwable The {@code Throwable} to examine, may be null.
     * @return Boolean {@code true} if nested otherwise {@code false}.
     */
    public static boolean isNestedThrowable(Throwable throwable) {
        if (throwable == null)
            return false;

        if (throwable instanceof SQLException || throwable instanceof InvocationTargetException)
            return true;

        if (isThrowableNested())
            return true;

        Class<?> cls = throwable.getClass();
        for (String CAUSE_MTD_NAME : CAUSE_MTD_NAMES) {
            try {
                Method mtd = cls.getMethod(CAUSE_MTD_NAME, null);

                if (mtd != null && Throwable.class.isAssignableFrom(mtd.getReturnType()))
                    return true;
            }
            catch (NoSuchMethodException | SecurityException ignored) {
                // exception ignored
            }
        }

        try {
            Field field = cls.getField("detail");

            if (field != null)
                return true;
        }
        catch (NoSuchFieldException | SecurityException ignored) {
            // exception ignored
        }

        return false;
    }

    /**
     * Introspects the {@code Throwable} to obtain the cause.
     *
     * The method searches for methods with specific names that return a {@code Throwable} object.
     * This will pick up most wrapping exceptions, including those from JDK 1.4.
     *
     * The default list searched for are:</p> <ul> <li>{@code getCause()}</li>
     * <li>{@code getNextException()}</li> <li>{@code getTargetException()}</li>
     * <li>{@code getException()}</li> <li>{@code getSourceException()}</li>
     * <li>{@code getRootCause()}</li> <li>{@code getCausedByException()}</li>
     * <li>{@code getNested()}</li> </ul>
     *
     * <p>In the absence of any such method, the object is inspected for a {@code detail}
     * field assignable to a {@code Throwable}.</p>
     *
     * If none of the above is found, returns {@code null}.
     *
     * @param throwable The throwable to introspect for a cause, may be null.
     * @return The cause of the {@code Throwable},
     *         {@code null} if none found or null throwable input.
     */
    public static Throwable getCause(Throwable throwable) {
        return getCause(throwable, CAUSE_MTD_NAMES);
    }

    /**
     * Introspects the {@code Throwable} to obtain the cause.
     *
     * <ol> <li>Try known exception types.</li>
     * <li>Try the supplied array of method names.</li>
     * <li>Try the field 'detail'.</li> </ol>
     *
     * <p>A {@code null} set of method names means use the default set.
     * A {@code null} in the set of method names will be ignored.</p>
     *
     * @param throwable The throwable to introspect for a cause, may be null.
     * @param mtdNames The method names, null treated as default set.
     * @return The cause of the {@code Throwable}, {@code null} if none found or null throwable input.
     */
    public static Throwable getCause(Throwable throwable, String[] mtdNames) {
        if (throwable == null)
            return null;

        Throwable cause = getCauseUsingWellKnownTypes(throwable);

        if (cause == null) {
            if (mtdNames == null)
                mtdNames = CAUSE_MTD_NAMES;

            for (String mtdName : mtdNames) {
                if (mtdName != null) {
                    cause = getCauseUsingMethodName(throwable, mtdName);

                    if (cause != null)
                        break;
                }
            }

            if (cause == null)
                cause = getCauseUsingFieldName(throwable, "detail");
        }

        return cause;
    }

    /**
     * Returns the list of {@code Throwable} objects in the exception chain.
     *
     * A throwable without cause will return a list containing one element - the input throwable.
     * A throwable with one cause will return a list containing two elements - the input throwable
     * and the cause throwable. A {@code null} throwable will return a list of size zero.
     *
     * This method handles recursive cause structures that might otherwise cause infinite loops.
     * The cause chain is processed until the end is reached, or until the next item in the chain
     * is already in the result set.</p>
     *
     * @param throwable The throwable to inspect, may be null.
     * @return The list of throwables, never null.
     */
    public static List<Throwable> getThrowableList(Throwable throwable) {
        List<Throwable> list = new ArrayList<>();

        while (throwable != null && !list.contains(throwable)) {
            list.add(throwable);
            throwable = getCause(throwable);
        }

        return list;
    }

    /**
     * Returns the list of {@code Throwable} objects in the exception chain.
     *
     * A throwable without cause will return an array containing one element - the input throwable.
     * A throwable with one cause will return an array containing two elements - the input throwable
     * and the cause throwable. A {@code null} throwable will return an array of size zero.
     *
     * @param throwable The throwable to inspect, may be null.
     * @return The array of throwables, never null.
     */
    public static Throwable[] getThrowables(Throwable throwable) {
        List<Throwable> list = getThrowableList(throwable);

        return list.toArray(new Throwable[list.size()]);
    }

    /**
     * A way to get the entire nested stack-trace of an throwable.
     *
     * The result of this method is highly dependent on the JDK version
     * and whether the exceptions override printStackTrace or not.
     *
     * @param throwable The {@code Throwable} to be examined.
     * @return The nested stack trace, with the root cause first.
     */
    public static String getFullStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        Throwable[] ts = getThrowables(throwable);

        for (Throwable t : ts) {
            t.printStackTrace(pw);

            if (isNestedThrowable(t))
                break;
        }

        return sw.getBuffer().toString();
    }

    /**
     * Synchronously waits for all futures in the collection.
     *
     * @param futs Futures to wait for.
     */
    public static void waitAll(@Nullable Iterable<IgniteFuture<?>> futs) {
        if (F.isEmpty(futs))
            return;

        for (IgniteFuture fut : futs)
            fut.get();
    }

    /**
     * Pretty-formatting for minutes.
     *
     * @param mins Minutes to format.
     * @return Formatted presentation of minutes.
     */
    public static String formatMins(long mins) {
        assert mins >= 0;

        if (mins == 0)
            return "< 1 min";

        SB sb = new SB();

        long dd = mins / 1440; // 1440 mins = 60 mins * 24 hours

        if (dd > 0)
            sb.a(dd).a(dd == 1 ? " day " : " days ");

        mins %= 1440;

        long hh = mins / 60;

        if (hh > 0)
            sb.a(hh).a(hh == 1 ? " hour " : " hours ");

        mins %= 60;

        if (mins > 0)
            sb.a(mins).a(mins == 1 ? " min " : " mins ");

        return sb.toString().trim();
    }

    /**
     * Exits with code {@code -1} if maximum memory is below 90% of minimally allowed threshold.
     *
     * @param min Minimum memory threshold.
     */
    public static void checkMinMemory(long min) {
        long maxMem = Runtime.getRuntime().maxMemory();

        if (maxMem < .85 * min) {
            printerrln("Heap limit is too low (" + (maxMem / (1024 * 1024)) +
                "MB), please increase heap size at least up to " + (min / (1024 * 1024)) + "MB.");

            System.exit(-1);
        }
    }

    /**
     * Copies input byte stream to output byte stream.
     *
     * @param in Input byte stream.
     * @param out Output byte stream.
     * @param bufSize Intermediate buffer size.
     * @return Number of the copied bytes.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static int copy(InputStream in, OutputStream out, int bufSize) throws IOException {
        byte[] buf = new byte[bufSize];

        int cnt = 0;

        for (int n; (n = in.read(buf)) > 0;) {
            out.write(buf, 0, n);

            cnt += n;
        }

        return cnt;
    }

    /**
     * Tries to resolve Ignite installation home folder.
     *
     * @return Installation home folder.
     * @throws IgniteCheckedException If Ignite home folder was not set.
     */
    public static String resolveIgniteHome() throws IgniteCheckedException {
        String var = IgniteSystemProperties.getString(IGNITE_HOME);

        if (var != null)
            return var;
        else
            throw new IgniteCheckedException("Failed to resolve Ignite home folder " +
                "(please set 'IGNITE_HOME' environment or system variable)");
    }

    /**
     * Parses double from possibly {@code null} or invalid string.
     *
     * @param s String to parse double from. If string is null or invalid, a default value is used.
     * @param dflt Default value for double, if parsing failed.
     * @return Resulting double.
     */
    public static double parseDouble(@Nullable String s, double dflt) {
        try {
            return s != null ? Double.parseDouble(s) : dflt;
        }
        catch (NumberFormatException ignored) {
            return dflt;
        }
    }
}