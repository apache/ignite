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

package org.apache.ignite.tools.junit;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines global scope.
 * <p>
 * Contains often used utility functions allowing to cut down on code bloat. This
 * is somewhat analogous to {@code Predef} in Scala. Note that this should only be used
 * when this typedef <b>does not sacrifice</b> the code readability.
 */
final class X {
    /** An empty immutable {@code Object} array. */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

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
    private static boolean isThrowableNested() {
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
    private static boolean isNestedThrowable(Throwable throwable) {
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
    private static Throwable getCause(Throwable throwable) {
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
    private static Throwable getCause(Throwable throwable, String[] mtdNames) {
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
    private static List<Throwable> getThrowableList(Throwable throwable) {
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
    private static Throwable[] getThrowables(Throwable throwable) {
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
    static String getFullStackTrace(Throwable throwable) {
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
}
