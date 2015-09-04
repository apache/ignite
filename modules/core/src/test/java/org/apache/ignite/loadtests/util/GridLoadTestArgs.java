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

package org.apache.ignite.loadtests.util;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

/**
 * Contains constants and methods for working with
 * command line arguments, JVM properties and environment
 * variables.
 */
public class GridLoadTestArgs {
    /** Cache name. */
    public static final String CACHE_NAME = "IGNITE_CACHE_NAME";

    /** Threads count. */
    public static final String THREADS_CNT = "IGNITE_THREADS_COUNT";

    /** Test duration in seconds. */
    public static final String TEST_DUR_SEC = "IGNITE_TEST_DUR_SEC";

    /** Value size. */
    public static final String VALUE_SIZE = "IGNITE_VALUE_SIZE";

    /** Properties map for dumping. */
    private static ThreadLocal<Map<String, String>> props = new ThreadLocal<Map<String, String>>() {
        @Override protected Map<String, String> initialValue() {
            return new HashMap<>();
        }
    };

    /**
     * Gets the value of either JVM property or environment variable,
     * if property is not set.
     *
     * @param name Property name.
     * @param dflt Default value.
     * @return JVM property value or environment variable value if
     *         JVM property is {@code null} or default value if both
     *         are {@code null}.
     */
    public static String getStringProperty(String name, String dflt) {
        String ret = getStringProperty0(name);

        if (ret == null)
            ret = dflt;

        props.get().put(name, ret);

        return ret;
    }

    /**
     * Gets the value of either JVM property or environment variable,
     * if property is not set.
     *
     * @param name Property name.
     * @return JVM property value or environment variable value if
     *         JVM property is undefined. Returns {@code null} if
     *         both JVM property and environment variable are not set.
     */
    @Nullable public static String getStringProperty(String name) {
        return saveProperty(name, getStringProperty0(name));
    }

    /**
     * Helper method for getting property values.
     *
     * @param name Property name.
     * @return JVM property value or environment variable value if
     *         JVM property is undefined. Returns {@code null} if
     *         both JVM property and environment variable are not set.
     */
    @Nullable private static String getStringProperty0(String name) {
        String ret = System.getProperty(name);

        return ret != null ? ret : System.getenv(name);
    }

    /**
     * Gets the integer value of either JVM property or environment variable,
     * if property is not set.
     *
     * @param name Property name.
     * @return JVM property value or environment variable value if
     *         JVM property is {@code null} or {@code null} if both
     *         are {@code null}.
     */
    @Nullable public static Integer getIntProperty(String name) {
        return saveProperty(name, getIntProperty0(name));
    }

    /**
     * Gets the integer value of either JVM property or environment variable,
     * if property is not set.
     *
     * @param name Property name.
     * @param dflt Default value.
     * @return JVM property value or environment variable value if
     *         JVM property is {@code null} or default value if both
     *         are {@code null}.
     */
    @SuppressWarnings("ConstantConditions")
    public static int getIntProperty(String name, int dflt) {
        Integer ret = getIntProperty0(name);

        return saveProperty(name, ret != null ? ret : dflt);
    }

    /**
     * Helper method for getting int properties.
     *
     * @param name Property name.
     * @return JVM property value or environment variable value if
     *         JVM property is {@code null} or {@code null} if both
     *         are {@code null}.
     */
    @Nullable private static Integer getIntProperty0(String name) {
        Integer ret = Integer.getInteger(name);

        if (ret == null) {
            String env = System.getenv(name);

            ret = env != null ? Integer.valueOf(env) : null;
        }

        return ret;
    }

    /**
     * Gets the integer value of either JVM property or environment variable,
     * if property is not set.
     *
     * @param name Property name.
     * @param dflt Default value.
     * @param validClo Value validation closure, which returns {@code null}, if the value
     *                 is valid, and error message, if it's not valid.
     * @return JVM property value or environment variable value if
     *         JVM property is {@code null} or default value if both
     *         are {@code null}.
     * @throws IgniteCheckedException If the value didn't pass the validation.
     */
    public static int getIntProperty(String name, int dflt, IgniteClosure<Integer, String> validClo)
        throws IgniteCheckedException {
        int ret = getIntProperty(name, dflt);

        String errMsg = validClo.apply(ret);

        if (errMsg != null)
            throw new IgniteCheckedException("Illegal value for " + name + " parameter: " + errMsg);

        return ret;
    }

    /**
     * Gets the long value of either JVM property or environment variable,
     * if property is not set.
     *
     * @param name Property name.
     * @return JVM property value or environment variable value if
     *         JVM property is undefined. Returns {@code null} if
     *         both JVM property and environment variable are not set.
     */
    @Nullable public static Long getLongProperty(String name) {
        return saveProperty(name, getLongProperty0(name));
    }

    /**
     * Gets the long value of either JVM property or environment variable,
     * if property is not set.
     *
     * @param name Property name.
     * @param dflt Default value.
     * @return JVM property value or environment variable value if
     *         JVM property is {@code null} or default value if both
     *         are {@code null}.
     */
    @SuppressWarnings("ConstantConditions")
    public static long getLongProperty(String name, long dflt) {
        Long ret = getLongProperty(name);

        return saveProperty(name, ret != null ? ret : dflt);
    }

    /**
     * Helper method for getting long property.
     *
     * @param name Property name.
     * @return JVM property value or environment variable value if
     *         JVM property is undefined. Returns {@code null} if
     *         both JVM property and environment variable are not set.
     */
    @Nullable private static Long getLongProperty0(String name) {
        Long ret = Long.getLong(name);

        if (ret == null) {
            String env = System.getenv(name);

            ret = env != null ? Long.valueOf(env) : null;
        }

        return ret;
    }

    /**
     * Gets the boolean value of either JVM property or environment variable,
     * if property is not set.
     *
     * @param name Property name.
     * @param dflt Default value.
     * @return JVM property value or environment variable value if
     *         JVM property is {@code null} or default value if both
     *         are {@code null}.
     */
    @SuppressWarnings("ConstantConditions")
    public static boolean getBooleanProperty(String name, boolean dflt) {
        Boolean ret = Boolean.getBoolean(name);

        if (ret == null) {
            String env = System.getenv(name);

            ret = env != null ? Boolean.valueOf(env) : null;
        }

        return saveProperty(name, ret != null ? ret : dflt);
    }

    /**
     * Prints a message about undefined JVM property to standard
     * error.
     *
     * @param propName JVM property name.
     */
    public static void printErrorUndefined(String propName) {
        System.err.println("JVM property " + propName + " should be defined " +
            "(use -D" + propName + "=...)");
    }

    /**
     * Dumps the properties (name + value), that were retrieved using
     * {@code get[type]Property()}.
     *
     * @param out Output stream to dump properties to.
     */
    public static void dumpProperties(PrintStream out) {
        for (Map.Entry<String, String> prop : props.get().entrySet())
            out.println(prop.getKey() + ": " + prop.getValue());
    }

    /**
     * Helper method for saving a property to thread local for later use in
     * {@link #dumpProperties(PrintStream)}.
     *
     * @param name Property name.
     * @param val Property value.
     * @return Property value.
     */
    @Nullable private static <T> T saveProperty(String name, @Nullable T val) {
        props.get().put(name, val != null ? val.toString() : null);

        return val;
    }
}