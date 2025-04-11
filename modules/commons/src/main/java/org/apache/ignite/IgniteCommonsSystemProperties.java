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

package org.apache.ignite;

import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.DFLT_TO_STRING_COLLECTION_LIMIT;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.DFLT_TO_STRING_INCLUDE_SENSITIVE;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.DFLT_TO_STRING_MAX_LENGTH;

/**
 * Contains constants for all common system properties and environmental variables in Ignite.
 * These properties and variables can be used to affect the behavior of Ignite.
 */
public class IgniteCommonsSystemProperties {
    /**
     * Setting to {@code true} enables writing sensitive information in {@code toString()} output.
     */
    @SystemProperty(value = "Enables writing sensitive information in toString() output",
        defaults = "" + DFLT_TO_STRING_INCLUDE_SENSITIVE)
    public static final String IGNITE_TO_STRING_INCLUDE_SENSITIVE = "IGNITE_TO_STRING_INCLUDE_SENSITIVE";

    /** Maximum length for {@code toString()} result. */
    @SystemProperty(value = "Maximum length for toString() result", type = Integer.class,
        defaults = "" + DFLT_TO_STRING_MAX_LENGTH)
    public static final String IGNITE_TO_STRING_MAX_LENGTH = "IGNITE_TO_STRING_MAX_LENGTH";

    /**
     * Limit collection (map, array) elements number to output.
     */
    @SystemProperty(value = "Number of collection (map, array) elements to output",
        type = Integer.class, defaults = "" + DFLT_TO_STRING_COLLECTION_LIMIT)
    public static final String IGNITE_TO_STRING_COLLECTION_LIMIT = "IGNITE_TO_STRING_COLLECTION_LIMIT";

    /**
     * @param enumCls Enum type.
     * @param name Name of the system property or environment variable.
     * @param <E> Type of the enum.
     * @return Enum value or {@code null} if the property is not set.
     */
    public static <E extends Enum<E>> E getEnum(Class<E> enumCls, String name) {
        return getEnum(enumCls, name, null);
    }

    /**
     * @param name Name of the system property or environment variable.
     * @param dflt Default value if property is not set.
     * @param <E> Type of the enum.
     * @return Enum value or the given default.
     */
    public static <E extends Enum<E>> E getEnum(String name, E dflt) {
        return getEnum(dflt.getDeclaringClass(), name, dflt);
    }

    /**
     * @param enumCls Enum type.
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Enum value or the given default.
     */
    private static <E extends Enum<E>> E getEnum(Class<E> enumCls, String name, E dflt) {
        assert enumCls != null;

        String val = getString(name);

        if (val == null)
            return dflt;

        try {
            return Enum.valueOf(enumCls, val);
        }
        catch (IllegalArgumentException ignore) {
            return dflt;
        }
    }

    /**
     * Gets either system property or environment variable with given name.
     *
     * @param name Name of the system property or environment variable.
     * @return Value of the system property or environment variable.
     *         Returns {@code null} if neither can be found for given name.
     */
    @Nullable public static String getString(String name) {
        assert name != null;

        String v = System.getProperty(name);

        if (v == null)
            v = System.getenv(name);

        return v;
    }

    /**
     * Gets either system property or environment variable with given name.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Value of the system property or environment variable.
     *         Returns {@code null} if neither can be found for given name.
     */
    @Nullable public static String getString(String name, String dflt) {
        String val = getString(name);

        return val == null ? dflt : val;
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code boolean} using {@code Boolean.valueOf()} method.
     *
     * @param name Name of the system property or environment variable.
     * @return Boolean value of the system property or environment variable.
     *         Returns {@code False} in case neither system property
     *         nor environment variable with given name is found.
     */
    public static boolean getBoolean(String name) {
        return getBoolean(name, false);
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code boolean} using {@code Boolean.valueOf()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Boolean value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public static boolean getBoolean(String name, boolean dflt) {
        String val = getString(name);

        return val == null ? dflt : Boolean.parseBoolean(val);
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code int} using {@code Integer.parseInt()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Integer value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public static int getInteger(String name, int dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        int res;

        try {
            res = Integer.parseInt(s);
        }
        catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code float} using {@code Float.parseFloat()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Float value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public static float getFloat(String name, float dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        float res;

        try {
            res = Float.parseFloat(s);
        }
        catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code long} using {@code Long.parseLong()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Integer value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public static long getLong(String name, long dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        long res;

        try {
            res = Long.parseLong(s);
        }
        catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code double} using {@code Double.parseDouble()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Integer value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public static double getDouble(String name, double dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        double res;

        try {
            res = Double.parseDouble(s);
        }
        catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }
}
