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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Contains constants for all system properties and environmental variables in Ignite.
 * These properties and variables can be used to affect the behavior of Ignite.
 */
public enum IgniteSystemProperty {
    /**
     * If this system property is present the Ignite will include instance name into verbose log.
     */
    IGNITE_LOG_INSTANCE_NAME("If this system property is present the Ignite will include instance name " +
        "into verbose log."),

    /**
     * If this system property is present the Ignite will include grid name into verbose log.
     *
     * @deprecated Use {@link #IGNITE_LOG_INSTANCE_NAME}.
     */
    @Deprecated
    IGNITE_LOG_GRID_NAME("If this system property is present the Ignite will include grid name into " +
        "verbose log. Deprecated: use " + IGNITE_LOG_INSTANCE_NAME.name() + " instead."),

    /**
     * This property is used internally to pass an exit code to loader when Ignite instance is being restarted.
     */
    IGNITE_RESTART_CODE("This property is used internally to pass an exit code to loader when Ignite instance " +
        "is being restarted."),

    /**
     * Presence of this system property with value {@code true} will make the grid
     * node start as a daemon node. Node that this system property will override
     * {@link org.apache.ignite.configuration.IgniteConfiguration#isDaemon()} configuration.
     */
    IGNITE_DAEMON("Presence of this system property with value [true] will make the grid node start " +
        "as a daemon node. Node that this system property will override configuration."),

    /**
     * Page lock tracker type.
     * -1 - Disable lock tracking.
     *  1 - HEAP_STACK
     *  2 - HEAP_LOG
     *  3 - OFF_HEAP_STACK
     *  4 - OFF_HEAP_LOG
     *
     * Default is 2 - HEAP_LOG.
     */
    IGNITE_PAGE_LOCK_TRACKER_TYPE("Page lock tracker type." + U.nl()+
        "   -1 - Disable lock tracking." + U.nl()+
        "   1 - HEAP_STACK" + U.nl()+
        "   2 - HEAP_LOG" + U.nl()+
        "   3 - OFF_HEAP_STACK" + U.nl()+
        "   4 - OFF_HEAP_LOG" + U.nl()+
        U.nl() +
        "   Default is 2 - HEAP_LOG."),

    /**
     * Sets default {@link CacheConfiguration#setDiskPageCompression disk page compression}.
     */
    IGNITE_DEFAULT_DISK_PAGE_COMPRESSION("Sets default [CacheConfiguration#setDiskPageCompression] " +
        "disk page compression}.");

    /** System property description. */
    private final String desc;

    /**
     * @param desc System property description.
     */
    IgniteSystemProperty(String desc) {
        this.desc = desc;
    }

    /** @return System property description. */
    public String description() {
        return desc;
    }

    /**
     * Gets either system property or environment variable with given name.
     *
     * @return Value of the system property or environment variable.
     *         Returns {@code null} if neither can be found for given name.
     */
    @Nullable public String getString() {
        String v = System.getProperty(name());

        if (v == null)
            v = System.getenv(name());

        return v;
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code boolean} using {@code Boolean.valueOf()} method.
     *
     * @param dflt Default value.
     * @return Boolean value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public boolean getBoolean(boolean dflt) {
        String val = getString();

        return val == null ? dflt : Boolean.parseBoolean(val);
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code int} using {@code Integer.parseInt()} method.
     *
     * @param dflt Default value.
     * @return Integer value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public int getInteger(int dflt) {
        String s = getString();

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
     * @param enumCls Enum type.
     * @param dflt Default value.
     * @return Enum value or the given default.
     */
    public <E extends Enum<E>> E getEnum(Class<E> enumCls, String name, E dflt) {
        assert enumCls != null;

        String val = getString();

        if (val == null)
            return dflt;

        try {
            return Enum.valueOf(enumCls, val);
        }
        catch (IllegalArgumentException ignore) {
            return dflt;
        }
    }

    /** Prints properties info to console. */
    public static void printFormatted() {
        List<IgniteSystemProperty> props = Arrays.asList(values());

        props.sort(Comparator.comparing(Enum::name));

        X.println("Ignite system properties:");

        for (IgniteSystemProperty prop : props) {
            boolean deprecated = U.findField(IgniteSystemProperty.class, prop.name())
                .isAnnotationPresent(Deprecated.class);

            String deprecatedStr = deprecated ? "[Deprecated] " : "";

            X.println(String.format("%-30s - %s%s", prop.name(), deprecatedStr, prop.description()));
        }
    }
}
