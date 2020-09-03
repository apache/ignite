/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.configuration.distributed;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.function.Function;
import org.apache.ignite.plugin.security.SecurityPermission;

/**
 * Implements type-independent part of the public property.
 */
public class PublicSimpleProperty<T extends Serializable> implements PublicProperty<T> {
    /** Description. */
    private final String desc;

    /** Read permission. */
    private final SecurityPermission readPerm;

    /** Write permission. */
    private final SecurityPermission writePerm;

    /** Parser. */
    private final Function<String, T> parser;

    /** Formatter. */
    private final Function<T, String> formatter;

    /**
     * @param desc Description.
     * @param readPerm Read permission.
     * @param writePerm Write permission.
     * @param parser Parser to convert text representation to property value.
     */
    public PublicSimpleProperty(
        String desc,
        SecurityPermission readPerm,
        SecurityPermission writePerm,
        Function<String, T> parser
    ) {
        this(desc, readPerm, writePerm, parser, Objects::toString);
    }

    /**
     * @param desc Description.
     * @param readPerm Read permission.
     * @param writePerm Write permission.
     * @param parser Parser to convert text representation to property value.
     * @param formatter Formatter to convert property value to test representation.
     */
    public PublicSimpleProperty(
        String desc,
        SecurityPermission readPerm,
        SecurityPermission writePerm,
        Function<String, T> parser,
        Function<T, String> formatter
    ) {
        this.desc = desc;
        this.readPerm = readPerm;
        this.writePerm = writePerm;
        this.parser = parser;
        this.formatter = formatter;
    }

    @Override public T parse(String str) {
        return parser.apply(str);
    }

    /** {@inheritDoc} */
    @Override public String format(T val) {
        return formatter.apply(val);
    }

    /** {@inheritDoc} */
    @Override public String description() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public SecurityPermission readPermission() {
        return readPerm;
    }

    /** {@inheritDoc} */
    @Override public SecurityPermission writePermission() {
        return writePerm;
    }

    /**
     * @param val String to parse.
     * @return Integer value.
     */
    public static Integer parseNonNegativeInteger(String val) {
        if (val == null || val.trim().isEmpty())
            return null;

        int intVal = Integer.parseInt(val);

        if (intVal < 0)
            throw new IllegalArgumentException("The value must not be negative");

        return intVal;
    }

    /**
     * @param val String to parse.
     * @return Long value.
     */
    public static Long parseNonNegativeLong(String val) {
        if (val == null || val.trim().isEmpty())
            return null;

        long intVal = Long.parseLong(val);

        if (intVal < 0)
            throw new IllegalArgumentException("The value must not be negative");

        return intVal;
    }

    /**
     * @param val String value.
     * @return String set.
     */
    public static HashSet<String> parseStringSet(String val) {
        HashSet<String> set = new HashSet<>();

        if (val == null || val.trim().isEmpty())
            return set;

        String[] vals = val.split("\\W+");

        set.addAll(Arrays.asList(vals));

        return set;
    }
}
