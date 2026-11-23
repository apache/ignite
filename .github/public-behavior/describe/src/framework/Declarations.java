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

package framework;

import java.io.PrintStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Public declarations of a tree, printed in the format that README.md describes: one declaration per line with its area, key,
 * value, flags, location, rename group and deprecation, separated by tabs.
 */
public class Declarations {
    /** Flag of a deprecated declaration. */
    private static final String DEPRECATED = "deprecated";

    /** Flag of an experimental declaration. */
    private static final String EXPERIMENTAL = "experimental";

    /** Declarations by area and key. */
    private final Map<String, Declaration> decls = new TreeMap<>();

    /** Problems that make the description incomplete. */
    private final List<String> errors = new ArrayList<>();

    /** Adds a declaration whose key has no internal names. */
    public void add(String area, String key, String val, String flags, String location) {
        add(area, key, val, flags, location, null, "");
    }

    /**
     * Adds a declaration. Declarations with the same key, such as a system property that two constants declare, are merged:
     * their values are listed together, so a change to any of them is a change of the merged declaration.
     *
     * @param location Where the declaration is.
     * @param deprecation Where the annotation that deprecates the declaration is: its own, or that of its type or parent
     *      command. {@code null} if the declaration is not deprecated. The deprecation hint of a removed declaration uses it.
     * @param group Internal name in the key that users do not see, or an empty string. Comparison matches declarations whose
     *      group only changed its name, so the group must be a part of the key.
     */
    public void add(String area, String key, String val, String flags, String location, String deprecation, String group) {
        String k = oneLine(key);

        if (!k.contains(group))
            errors.add("Rename group " + group + " is not a part of the key " + k);

        Declaration d = decls.computeIfAbsent(area + '\t' + k, x -> new Declaration(area, k, location, deprecation, group));

        d.values.add(oneLine(val));

        if (!flags.isEmpty())
            d.flags.addAll(Arrays.asList(flags.split(",")));
    }

    /** Adds a problem that makes the description incomplete. */
    public void error(String problem) {
        errors.add(problem);
    }

    /** @return Problems that make the description incomplete. */
    public List<String> errors() {
        return errors;
    }

    /** Prints the declarations. */
    public void print(PrintStream out) {
        for (Declaration d : decls.values()) {
            out.println(String.join("\t", d.area, d.key, String.join(" | ", d.values), String.join(",", d.flags), d.location,
                d.group, d.deprecation == null ? "" : d.deprecation));
        }
    }

    /** @return Flags of a declaration. */
    public static String flags(boolean deprecated, boolean experimental) {
        return deprecated && experimental ? DEPRECATED + ',' + EXPERIMENTAL : deprecated ? DEPRECATED : experimental ? EXPERIMENTAL : "";
    }

    /** */
    public static String oneLine(String s) {
        return s.replaceAll("\\s+", " ").trim();
    }

    /** @return Value as Java source writes it. */
    public static String value(Object val) {
        if (val instanceof String)
            return '"' + ((String)val).replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\t", "\\t") + '"';

        if (val instanceof Character)
            return "'" + val + "'";

        if (val instanceof Class)
            return ((Class<?>)val).getTypeName() + ".class";

        if (val instanceof Enum)
            return ((Enum<?>)val).getDeclaringClass().getTypeName() + '.' + ((Enum<?>)val).name();

        if (val != null && val.getClass().isArray()) {
            List<String> items = new ArrayList<>();

            for (int i = 0; i < Array.getLength(val); i++)
                items.add(value(Array.get(val, i)));

            return '{' + String.join(", ", items) + '}';
        }

        return String.valueOf(val);
    }

    /** @return Value of a static field, which may be private. */
    public static Object staticValue(Field f) {
        try {
            f.setAccessible(true);

            return f.get(null);
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Declaration as it is printed. */
    private static class Declaration {
        /** */
        final String area;

        /** */
        final String key;

        /** */
        final String location;

        /** */
        final String deprecation;

        /** */
        final String group;

        /** */
        final Set<String> values = new TreeSet<>();

        /** */
        final Set<String> flags = new TreeSet<>();

        /** */
        Declaration(String area, String key, String location, String deprecation, String group) {
            this.area = area;
            this.key = key;
            this.location = location;
            this.deprecation = deprecation;
            this.group = group;
        }
    }
}
