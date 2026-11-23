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
 * value, flags, location, rename group and deprecation, separated by tabs. Texts are written without losses: two declarations
 * that differ in any character stay different. A collector that knows that some whitespace does not matter, such as in an SQL
 * signature, normalizes it itself.
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
        add(area, key, val, flags, location, "", null);
    }

    /**
     * Adds a declaration. Declarations with the same key, such as a system property that two constants declare, are merged:
     * their values are listed together, so a change to any of them is a change of the merged declaration.
     *
     * @param location Where the declaration is.
     * @param group Internal name in the key that users do not see, or an empty string. Comparison matches declarations whose
     *      group only changed its name, so the group must be a part of the key.
     * @param deprecation Where the annotation that deprecates the declaration is: its own, or that of its type or parent
     *      command. {@code null} if the declaration is not deprecated. The deprecation hint of a removed declaration uses it.
     */
    public void add(String area, String key, String val, String flags, String location, String group, String deprecation) {
        if (!key.contains(group))
            errors.add("Rename group " + group + " is not a part of the key " + key);

        String k = escape(key, false);
        Declaration d = decls.computeIfAbsent(area + '\t' + k, x -> new Declaration(area, k, escape(location, false),
            escape(group, false), deprecation == null ? "" : escape(deprecation, false)));

        d.values.add(escape(val, true));

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
                d.group, d.deprecation));
        }
    }

    /**
     * @param value Whether the text is a value. Values of the same key are joined with {@code " | "}, so {@code |} in a value is
     *      escaped too.
     * @return Text on one line from which the original can be restored. A backslash, a control character and a space at either
     *      end are escaped, so the report shows them.
     */
    private static String escape(String s, boolean value) {
        StringBuilder res = new StringBuilder();

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (c == '\\' || value && c == '|')
                res.append('\\').append(c);
            else if (c == '\t')
                res.append("\\t");
            else if (c == '\n')
                res.append("\\n");
            else if (c == '\r')
                res.append("\\r");
            else if (c < ' ')
                res.append(String.format("\\u%04x", (int)c));
            else if (c == ' ' && (i == 0 || i == s.length() - 1))
                res.append("\\s");
            else
                res.append(c);
        }

        return res.toString();
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
        final String group;

        /** */
        final String deprecation;

        /** */
        final Set<String> values = new TreeSet<>();

        /** */
        final Set<String> flags = new TreeSet<>();

        /** */
        Declaration(String area, String key, String location, String group, String deprecation) {
            this.area = area;
            this.key = key;
            this.location = location;
            this.group = group;
            this.deprecation = deprecation;
        }
    }
}
