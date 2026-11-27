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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Public declarations of a tree, printed in the format that README.md describes: one declaration per line with its area, key,
 * value, flags and location, separated by tabs. Texts are written without losses: two declarations that differ in any
 * character stay different. A collector that knows that some whitespace does not matter normalizes it itself.
 */
public class Declarations {
    /** Flag of a deprecated declaration. */
    public static final String DEPRECATED = "deprecated";

    /** Declarations by area and key. */
    private final Map<String, Declaration> decls = new TreeMap<>();

    /**
     * Adds a declaration. Declarations with the same key, such as a system property that two constants declare, are merged:
     * their values are listed together, so a change to any of them is a change of the merged declaration.
     *
     * @param flags Flags separated by commas, or an empty string.
     * @param location Where the declaration is.
     */
    public void add(String area, String key, String val, String flags, String location) {
        String k = escape(key, false);
        Declaration d = decls.computeIfAbsent(area + '\t' + k, x -> new Declaration(area, k));

        d.values.add(escape(val, true));
        d.locations.add(escape(location, false));

        for (String flag : flags.split(",")) {
            if (!flag.isEmpty())
                d.flags.add(flag);
        }
    }

    /** Prints the declarations. A merged declaration shows the first of its locations in sort order. */
    public void print(PrintStream out) {
        for (Declaration d : decls.values()) {
            out.println(String.join("\t", d.area, d.key, String.join(" | ", d.values), String.join(",", d.flags),
                d.locations.first()));
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

    /** Declaration as it is printed. */
    private static class Declaration {
        /** */
        final String area;

        /** */
        final String key;

        /** */
        final Set<String> values = new TreeSet<>();

        /** */
        final Set<String> flags = new TreeSet<>();

        /** */
        final TreeSet<String> locations = new TreeSet<>();

        /** */
        Declaration(String area, String key) {
            this.area = area;
            this.key = key;
        }
    }
}
