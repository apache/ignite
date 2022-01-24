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

package org.apache.ignite.internal.util;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Utility methods used for cluster's named objects: schemas, tables, columns, indexes, etc.
 */
public class IgniteObjectName {
    /** No instance methods. */
    private IgniteObjectName() {
    }

    /**
     * Parse database's object name: unquote name or cast to upper case not-quoted name.
     *
     * @param str String to parse object name.
     * @return Unquoted name or name is cast to upper case. "tbl0" -&gt; "TBL0", "\"Tbl0\"" -&gt; "Tbl0".
     */
    public static String parse(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }

        if (str.startsWith("\"") && str.endsWith("\"")) {
            if (str.length() == 1) {
                throw new IgniteInternalException("Invalid identifier: single quota");
            }

            return str.substring(1, str.length() - 1);
        } else {
            return str.toUpperCase();
        }
    }

    /**
     * Parse canonical table [schemaName].[tableName], unquote identifiers and normalize case, e.g. "public.tbl0" -&gt; "PUBLIC.TBL0",
     * "PUBLIC.\"Tbl0\"" -&gt; "PUBLIC.Tbl0", "\"MySchema\".\"Tbl0\"" -&gt; "MySchema.Tbl0", etc.
     *
     * @param str String to parse canonical name.
     * @return Unquote identifiers and normalize case.
     */
    public static String parseCanonicalName(String str) {
        if (str == null || str.isEmpty()) {
            throw new IgniteInternalException("Invalid identifier: empty string");
        }

        StringBuilder name = new StringBuilder();
        boolean quoted = false;
        int idBegin = 0;
        int idEnd = 0;

        for (int i = 0; i < str.length(); ++i) {
            switch (str.charAt(i)) {
                case '\"':
                    if (quoted) {
                        idEnd = i;
                    } else {
                        idBegin = i + 1;
                        quoted = true;
                    }

                    break;

                case '.':
                    if (!quoted) {
                        idEnd = i;
                    }

                    String id = str.substring(idBegin, idEnd);

                    name.append(quoted ? id : id.toUpperCase()).append('.');

                    quoted = false;
                    idBegin = i + 1;

                    break;

                default:
                    break;
            }
        }

        if (!quoted) {
            idEnd = str.length();
        }

        // append last name
        String id = str.substring(idBegin, idEnd);

        name.append(quoted ? id : id.toUpperCase());

        return name.toString();
    }

    /**
     * Quote all database's objects names at the collection.
     *
     * @param names Collection of objects names.
     * @return List of the quoted objects names.
     */
    public static List<String> quoteNames(List<String> names) {
        if (names == null) {
            return null;
        }

        return names.stream().map(IgniteObjectName::quote).collect(Collectors.toList());
    }

    /**
     * Quote database's object name, e.g. "myColumn" -&gt; "\"myColumn\""
     *
     * @param str Object name.
     * @return Quoted object name.
     */
    public static String quote(String str) {
        return "\"" + str + "\"";
    }
}