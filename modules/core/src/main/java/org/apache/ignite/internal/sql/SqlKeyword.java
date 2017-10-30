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

package org.apache.ignite.internal.sql;

import org.apache.ignite.IgniteException;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

/**
 * SQL keyword constants.
 */
public class SqlKeyword {
    /** Keyword: ASC. */
    public static final String ASC = "ASC";

    /** Keyword: CREATE. */
    public static final String CREATE = "CREATE";

    /** Keyword: DESC. */
    public static final String DESC = "DESC";

    /** Keyword: DROP. */
    public static final String DROP = "DROP";

    /** Keyword: EXISTS. */
    public static final String EXISTS = "EXISTS";

    /** Keyword: FULLTEXT. */
    public static final String FULLTEXT = "FULLTEXT";

    /** Keyword: IF. */
    public static final String IF = "IF";

    /** Keyword: INDEX. */
    public static final String INDEX = "INDEX";

    /** Keyword: KEY. */
    public static final String KEY = "KEY";

    /** Keyword: NOT. */
    public static final String NOT = "NOT";

    /** Keyword: ON. */
    public static final String ON = "ON";

    /** Keyword: PRIMARY. */
    public static final String PRIMARY = "PRIMARY";

    /** Keyword: SPATIAL. */
    public static final String SPATIAL = "SPATIAL";

    /** Keyword: TABLE. */
    public static final String TABLE = "TABLE";

    /** All keywords. */
    private static final HashSet<String> KEYWORDS;

    static {
        KEYWORDS = new HashSet<>();

        try {
            for (Field field : SqlKeyword.class.getDeclaredFields()) {
                String val = (String)field.get(null);

                KEYWORDS.add(val);
            }
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteException("Failed to initialize keywords collection.", e);
        }
    }

    /**
     * Check if string is a keyword.
     *
     * @param str String.
     * @return {@code True} if it is a keyword.
     */
    public static boolean isKeyword(String str) {
        return KEYWORDS.contains(str);
    }

    /**
     * Private constructor.
     */
    private SqlKeyword() {
        // No-op.
    }
}
