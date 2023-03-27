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

package org.apache.ignite.internal.processors.odbc.odbc.escape;

/**
 * ODBC escape sequence type.
 */
public enum OdbcEscapeType {
    /** Scalar function. */
    SCALAR_FUNCTION("fn", true, false),

    /** Outer join. */
    OUTER_JOIN("oj", true, false),

    /** Stored procedure call */
    CALL("call", true, false),

    /** Date. */
    DATE("d", true, false),

    /** Timestamp. */
    TIMESTAMP("ts", true, false),

    /** Time. */
    TIME("t", true, false),

    /** GUID. */
    GUID("guid", true, false),

    /** LIKE escape character clause. */
    ESCAPE_WO_TOKEN("\'", false, false),

    /** LIKE escape character clause. */
    ESCAPE("escape", true, false);

    /** Values in convenient order. */
    private static final OdbcEscapeType[] VALS = new OdbcEscapeType[] {
        SCALAR_FUNCTION, // Assume that scalar functions are very frequent.
        DATE, TIMESTAMP, // Date and timestamp are relatively frequent as well; also TS must go before T.
        OUTER_JOIN,      // Joins are less frequent,
        CALL,            // Procedure calls are less frequent than joins.
        ESCAPE_WO_TOKEN, ESCAPE, TIME, GUID // LIKE, TIME and GUID are even less frequent.
    };

    /**
     * Get values in convenient order, where the most frequent values goes first, and "startsWith" invocation is
     * enough to get type (i.e. "ts" goes before "t").
     *
     * @return Values.
     */
    public static OdbcEscapeType[] sortedValues() {
        return VALS;
    }

    /** Escape sequence body. */
    private final String body;

    /** Whether this is a standard token with no special handling. */
    private final boolean standard;

    /** Whether empty escape sequence is allowed. */
    private final boolean allowEmpty;

    /**
     * Constructor.
     *
     * @param body Escape sequence body.
     * @param standard Whether this is a standard token with no special handling.
     * @param allowEmpty Whether empty escape sequence is allowed.
     */
    OdbcEscapeType(String body, boolean standard, boolean allowEmpty) {
        this.body = body;
        this.standard = standard;
        this.allowEmpty = allowEmpty;
    }

    /**
     * @return Escape sequence body.
     */
    public String body() {
        return body;
    }

    /**
     * @return Whether this is a standard token with no special handling.
     */
    public boolean standard() {
        return standard;
    }

    /**
     * @return Whether empty escape sequence is allowed.
     */
    public boolean allowEmpty() {
        return allowEmpty;
    }
}
