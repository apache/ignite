/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
