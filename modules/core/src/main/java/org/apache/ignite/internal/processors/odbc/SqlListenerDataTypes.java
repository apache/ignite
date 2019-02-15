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

package org.apache.ignite.internal.processors.odbc;

/**
 * Data type names usable in SQL queries
 * after escape sequence transformation
 */
public class SqlListenerDataTypes {
    /** Type name for 64-bit integer */
    public static final String BIGINT = "BIGINT";

    /** Type name for byte array */
    public static final String BINARY = "BINARY";

    /** Type name for boolean flag */
    public static final String BIT = "BIT";

    /** Type name for unicode string */
    public static final String CHAR = "CHAR";

    /** Type name for decimal number */
    public static final String DECIMAL = "DECIMAL";

    /** Type name for unicode string */
    public static final String VARCHAR = "VARCHAR";

    /** Type name for floating point number */
    public static final String DOUBLE = "DOUBLE";

    /** Type name for single precision floating point number */
    public static final String REAL = "REAL";

    /** Type name for universally unique identifier */
    public static final String UUID = "UUID";

    /** Type name for 16-bit integer */
    public static final String SMALLINT = "SMALLINT";

    /** Type name for 32-bit integer */
    public static final String INTEGER = "INTEGER";

    /** Type name for 8-bit integer */
    public static final String TINYINT = "TINYINT";

    /** Type name for date */
    public static final String DATE = "DATE";

    /** Type name for time */
    public static final String TIME = "TIME";

    /** Type name for timestamp */
    public static final String TIMESTAMP = "TIMESTAMP";
}
