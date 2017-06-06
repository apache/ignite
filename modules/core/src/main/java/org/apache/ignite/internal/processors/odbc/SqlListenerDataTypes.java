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
