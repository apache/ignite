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

package org.apache.ignite.internal.sql.command;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * SQL column type.
 */
public enum SqlColumnType {
    /** Boolean. */
    BOOLEAN,

    /** Byte. */
    BYTE,

    /** Short. */
    SHORT,

    /** Integer. */
    INT,

    /** Long. */
    LONG,

    /** Float. */
    FLOAT,

    /** Double. */
    DOUBLE,

    /** Decimal. */
    DECIMAL,

    /** Char. */
    CHAR,

    /** Varchar. */
    VARCHAR,

    /** Date. */
    DATE,

    /** Time. */
    TIME,

    /** Timestamp. */
    TIMESTAMP,

    /** UUID. */
    UUID;

    /** FIXME */
    public static @Nullable Class<?> classForType(@NotNull SqlColumnType typ) {
        switch (typ) {
            case BOOLEAN:
                return java.lang.Boolean.class;

            case BYTE:
                return java.lang.Byte.class;

            case SHORT:
                return java.lang.Short.class;

            case INT:
                return java.lang.Integer.class;

            case LONG:
                return java.lang.Long.class;

            case DECIMAL:
                return java.math.BigDecimal.class;

            case FLOAT:
                return java.lang.Float.class;

            case DOUBLE:
                return java.lang.Double.class;

            case TIME:
                return java.sql.Time.class;

            case DATE:
                return java.sql.Date.class;

            case TIMESTAMP:
                return java.sql.Timestamp.class;

            case CHAR:
            case VARCHAR:
                return java.lang.String.class;

            //case BYTES:
            case UUID:
                // "[B", not "byte[]";
                return byte[].class;

            default:
                return null;
        }
    }
}
