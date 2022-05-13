/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cache.query.index.sorted;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

/**
 * List of available types to use as index key.
 */
public class IndexKeyTypes {
    /** The data type is unknown at this time. */
    public static final int UNKNOWN = -1;

    /** The value type for NULL. */
    public static final int NULL = 0;

    /** The value type for BOOLEAN values. */
    public static final int BOOLEAN = 1;

    /** The value type for BYTE values. */
    public static final int BYTE = 2;

    /** The value type for SHORT values. */
    public static final int SHORT = 3;

    /** The value type for INT values. */
    public static final int INT = 4;

    /** The value type for INT values. */
    public static final int LONG = 5;

    /** The value type for DECIMAL values. */
    public static final int DECIMAL = 6;

    /** The value type for DOUBLE values. */
    public static final int DOUBLE = 7;

    /** The value type for FLOAT values. */
    public static final int FLOAT = 8;

    /** The value type for TIME values. */
    public static final int TIME = 9;

    /**
     * The value type for DATE values.
     */
    public static final int DATE = 10;

    /**
     * The value type for TIMESTAMP values.
     */
    public static final int TIMESTAMP = 11;

    /**
     * The value type for BYTES values.
     */
    public static final int BYTES = 12;

    /**
     * The value type for STRING values.
     */
    public static final int STRING = 13;

    /**
     * The value type for case insensitive STRING values.
     */
    public static final int STRING_IGNORECASE = 14;

    /**
     * The value type for BLOB values.
     */
    public static final int BLOB = 15;

    /**
     * The value type for CLOB values.
     */
    public static final int CLOB = 16;

    /**
     * The value type for ARRAY values.
     */
    public static final int ARRAY = 17;

    /**
     * The value type for RESULT_SET values.
     */
    public static final int RESULT_SET = 18;

    /**
     * The value type for JAVA_OBJECT values.
     */
    public static final int JAVA_OBJECT = 19;

    /**
     * The value type for UUID values.
     */
    public static final int UUID = 20;

    /**
     * The value type for string values with a fixed size.
     */
    public static final int STRING_FIXED = 21;

    /**
     * The value type for string values with a fixed size.
     */
    public static final int GEOMETRY = 22;

    // 23 was a short-lived experiment "TIMESTAMP UTC" which has been removed.

    /**
     * The value type for TIMESTAMP WITH TIME ZONE values.
     */
    public static final int TIMESTAMP_TZ = 24;

    /**
     * The value type for ENUM values.
     */
    public static final int ENUM = 25;

    /**
     *
     */
/*
    public static int of(Class<?> cls) {
        Class<?> x = cls;

        if (x == null || Void.TYPE == x) {
            return NULL;
        }
        if (x.isPrimitive()) {
            x = Utils.getNonPrimitiveClass(x);
        }
        if (String.class == x) {
            return STRING;
        } else if (Integer.class == x) {
            return INT;
        } else if (Long.class == x) {
            return LONG;
        } else if (Boolean.class == x) {
            return BOOLEAN;
        } else if (Double.class == x) {
            return DOUBLE;
        } else if (Byte.class == x) {
            return BYTE;
        } else if (Short.class == x) {
            return SHORT;
        } else if (Character.class == x) {
            throw DbException.get(
                ErrorCode.DATA_CONVERSION_ERROR_1, "char (not supported)");
        } else if (Float.class == x) {
            return FLOAT;
        } else if (byte[].class == x) {
            return BYTES;
        } else if (java.util.UUID.class == x) {
            return UUID;
        } else if (Void.class == x) {
            return NULL;
        } else if (BigDecimal.class.isAssignableFrom(x)) {
            return DECIMAL;
        } else if (ResultSet.class.isAssignableFrom(x)) {
            return RESULT_SET;
        } else if (Date.class.isAssignableFrom(x)) {
            return DATE;
        } else if (Time.class.isAssignableFrom(x)) {
            return TIME;
        } else if (Timestamp.class.isAssignableFrom(x)) {
            return TIMESTAMP;
        } else if (java.util.Date.class.isAssignableFrom(x)) {
            return TIMESTAMP;
        } else if (java.sql.Clob.class.isAssignableFrom(x)) {
            return CLOB;
        } else if (java.sql.Blob.class.isAssignableFrom(x)) {
            return BLOB;
        } else if (Object[].class.isAssignableFrom(x)) {
            // this includes String[] and so on
            return ARRAY;
        } else if (isGeometryClass(x)) {
            return GEOMETRY;
        } else if (LocalDate.class == x) {
            return DATE;
        } else if (LocalTime.class == x) {
            return TIME;
        } else if (LocalDateTime.class == x) {
            return TIMESTAMP;
        } else if (LocalDateTimeUtils.OFFSET_DATE_TIME == x || LocalDateTimeUtils.INSTANT == x) {
            return TIMESTAMP_TZ;
        } else {
            return JAVA_OBJECT;
        }
    }
*/
}
