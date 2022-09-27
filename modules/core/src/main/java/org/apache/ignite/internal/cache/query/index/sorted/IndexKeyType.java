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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * List of available types to use as index key.
 */
public enum IndexKeyType {
    /** The data type is unknown at this time. */
    UNKNOWN(-1),

    /** The value type for NULL. */
    NULL(0),

    /** The value type for BOOLEAN values. */
    BOOLEAN(1),

    /** The value type for BYTE values. */
    BYTE(2),

    /** The value type for SHORT values. */
    SHORT(3),

    /** The value type for INT values. */
    INT(4),

    /** The value type for LONG values. */
    LONG(5),

    /** The value type for DECIMAL values. */
    DECIMAL(6),

    /** The value type for DOUBLE values. */
    DOUBLE(7),

    /** The value type for FLOAT values. */
    FLOAT(8),

    /** The value type for TIME values. */
    TIME(9),

    /**
     * The value type for DATE values.
     */
    DATE(10),

    /**
     * The value type for TIMESTAMP values.
     */
    TIMESTAMP(11),

    /**
     * The value type for BYTES values.
     */
    BYTES(12),

    /**
     * The value type for STRING values.
     */
    STRING(13),

    /**
     * The value type for case insensitive STRING values.
     */
    STRING_IGNORECASE(14),

    /**
     * The value type for BLOB values.
     */
    BLOB(15),

    /**
     * The value type for CLOB values.
     */
    CLOB(16),

    /**
     * The value type for ARRAY values.
     */
    ARRAY(17),

    /**
     * The value type for RESULT_SET values.
     */
    RESULT_SET(18),

    /**
     * The value type for JAVA_OBJECT values.
     */
    JAVA_OBJECT(19),

    /**
     * The value type for UUID values.
     */
    UUID(20),

    /**
     * The value type for string values with a fixed size.
     */
    STRING_FIXED(21),

    /**
     * The value type for string values with a fixed size.
     */
    GEOMETRY(22),

    // 23 was a short-lived experiment "TIMESTAMP UTC" which has been removed.

    /**
     * The value type for TIMESTAMP WITH TIME ZONE values.
     */
    TIMESTAMP_TZ(24),

    /**
     * The value type for ENUM values.
     */
    ENUM(25);

    /**
     * Code for value type. Should be compatible with H2 value types.
     */
    private final int code;

    /** */
    IndexKeyType(int code) {
        this.code = code;
    }

    /** */
    public int code() {
        return code;
    }

    /** */
    private static final IndexKeyType[] keyTypesByCode;

    static {
        int maxCode = Collections.max(Arrays.asList(values()), Comparator.comparing(IndexKeyType::code)).code;

        keyTypesByCode = new IndexKeyType[maxCode + 1];

        for (IndexKeyType type : values()) {
            assert type.code >= 0 || type == UNKNOWN; // Only one negative value is allowed.

            if (type.code >= 0)
                keyTypesByCode[type.code] = type;
        }
    }

    /**
     * Find type by code.
     */
    public static IndexKeyType forCode(int code) {
        if (code == UNKNOWN.code)
            return UNKNOWN;

        if (code < 0 || code >= keyTypesByCode.length)
            throw new IllegalArgumentException("Argument is invalid: " + code);

        return keyTypesByCode[code];
    }

    /**
     * Find type by class.
     */
    public static IndexKeyType forClass(Class<?> cls) {
        if (cls == null)
            return NULL;

        cls = U.box(cls);

        if (String.class == cls)
            return STRING;
        else if (Integer.class == cls)
            return INT;
        else if (Long.class == cls)
            return LONG;
        else if (Boolean.class == cls)
            return BOOLEAN;
        else if (Double.class == cls)
            return DOUBLE;
        else if (Byte.class == cls)
            return BYTE;
        else if (Short.class == cls)
            return SHORT;
        else if (Character.class == cls)
            throw new IgniteException("Cannot convert class char (not supported)");
        else if (Float.class == cls)
            return FLOAT;
        else if (byte[].class == cls)
            return BYTES;
        else if (java.util.UUID.class == cls)
            return UUID;
        else if (Void.class == cls)
            return NULL;
        else if (BigDecimal.class.isAssignableFrom(cls))
            return DECIMAL;
        else if (ResultSet.class.isAssignableFrom(cls))
            return RESULT_SET;
        else if (Date.class.isAssignableFrom(cls))
            return DATE;
        else if (Time.class.isAssignableFrom(cls))
            return TIME;
        else if (Timestamp.class.isAssignableFrom(cls))
            return TIMESTAMP;
        else if (java.util.Date.class.isAssignableFrom(cls))
            return TIMESTAMP;
        else if (java.sql.Clob.class.isAssignableFrom(cls))
            return CLOB;
        else if (java.sql.Blob.class.isAssignableFrom(cls))
            return BLOB;
        else if (Object[].class.isAssignableFrom(cls)) {
            // This includes String[] and so on.
            return ARRAY;
        }
        else if (QueryUtils.isGeometryClass(cls))
            return GEOMETRY;
        else if (LocalDate.class == cls)
            return DATE;
        else if (LocalTime.class == cls)
            return TIME;
        else if (LocalDateTime.class == cls)
            return TIMESTAMP;
        else if (OffsetDateTime.class == cls || Instant.class == cls)
            return TIMESTAMP_TZ;
        else
            return JAVA_OBJECT;
    }
}
