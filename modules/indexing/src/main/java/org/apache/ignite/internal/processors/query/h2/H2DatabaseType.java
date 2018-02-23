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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.DataType;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Enum that helps to map java types to database types.
 */
public enum H2DatabaseType {
    /** */
    INT("INT"),

    /** */
    BOOL("BOOL"),

    /** */
    TINYINT("TINYINT"),

    /** */
    SMALLINT("SMALLINT"),

    /** */
    BIGINT("BIGINT"),

    /** */
    DECIMAL("DECIMAL"),

    /** */
    DOUBLE("DOUBLE"),

    /** */
    REAL("REAL"),

    /** */
    TIME("TIME"),

    /** */
    TIMESTAMP("TIMESTAMP"),

    /** */
    DATE("DATE"),

    /** */
    VARCHAR("VARCHAR"),

    /** */
    CHAR("CHAR"),

    /** */
    BINARY("BINARY"),

    /** */
    UUID("UUID"),

    /** */
    ARRAY("ARRAY"),

    /** */
    GEOMETRY("GEOMETRY"),

    /** */
    OTHER("OTHER");

    /** Map of Class to enum. */
    private static final Map<Class<?>, H2DatabaseType> map = new HashMap<>();

    /*
     * Initialize map of DB types.
     */
    static {
        map.put(int.class, INT);
        map.put(Integer.class, INT);
        map.put(boolean.class, BOOL);
        map.put(Boolean.class, BOOL);
        map.put(byte.class, TINYINT);
        map.put(Byte.class, TINYINT);
        map.put(short.class, SMALLINT);
        map.put(Short.class, SMALLINT);
        map.put(long.class, BIGINT);
        map.put(Long.class, BIGINT);
        map.put(BigDecimal.class, DECIMAL);
        map.put(double.class, DOUBLE);
        map.put(Double.class, DOUBLE);
        map.put(float.class, REAL);
        map.put(Float.class, REAL);
        map.put(Time.class, TIME);
        map.put(Timestamp.class, TIMESTAMP);
        map.put(java.util.Date.class, TIMESTAMP);
        map.put(java.sql.Date.class, DATE);
        map.put(String.class, VARCHAR);
        map.put(java.util.UUID.class, UUID);
        map.put(byte[].class, BINARY);
    }

    /** */
    private final String dbType;

    /**
     * Constructs new instance.
     *
     * @param dbType DB type name.
     */
    H2DatabaseType(String dbType) {
        this.dbType = dbType;
    }

    /**
     * Resolves enum by class.
     *
     * @param cls Class.
     * @return Enum value.
     */
    public static H2DatabaseType fromClass(Class<?> cls) {
        H2DatabaseType res = map.get(cls);

        if (res != null)
            return res;

        if (DataType.isGeometryClass(cls))
            return GEOMETRY;

        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            if (LocalDateTimeUtils.isLocalDate(cls))
                return DATE;
            else if (LocalDateTimeUtils.isLocalTime(cls))
                return TIME;
            else if (LocalDateTimeUtils.isLocalDateTime(cls))
                return TIMESTAMP;
        }

        return cls.isArray() && !cls.getComponentType().isPrimitive() ? ARRAY : OTHER;
    }

    /**
     * Gets DB type name.
     *
     * @return DB type name.
     */
    public String dBTypeAsString() {
        return dbType;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2DatabaseType.class, this);
    }
}
