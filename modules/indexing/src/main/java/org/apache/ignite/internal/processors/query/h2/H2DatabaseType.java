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

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.DataType;

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
    private static final Map<Class<?>, H2DatabaseType> MAP = new HashMap<>();

    /*
     * Initialize map of DB types.
     */
    static {
        MAP.put(int.class, INT);
        MAP.put(Integer.class, INT);
        MAP.put(boolean.class, BOOL);
        MAP.put(Boolean.class, BOOL);
        MAP.put(byte.class, TINYINT);
        MAP.put(Byte.class, TINYINT);
        MAP.put(short.class, SMALLINT);
        MAP.put(Short.class, SMALLINT);
        MAP.put(long.class, BIGINT);
        MAP.put(Long.class, BIGINT);
        MAP.put(BigDecimal.class, DECIMAL);
        MAP.put(double.class, DOUBLE);
        MAP.put(Double.class, DOUBLE);
        MAP.put(float.class, REAL);
        MAP.put(Float.class, REAL);
        MAP.put(Time.class, TIME);
        MAP.put(Timestamp.class, TIMESTAMP);
        MAP.put(java.util.Date.class, TIMESTAMP);
        MAP.put(java.sql.Date.class, DATE);
        MAP.put(String.class, VARCHAR);
        MAP.put(java.util.UUID.class, UUID);
        MAP.put(byte[].class, BINARY);
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
        H2DatabaseType res = MAP.get(cls);

        if (res != null)
            return res;

        if (DataType.isGeometryClass(cls))
            return GEOMETRY;

        if (LocalDateTimeUtils.LOCAL_DATE == cls)
            return DATE;
        else if (LocalDateTimeUtils.LOCAL_TIME == cls)
            return TIME;
        else if (LocalDateTimeUtils.LOCAL_DATE_TIME == cls)
            return TIMESTAMP;

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
