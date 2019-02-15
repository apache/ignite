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
            if (LocalDateTimeUtils.LOCAL_DATE == cls)
                return DATE;
            else if (LocalDateTimeUtils.LOCAL_TIME == cls)
                return TIME;
            else if (LocalDateTimeUtils.LOCAL_DATE_TIME == cls)
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
