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

package org.apache.ignite.internal.jdbc.thin;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import org.jetbrains.annotations.Nullable;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static org.apache.ignite.internal.jdbc.thin.ConnectionPropertiesImpl.PROP_PREFIX;

/**
 * Utility methods for thin JDBC driver.
 */
public class JdbcThinUtils {
    /** URL prefix. */
    public static final String URL_PREFIX = "jdbc:ignite:thin://";

    /** Port number property name. */
    public static final String PROP_PORT = PROP_PREFIX + "port";

    /** Hostname property name. */
    public static final String PROP_HOST = PROP_PREFIX + "host";

    /** Byte representation of value "default" */
    private static final byte BYTE_DEFAULT = 2;

    /** Byte representation of value "disabled". */
    private static final byte BYTE_ENABLED = 1;

    /** Byte representation of value "disabled". */
    private static final byte BYTE_DISABLED = 0;

    /**
     * Converts Java class name to type from {@link Types}.
     *
     * @param cls Java class name.
     * @return Type from {@link Types}.
     */
    public static int type(String cls) {
        if (Boolean.class.getName().equals(cls) || boolean.class.getName().equals(cls))
            return BOOLEAN;
        else if (Byte.class.getName().equals(cls) || byte.class.getName().equals(cls))
            return TINYINT;
        else if (Short.class.getName().equals(cls) || short.class.getName().equals(cls))
            return SMALLINT;
        else if (Integer.class.getName().equals(cls) || int.class.getName().equals(cls))
            return INTEGER;
        else if (Long.class.getName().equals(cls) || long.class.getName().equals(cls))
            return BIGINT;
        else if (Float.class.getName().equals(cls) || float.class.getName().equals(cls))
            return FLOAT;
        else if (Double.class.getName().equals(cls) || double.class.getName().equals(cls))
            return DOUBLE;
        else if (String.class.getName().equals(cls))
            return VARCHAR;
        else if (byte[].class.getName().equals(cls))
            return BINARY;
        else if (Time.class.getName().equals(cls))
            return TIME;
        else if (Timestamp.class.getName().equals(cls))
            return TIMESTAMP;
        else if (Date.class.getName().equals(cls) || java.sql.Date.class.getName().equals(cls))
            return DATE;
        else if (BigDecimal.class.getName().equals(cls))
            return DECIMAL;
        else
            return OTHER;
    }

    /**
     * Converts Java class name to SQL type name.
     *
     * @param cls Java class name.
     * @return SQL type name.
     */
    public static String typeName(String cls) {
        if (Boolean.class.getName().equals(cls) || boolean.class.getName().equals(cls))
            return "BOOLEAN";
        else if (Byte.class.getName().equals(cls) || byte.class.getName().equals(cls))
            return "TINYINT";
        else if (Short.class.getName().equals(cls) || short.class.getName().equals(cls))
            return "SMALLINT";
        else if (Integer.class.getName().equals(cls) || int.class.getName().equals(cls))
            return "INTEGER";
        else if (Long.class.getName().equals(cls) || long.class.getName().equals(cls))
            return "BIGINT";
        else if (Float.class.getName().equals(cls) || float.class.getName().equals(cls))
            return "FLOAT";
        else if (Double.class.getName().equals(cls) || double.class.getName().equals(cls))
            return "DOUBLE";
        else if (String.class.getName().equals(cls))
            return "VARCHAR";
        else if (byte[].class.getName().equals(cls))
            return "BINARY";
        else if (Time.class.getName().equals(cls))
            return "TIME";
        else if (Timestamp.class.getName().equals(cls))
            return "TIMESTAMP";
        else if (Date.class.getName().equals(cls) || java.sql.Date.class.getName().equals(cls))
            return "DATE";
        else if (BigDecimal.class.getName().equals(cls))
            return "DECIMAL";
        else
            return "OTHER";
    }

    /**
     * @param type a value from <code>java.sql.Types</code>.
     * @return {@code true} if type is plain and supported by thin JDBC driver.
     */
    public static boolean isPlainJdbcType(int type) {
        return type != Types.ARRAY
            && type != Types.BLOB
            && type != Types.CLOB
            && type != Types.DATALINK
            && type != Types.JAVA_OBJECT
            && type != Types.NCHAR
            && type != Types.NVARCHAR
            && type != Types.LONGNVARCHAR
            && type != Types.REF
            && type != Types.ROWID
            && type != Types.SQLXML;
    }

    /**
     * Determines whether type is nullable.
     *
     * @param name Column name.
     * @param cls Java class name.
     * @return {@code True} if nullable.
     */
    public static boolean nullable(String name, String cls) {
        return !"_KEY".equalsIgnoreCase(name) &&
            !"_VAL".equalsIgnoreCase(name) &&
            !(boolean.class.getName().equals(cls) ||
            byte.class.getName().equals(cls) ||
            short.class.getName().equals(cls) ||
            int.class.getName().equals(cls) ||
            long.class.getName().equals(cls) ||
            float.class.getName().equals(cls) ||
            double.class.getName().equals(cls));
    }

    /**
     * Converts raw byte value to the nullable Boolean. Useful for the deserialization in the handshake.
     *
     * @param raw byte value to convert to Boolean.
     * @return converted value.
     */
    @Nullable public static Boolean nullableBooleanFromByte(byte raw) {
        switch (raw) {
            case BYTE_DEFAULT:
                return null;
            case BYTE_ENABLED:
                return Boolean.TRUE;
            case BYTE_DISABLED:
                return Boolean.FALSE;
            default:
                throw new NumberFormatException("Incorrect byte: " + raw + ". Impossible to read nullable Boolean from it.");
        }
    }

    /**
     * Converts nullable Boolean to the raw byte. Useful for the serialization in the handshake.
     *
     * @param val value to convert.
     * @return byte representation.
     */
    public static byte nullableBooleanToByte(@Nullable Boolean val) {
        if (val == null)
            return BYTE_DEFAULT;

        return val ? BYTE_ENABLED : BYTE_DISABLED;
    }
}
