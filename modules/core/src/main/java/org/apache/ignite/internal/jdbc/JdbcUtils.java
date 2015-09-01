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

package org.apache.ignite.internal.jdbc;

import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;

/**
 * Utility methods for JDBC driver.
 */
class JdbcUtils {
    /** Marshaller. */
    private static final Marshaller MARSHALLER = new JdkMarshaller();

    /**
     * Marshals task argument to byte array.
     *
     * @param args Task argument.
     * @return Byte array.
     * @throws SQLException In marshalling failed.
     */
    public static byte[] marshalArgument(Map<String, Object> args) throws SQLException {
        assert args != null;

        try {
            return MARSHALLER.marshal(args);
        }
        catch (IgniteCheckedException e) {
            throw new SQLException("Failed to unmarshal result.", e);
        }
    }

    /**
     * Unmarshals exception from byte array.
     *
     * @param bytes Byte array.
     * @return Exception.
     * @throws SQLException If unmarshalling failed.
     */
    public static SQLException unmarshalError(byte[] bytes) throws SQLException {
        return unmarshal(bytes);
    }

    /**
     * Unmarshals object from byte array.
     *
     * @param bytes Byte array.
     * @return Object.
     * @throws SQLException If unmarshalling failed.
     */
    public static <T> T unmarshal(byte[] bytes) throws SQLException {
        assert bytes != null;

        try {
            return MARSHALLER.unmarshal(bytes, null);
        }
        catch (IgniteCheckedException e) {
            throw new SQLException("Failed to unmarshal result.", e);
        }
    }

    /**
     * Creates task argument for first execution.
     *
     * @param nodeId Node ID.
     * @param cacheName Cache name.
     * @param sql SQL query.
     * @param timeout Query timeout.
     * @param args Query arguments.
     * @param pageSize Page size.
     * @param maxRows Maximum number of rows.
     * @return Task argument.
     */
    public static Map<String, Object> taskArgument(UUID nodeId, String cacheName, String sql,
        long timeout, Object[] args, int pageSize, int maxRows) {
        assert sql != null;
        assert timeout >= 0;
        assert pageSize > 0;
        assert maxRows >= 0;

        Map<String, Object> map = U.newHashMap(7);

        map.put("confNodeId", nodeId);
        map.put("cache", cacheName);
        map.put("sql", sql);
        map.put("timeout", timeout);
        map.put("args", args != null ? Arrays.asList(args) : Collections.emptyList());
        map.put("pageSize", pageSize);
        map.put("maxRows", maxRows);

        return map;
    }

    /**
     * Creates task argument.
     *
     * @param nodeId Node ID.
     * @param futId Future ID.
     * @param pageSize Page size.
     * @param maxRows Maximum number of rows.
     * @return Task argument.
     */
    public static Map<String, Object> taskArgument(UUID nodeId, UUID futId, int pageSize, int maxRows) {
        assert nodeId != null;
        assert futId != null;
        assert pageSize > 0;
        assert maxRows >= 0;

        Map<String, Object> map = U.newHashMap(4);

        map.put("nodeId", nodeId);
        map.put("futId", futId);
        map.put("pageSize", pageSize);
        map.put("maxRows", maxRows);

        return map;
    }

    /**
     * Converts Java class name to type from {@link Types}.
     *
     * @param cls Java class name.
     * @return Type from {@link Types}.
     */
    @SuppressWarnings("IfMayBeConditional")
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
        else if (Date.class.getName().equals(cls))
            return DATE;
        else
            return OTHER;
    }

    /**
     * Converts Java class name to SQL type name.
     *
     * @param cls Java class name.
     * @return SQL type name.
     */
    @SuppressWarnings("IfMayBeConditional")
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
        else if (Date.class.getName().equals(cls))
            return "DATE";
        else
            return "OTHER";
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
}