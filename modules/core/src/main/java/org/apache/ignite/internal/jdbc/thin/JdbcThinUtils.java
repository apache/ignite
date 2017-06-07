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

import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.configuration.SqlConnectorConfiguration;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

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
 * Utility methods for thin JDBC driver.
 */
public class JdbcThinUtils {
    /** URL prefix. */
    public static final String URL_PREFIX = "jdbc:ignite:thin://";

    /** Prefix for property names. */
    public static final String PROP_PREFIX = "ignite.jdbc";

    /** Port number property name. */
    public static final String PROP_PORT = PROP_PREFIX + "port";

    /** Hostname property name. */
    public static final String PROP_HOST = PROP_PREFIX + "host";

    /** Parameter: distributed joins flag. */
    public static final String PARAM_DISTRIBUTED_JOINS = "distributedJoins";

    /** Parameter: enforce join order flag. */
    public static final String PARAM_ENFORCE_JOIN_ORDER = "enforceJoinOrder";

    /** Parameter: collocated flag. */
    public static final String PARAM_COLLOCATED = "collocated";

    /** Parameter: replicated only flag. */
    public static final String PARAM_REPLICATED_ONLY = "replicatedOnly";

    /** Parameter: socket send buffer. */
    public static final String PARAM_SOCK_SND_BUF = "socketSendBuffer";

    /** Parameter: socket receive buffer. */
    public static final String PARAM_SOCK_RCV_BUF = "socketReceiveBuffer";

    /** Parameter: TCP no-delay flag. */
    public static final String PARAM_TCP_NO_DELAY = "tcpNoDelay";

    /** Distributed joins property name. */
    public static final String PROP_DISTRIBUTED_JOINS = PROP_PREFIX + PARAM_DISTRIBUTED_JOINS;

    /** Transactions allowed property name. */
    public static final String PROP_ENFORCE_JOIN_ORDER = PROP_PREFIX + PARAM_ENFORCE_JOIN_ORDER;

    /** Collocated property name. */
    public static final String PROP_COLLOCATED = PROP_PREFIX + PARAM_COLLOCATED;

    /** Replicated only property name. */
    public static final String PROP_REPLICATED_ONLY = PROP_PREFIX + PARAM_REPLICATED_ONLY;

    /** Socket send buffer property name. */
    public static final String PROP_SOCK_SND_BUF = PROP_PREFIX + PARAM_SOCK_SND_BUF;

    /** Socket receive buffer property name. */
    public static final String PROP_SOCK_RCV_BUF = PROP_PREFIX + PARAM_SOCK_RCV_BUF;

    /** TCP no delay property name. */
    public static final String PROP_TCP_NO_DELAY = PROP_PREFIX + PARAM_TCP_NO_DELAY;

    /** Default port. */
    public static final int DFLT_PORT = SqlConnectorConfiguration.DFLT_PORT;

    /**
     * Trim prefix from property.
     *
     * @param prop Property.
     * @return Parameter name.
     */
    public static String trimPrefix(String prop) {
        return prop.substring(PROP_PREFIX.length());
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
}