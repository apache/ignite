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

package org.apache.ignite.cache.store.jdbc;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;

/**
 * Default implementation of {@link JdbcTypesTransformer}.
 */
public class JdbcTypesDefaultTransformer implements JdbcTypesTransformer {
    /** */
    private static final long serialVersionUID = 0L;

    /** Singleton instance to use. */
    public static final JdbcTypesDefaultTransformer INSTANCE = new JdbcTypesDefaultTransformer();

    /** {@inheritDoc} */
    @Override public Object getColumnValue(ResultSet rs, int colIdx, Class<?> type) throws SQLException {
        Object val = rs.getObject(colIdx);

        if (val == null)
            return null;

        if (type == int.class)
            return rs.getInt(colIdx);

        if (type == long.class)
            return rs.getLong(colIdx);

        if (type == double.class)
            return rs.getDouble(colIdx);

        if (type == boolean.class || type == Boolean.class)
            return rs.getBoolean(colIdx);

        if (type == byte.class)
            return rs.getByte(colIdx);

        if (type == short.class)
            return rs.getShort(colIdx);

        if (type == float.class)
            return rs.getFloat(colIdx);

        if (type == Integer.class || type == Long.class || type == Double.class ||
            type == Byte.class || type == Short.class ||  type == Float.class) {
            Number num = (Number)val;

            if (type == Integer.class)
                return num.intValue();
            else if (type == Long.class)
                return num.longValue();
            else if (type == Double.class)
                return num.doubleValue();
            else if (type == Byte.class)
                return num.byteValue();
            else if (type == Short.class)
                return num.shortValue();
            else if (type == Float.class)
                return num.floatValue();
        }

        if (type == UUID.class) {
            if (val instanceof UUID)
                return val;

            if (val instanceof byte[]) {
                ByteBuffer bb = ByteBuffer.wrap((byte[])val);

                long most = bb.getLong();
                long least = bb.getLong();

                return new UUID(most, least);
            }

            if (val instanceof String)
                return UUID.fromString((String)val);
        }

        // Workaround for known issue with Oracle JDBC driver https://community.oracle.com/thread/2355464?tstart=0
        if (type == java.sql.Date.class && val instanceof java.util.Date)
            return new java.sql.Date(((java.util.Date)val).getTime());

        // Workaround for known issue with Oracle JDBC driver and timestamp.
        // http://stackoverflow.com/questions/13269564/java-lang-classcastexception-oracle-sql-timestamp-cannot-be-cast-to-java-sql-ti
        if (type == Timestamp.class && !(val instanceof Timestamp) &&
            val.getClass().getName().startsWith("oracle.sql.TIMESTAMP")) {
            try {
                return val.getClass().getMethod("timestampValue").invoke(val);
            }
            catch (Exception e) {
                throw new SQLException("Failed to read data of oracle.sql.TIMESTAMP type.", e);
            }
        }

        return val;
    }
}
