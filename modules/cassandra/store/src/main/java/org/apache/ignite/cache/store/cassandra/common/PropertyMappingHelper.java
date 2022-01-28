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

package org.apache.ignite.cache.store.cassandra.common;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.ignite.cache.store.cassandra.persistence.PojoFieldAccessor;
import org.apache.ignite.cache.store.cassandra.serializer.Serializer;

/**
 * Helper class providing bunch of methods to discover fields of POJO objects and
 * map builtin Java types to appropriate Cassandra types.
 */
public class PropertyMappingHelper {
    /** Bytes array Class type. */
    private static final Class BYTES_ARRAY_CLASS = (new byte[] {}).getClass();

    /** Mapping from Java to Cassandra types. */
    private static final Map<Class, DataType.Name> JAVA_TO_CASSANDRA_MAPPING = new HashMap<Class, DataType.Name>() {{
        put(String.class, DataType.Name.TEXT);
        put(Integer.class, DataType.Name.INT);
        put(int.class, DataType.Name.INT);
        put(Short.class, DataType.Name.SMALLINT);
        put(short.class, DataType.Name.SMALLINT);
        put(Long.class, DataType.Name.BIGINT);
        put(long.class, DataType.Name.BIGINT);
        put(Double.class, DataType.Name.DOUBLE);
        put(double.class, DataType.Name.DOUBLE);
        put(Boolean.class, DataType.Name.BOOLEAN);
        put(boolean.class, DataType.Name.BOOLEAN);
        put(Float.class, DataType.Name.FLOAT);
        put(float.class, DataType.Name.FLOAT);
        put(ByteBuffer.class, DataType.Name.BLOB);
        put(BYTES_ARRAY_CLASS, DataType.Name.BLOB);
        put(BigDecimal.class, DataType.Name.DECIMAL);
        put(InetAddress.class, DataType.Name.INET);
        put(Date.class, DataType.Name.TIMESTAMP);
        put(UUID.class, DataType.Name.UUID);
        put(BigInteger.class, DataType.Name.VARINT);
    }};

    /**
     * Maps Cassandra type to specified Java type.
     *
     * @param clazz java class.
     *
     * @return Cassandra type.
     */
    public static DataType.Name getCassandraType(Class clazz) {
        return JAVA_TO_CASSANDRA_MAPPING.get(clazz);
    }

    /**
     * Returns property accessor by class property name.
     *
     * @param clazz class from which to get property accessor.
     * @param prop name of the property.
     *
     * @return property accessor.
     */
    public static PojoFieldAccessor getPojoFieldAccessor(Class clazz, String prop) {
        PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(clazz);

        if (descriptors != null) {
            for (PropertyDescriptor descriptor : descriptors) {
                if (descriptor.getName().equals(prop)) {
                    Field field = null;

                    try {
                        field = clazz.getDeclaredField(prop);
                    }
                    catch (Throwable ignore) {
                    }

                    return new PojoFieldAccessor(descriptor, field);
                }
            }
        }

        try {
            return new PojoFieldAccessor(clazz.getDeclaredField(prop));
        }
        catch (Throwable e) {
            throw new IllegalArgumentException("POJO class " + clazz.getName() + " doesn't have '" + prop + "' property");
        }
    }

    /**
     * Returns value of specific column in the row returned by CQL statement.
     *
     * @param row row returned by CQL statement.
     * @param col column name.
     * @param clazz java class to which column value should be casted.
     * @param serializer serializer to use if column stores BLOB otherwise could be null.
     *
     * @return row column value.
     */
    public static Object getCassandraColumnValue(Row row, String col, Class clazz, Serializer serializer) {
        if (String.class.equals(clazz))
            return row.getString(col);

        if (Integer.class.equals(clazz))
            return row.isNull(col) ? null : row.getInt(col);

        if (int.class.equals(clazz)) {
            if (row.isNull(col)) {
                throw new IllegalArgumentException("Can't cast null value from Cassandra table column '" + col +
                        "' to " + "int value used in domain object model");
            }

            return row.getInt(col);
        }

        if (Short.class.equals(clazz))
            return row.isNull(col) ? null : row.getShort(col);

        if (short.class.equals(clazz)) {
            if (row.isNull(col)) {
                throw new IllegalArgumentException("Can't cast null value from Cassandra table column '" + col +
                        "' to " + "short value used in domain object model");
            }

            return row.getShort(col);
        }

        if (Long.class.equals(clazz))
            return row.isNull(col) ? null : row.getLong(col);

        if (long.class.equals(clazz)) {
            if (row.isNull(col)) {
                throw new IllegalArgumentException("Can't cast null value from Cassandra table column '" + col +
                        "' to " + "long value used in domain object model");
            }

            return row.getLong(col);
        }

        if (Double.class.equals(clazz))
            return row.isNull(col) ? null : row.getDouble(col);

        if (double.class.equals(clazz)) {
            if (row.isNull(col)) {
                throw new IllegalArgumentException("Can't cast null value from Cassandra table column '" + col +
                        "' to " + "double value used in domain object model");
            }

            return row.getDouble(col);
        }

        if (Boolean.class.equals(clazz))
            return row.isNull(col) ? null : row.getBool(col);

        if (boolean.class.equals(clazz)) {
            if (row.isNull(col)) {
                throw new IllegalArgumentException("Can't cast null value from Cassandra table column '" + col +
                        "' to " + "boolean value used in domain object model");
            }

            return row.getBool(col);
        }

        if (Float.class.equals(clazz))
            return row.isNull(col) ? null : row.getFloat(col);

        if (float.class.equals(clazz)) {
            if (row.isNull(col)) {
                throw new IllegalArgumentException("Can't cast null value from Cassandra table column '" + col +
                        "' to " + "float value used in domain object model");
            }

            return row.getFloat(col);
        }

        if (ByteBuffer.class.equals(clazz))
            return row.getBytes(col);

        if (PropertyMappingHelper.BYTES_ARRAY_CLASS.equals(clazz)) {
            ByteBuffer buf = row.getBytes(col);
            return buf == null ? null : buf.array();
        }

        if (BigDecimal.class.equals(clazz))
            return row.getDecimal(col);

        if (InetAddress.class.equals(clazz))
            return row.getInet(col);

        if (Date.class.equals(clazz))
            return row.getTimestamp(col);

        if (UUID.class.equals(clazz))
            return row.getUUID(col);

        if (BigInteger.class.equals(clazz))
            return row.getVarint(col);

        if (serializer == null) {
            throw new IllegalStateException("Can't deserialize value from '" + col + "' Cassandra column, " +
                "cause there is no BLOB serializer specified");
        }

        ByteBuffer buf = row.getBytes(col);

        return buf == null ? null : serializer.deserialize(buf);
    }
}
