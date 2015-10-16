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

package org.apache.ignite.cache.store.cassandra.utils.common;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.ignite.cache.store.cassandra.utils.serializer.Serializer;

/**
 * Helper class providing bunch of methods to discover fields of POJO objects and
 * map primitive Java types to appropriate Cassandra types
 */
public class PropertyMappingHelper {
    private static final Class BYTES_ARRAY_CLASS = (new byte[] {}).getClass();

    private static final Map<Class, DataType.Name> JAVA_TO_CASSANDRA_MAPPING = new HashMap<Class, DataType.Name>() {{
        put(String.class, DataType.Name.TEXT);
        put(Integer.class, DataType.Name.INT);
        put(int.class, DataType.Name.INT);
        put(Short.class, DataType.Name.INT);
        put(short.class, DataType.Name.INT);
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

    public static DataType.Name getCassandraType(Class clazz)
    {
        return JAVA_TO_CASSANDRA_MAPPING.get(clazz);
    }

    public static PropertyDescriptor getPojoPropertyDescriptor(Class clazz, String property) {
        List<PropertyDescriptor> descriptors = getPojoPropertyDescriptors(clazz, false);

        if (descriptors == null || descriptors.isEmpty())
            throw new IllegalArgumentException("POJO class doesn't have '" + property + "' property");

        for (PropertyDescriptor descriptor : descriptors) {
            if (descriptor.getName().equals(property))
                return descriptor;
        }

        throw new IllegalArgumentException("POJO class doesn't have '" + property + "' property");
    }

    public static List<PropertyDescriptor> getPojoPropertyDescriptors(Class clazz, boolean primitive) {
        return getPojoPropertyDescriptors(clazz, null, primitive);
    }

    public static <T extends Annotation> List<PropertyDescriptor> getPojoPropertyDescriptors(Class clazz,
        Class<T> annotation, boolean primitive) {
        PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(clazz);

        List<PropertyDescriptor> list = new ArrayList<>(descriptors == null ? 1 : descriptors.length);

        if (descriptors == null || descriptors.length == 0)
            return list;

        for (PropertyDescriptor descriptor : descriptors) {
            if (descriptor.getReadMethod() == null || descriptor.getWriteMethod() == null ||
                (primitive && !isPrimitivePropertyDescriptor(descriptor))) {
                continue;
            }

            if (annotation == null || descriptor.getReadMethod().getAnnotation(annotation) != null)
                list.add(descriptor);
        }

        return list;
    }

    public static boolean isPrimitivePropertyDescriptor(PropertyDescriptor descriptor)
    {
        return PropertyMappingHelper.JAVA_TO_CASSANDRA_MAPPING.containsKey(descriptor.getPropertyType());
    }

    public static Object getCassandraColumnValue(Row row, String column, Class clazz, Serializer serializer) {
        if (String.class.equals(clazz))
            return row.getString(column);
        else if (Integer.class.equals(clazz) || int.class.equals(clazz))
            return row.getInt(column);
        else if (Short.class.equals(clazz) || short.class.equals(clazz))
            return (short)row.getInt(column);
        else if (Long.class.equals(clazz) || long.class.equals(clazz))
            return row.getLong(column);
        else if (Double.class.equals(clazz) || double.class.equals(clazz))
            return row.getDouble(column);
        else if (Boolean.class.equals(clazz) || boolean.class.equals(clazz))
            return row.getBool(column);
        else if (Float.class.equals(clazz) || float.class.equals(clazz))
            return row.getFloat(column);
        else if (ByteBuffer.class.equals(clazz))
            return row.getBytes(column);
        else if (PropertyMappingHelper.BYTES_ARRAY_CLASS.equals(clazz)) {
            ByteBuffer buffer = row.getBytes(column);
            return buffer == null ? null : buffer.array();
        }
        else if (BigDecimal.class.equals(clazz))
            return row.getDecimal(column);
        else if (InetAddress.class.equals(clazz))
            return row.getInet(column);
        else if (Date.class.equals(clazz))
            return row.getDate(column);
        else if (UUID.class.equals(clazz))
            return row.getUUID(column);
        else if (BigInteger.class.equals(clazz))
            return row.getVarint(column);

        if (serializer == null) {
            throw new IllegalStateException("Can't deserialize value from '" + column + "' Cassandra column, " +
                "cause there is no BLOB serializer specified");
        }

        ByteBuffer buffer = row.getBytes(column);

        return buffer == null ? null : serializer.deserialize(buffer);
    }
}
