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

import com.datastax.driver.core.LocalDate;
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
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.ignite.cache.store.cassandra.handler.*;
import org.apache.ignite.cache.store.cassandra.persistence.PojoFieldAccessor;

/**
 * Helper class providing bunch of methods to discover fields of POJO objects and
 * map builtin Java types to appropriate type handlers.
 */
public class PropertyMappingHelper {
    /** Bytes array Class type. */
    private static final Class BYTES_ARRAY_CLASS = (new byte[] {}).getClass();

    /** Mapping from Java to type handlers. */
    private static final Map<Class, TypeHandler> JAVA_TO_CASSANDRA_MAPPING = new HashMap<Class, TypeHandler>() {{
        put(String.class, TypeHandlerHelper.getInstanceFromClass(StringTypeHandler.class));
        put(Integer.class, TypeHandlerHelper.getInstanceFromClass(IntegerTypeHandler.class));
        put(int.class, TypeHandlerHelper.getInstanceFromClass(IntegerTypeHandler.class));
        put(Short.class, TypeHandlerHelper.getInstanceFromClass(IntegerTypeHandler.class));
        put(short.class, TypeHandlerHelper.getInstanceFromClass(IntegerTypeHandler.class));
        put(Long.class, TypeHandlerHelper.getInstanceFromClass(LongTypeHandler.class));
        put(long.class, TypeHandlerHelper.getInstanceFromClass(LongTypeHandler.class));
        put(Double.class, TypeHandlerHelper.getInstanceFromClass(DoubleTypeHandler.class));
        put(double.class, TypeHandlerHelper.getInstanceFromClass(DoubleTypeHandler.class));
        put(Boolean.class, TypeHandlerHelper.getInstanceFromClass(BooleanTypeHandler.class));
        put(boolean.class, TypeHandlerHelper.getInstanceFromClass(BooleanTypeHandler.class));
        put(Float.class, TypeHandlerHelper.getInstanceFromClass(FloatTypeHandler.class));
        put(float.class, TypeHandlerHelper.getInstanceFromClass(FloatTypeHandler.class));
        put(ByteBuffer.class, TypeHandlerHelper.getInstanceFromClass(ByteBufferTypeHandler.class));
        put(BYTES_ARRAY_CLASS, TypeHandlerHelper.getInstanceFromClass(ByteArrayTypeHandler.class));
        put(BigDecimal.class, TypeHandlerHelper.getInstanceFromClass(BigDecimalTypeHandler.class));
        put(InetAddress.class, TypeHandlerHelper.getInstanceFromClass(InetAddressTypeHandler.class));
        put(Date.class, TypeHandlerHelper.getInstanceFromClass(DateTypeHandler.class));
        put(UUID.class, TypeHandlerHelper.getInstanceFromClass(UUIDTypeHandler.class));
        put(BigInteger.class, TypeHandlerHelper.getInstanceFromClass(BigIntegerTypeHandler.class));
        put(LocalDate.class, TypeHandlerHelper.getInstanceFromClass(LocalDateTypeHandler.class));
    }};

    /**
     * Maps java class to specified type handler.
     *
     * @param clazz java class.
     *
     * @return type handler.
     */
    public static TypeHandler getTypeHandler(Class clazz) {
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
}
