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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.visor.cache.VisorCache;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Custom object mapper for HTTP REST API.
 */
public class GridJettyObjectMapper extends ObjectMapper {
    /**
     * Default constructor.
     */
    public GridJettyObjectMapper() {
        super(null, new CustomSerializerProvider(), null);

        setDateFormat(DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, Locale.US));

        SimpleModule module = new SimpleModule();

        module.addSerializer(Throwable.class, THROWABLE_SERIALIZER);
        module.addSerializer(IgniteBiTuple.class, IGNITE_TUPLE_SERIALIZER);
        module.addSerializer(IgniteUuid.class, IGNITE_UUID_SERIALIZER);
        module.addSerializer(LessNamingBean.class, LESS_NAMING_SERIALIZER);

        registerModule(module);
    }

    /** Custom {@code null} key serializer. */
    private static final JsonSerializer<Object> NULL_KEY_SERIALIZER = new JsonSerializer<Object>() {
        /** {@inheritDoc} */
        @Override public void serialize(Object val, JsonGenerator gen, SerializerProvider ser) throws IOException {
            gen.writeFieldName("");
        }
    };

    /** Custom {@code null} value serializer. */
    private static final JsonSerializer<Object> NULL_VALUE_SERIALIZER = new JsonSerializer<Object>() {
        /** {@inheritDoc} */
        @Override public void serialize(Object val, JsonGenerator gen, SerializerProvider ser) throws IOException {
            gen.writeNull();
        }
    };

    /** Custom {@code null} string serializer. */
    private static final JsonSerializer<Object> NULL_STRING_VALUE_SERIALIZER = new JsonSerializer<Object>() {
        /** {@inheritDoc} */
        @Override public void serialize(Object val, JsonGenerator gen, SerializerProvider ser) throws IOException {
            gen.writeString("");
        }
    };

    /**
     * Custom serializers provider that provide special serializers for {@code null} values.
     */
    private static class CustomSerializerProvider extends DefaultSerializerProvider {
        /**
         * Default constructor.
         */
        CustomSerializerProvider() {
            super();
        }

        /**
         * Full constructor.
         *
         * @param src Blueprint object used as the baseline for this instance.
         * @param cfg Provider configuration.
         * @param f Serializers factory.
         */
        CustomSerializerProvider(SerializerProvider src, SerializationConfig cfg, SerializerFactory f) {
            super(src, cfg, f);
        }

        /** {@inheritDoc} */
        public DefaultSerializerProvider createInstance(SerializationConfig cfg, SerializerFactory jsf) {
            return new CustomSerializerProvider(this, cfg, jsf);
        }

        /** {@inheritDoc} */
        @Override public JsonSerializer<Object> findNullKeySerializer(JavaType serializationType,
            BeanProperty prop) throws JsonMappingException {
            return NULL_KEY_SERIALIZER;
        }

        /** {@inheritDoc} */
        @Override public JsonSerializer<Object> findNullValueSerializer(BeanProperty prop) throws JsonMappingException {
            if (prop.getType().getRawClass() == String.class)
                return NULL_STRING_VALUE_SERIALIZER;

            return NULL_VALUE_SERIALIZER;
        }
    }

    /** Custom serializer for {@link Throwable} */
    private static final JsonSerializer<Throwable> THROWABLE_SERIALIZER = new JsonSerializer<Throwable>() {
        /** {@inheritDoc} */
        @Override public void serialize(Throwable e, JsonGenerator gen, SerializerProvider ser) throws IOException {
            gen.writeStartObject();

            if (e instanceof VisorExceptionWrapper) {
                VisorExceptionWrapper wrapper = (VisorExceptionWrapper)e;

                gen.writeStringField("className", wrapper.getClassName());
            }
            else
                gen.writeStringField("className", e.getClass().getName());

            if (e.getMessage() != null)
                gen.writeStringField("message", e.getMessage());

            if (e.getCause() != null)
                gen.writeObjectField("cause", e.getCause());

            if (e instanceof SQLException) {
                SQLException sqlE = (SQLException)e;

                gen.writeNumberField("errorCode", sqlE.getErrorCode());
                gen.writeStringField("SQLState", sqlE.getSQLState());
            }

            gen.writeEndObject();
        }
    };

    /** Custom serializer for {@link IgniteUuid} */
    private static final JsonSerializer<IgniteUuid> IGNITE_UUID_SERIALIZER = new JsonSerializer<IgniteUuid>() {
        /** {@inheritDoc} */
        @Override public void serialize(IgniteUuid uid, JsonGenerator gen, SerializerProvider ser) throws IOException {
            gen.writeString(uid.toString());
        }
    };

    /** Custom serializer for {@link IgniteBiTuple} */
    private static final JsonSerializer<IgniteBiTuple> IGNITE_TUPLE_SERIALIZER = new JsonSerializer<IgniteBiTuple>() {
        /** {@inheritDoc} */
        @Override public void serialize(IgniteBiTuple t, JsonGenerator gen, SerializerProvider ser) throws IOException {
            gen.writeStartObject();

            gen.writeObjectField("key", t.getKey());
            gen.writeObjectField("value", t.getValue());

            gen.writeEndObject();
        }
    };

    /**
     * Custom serializer for Visor classes with non JavaBeans getters.
     */
    private static final JsonSerializer<Object> LESS_NAMING_SERIALIZER = new JsonSerializer<Object>() {
        /** Methods to exclude. */
        private final Collection<String> exclMtds = Arrays.asList("toString", "hashCode", "clone", "getClass");

        /** */
        private final Map<Class<?>, Collection<Method>> clsCache = new HashMap<>();

        /** */
        private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

        /** {@inheritDoc} */
        @Override public void serialize(Object bean, JsonGenerator gen, SerializerProvider ser) throws IOException {
            if (bean != null) {
                gen.writeStartObject();

                Class<?> cls = bean.getClass();

                Collection<Method> methods;

                // Get descriptor from cache.
                rwLock.readLock().lock();

                try {
                    methods = clsCache.get(cls);
                }
                finally {
                    rwLock.readLock().unlock();
                }

                // If missing in cache - build descriptor
                if (methods == null) {
                    Method[] publicMtds = cls.getMethods();

                    methods = new ArrayList<>(publicMtds.length);

                    for (Method mtd : publicMtds) {
                        Class retType = mtd.getReturnType();

                        String mtdName = mtd.getName();

                        if (mtd.getParameterTypes().length != 0 ||
                            retType == void.class || retType == cls ||
                            exclMtds.contains(mtdName) ||
                            (VisorCache.class.isAssignableFrom(retType) && "history".equals(mtdName)))
                            continue;

                        mtd.setAccessible(true);

                        methods.add(mtd);
                    }

                    // Allow multiple puts for the same class - they will simply override.
                    rwLock.writeLock().lock();

                    try {
                        clsCache.put(cls, methods);
                    }
                    finally {
                        rwLock.writeLock().unlock();
                    }
                }

                // Extract fields values using descriptor and build JSONObject.
                for (Method mtd : methods) {
                    try {
                        Object prop = mtd.invoke(bean);

                        if (prop != null)
                            gen.writeObjectField(mtd.getName(), prop);
                    }
                    catch (IOException ioe) {
                        throw ioe;
                    }
                    catch (Exception e) {
                        throw new IOException(e);
                    }
                }

                gen.writeEndObject();
            }
        }
    };
}
