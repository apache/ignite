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

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.Locale;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Custom object mapper for HTTP REST API.
 */
public class GridJettyObjectMapper extends ObjectMapper {
    /**
     * Default constructor.
     */
    public GridJettyObjectMapper() {
        this(null);
    }

    /**
     * @param ctx Defines a kernal context to enable deserialization into the Ignite binary object.
     */
    GridJettyObjectMapper(GridKernalContext ctx) {
        super(null, new CustomSerializerProvider(), null);

        setDateFormat(DateFormat.getDateTimeInstance(DateFormat.DEFAULT, DateFormat.DEFAULT, Locale.US));

        SimpleModule module = new SimpleModule();

        module.addSerializer(Throwable.class, THROWABLE_SERIALIZER);
        module.addSerializer(IgniteBiTuple.class, IGNITE_TUPLE_SERIALIZER);
        module.addSerializer(IgniteUuid.class, IGNITE_UUID_SERIALIZER);
        module.addSerializer(GridCacheSqlMetadata.class, IGNITE_SQL_METADATA_SERIALIZER);
        module.addSerializer(GridCacheSqlIndexMetadata.class, IGNITE_SQL_INDEX_METADATA_SERIALIZER);
        module.addSerializer(BinaryObjectImpl.class, IGNITE_BINARY_OBJECT_SERIALIZER);

        // Standard serializer loses nanoseconds.
        module.addSerializer(Timestamp.class, IGNITE_TIMESTAMP_SERIALIZER);
        module.addDeserializer(Timestamp.class, IGNITE_TIMESTAMP_DESERIALIZER);

        // Standard serializer may incorrectly apply timezone.
        module.addSerializer(Date.class, IGNITE_SQLDATE_SERIALIZER);
        module.addDeserializer(Date.class, IGNITE_SQLDATE_DESERIALIZER);

        if (ctx != null) {
            module.addDeserializer(BinaryObject.class, new IgniteBinaryObjectJsonDeserializer(ctx));

            IgnitePredicate<String> clsFilter = ctx.marshallerContext().classNameFilter();

            setDefaultTyping(new RestrictedTypeResolverBuilder(clsFilter).init(JsonTypeInfo.Id.CLASS, null));
        }

        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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

    /**
     * Custom serializers provider that provide special serializers for {@code null} values.
     */
    private static class CustomSerializerProvider extends DefaultSerializerProvider {
        /**
         * Default constructor.
         */
        CustomSerializerProvider() {
            // No-op.
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
        @Override public DefaultSerializerProvider createInstance(SerializationConfig cfg, SerializerFactory jsf) {
            return new CustomSerializerProvider(this, cfg, jsf);
        }

        /** {@inheritDoc} */
        @Override public JsonSerializer<Object> findNullKeySerializer(JavaType serializationType, BeanProperty prop) {
            return NULL_KEY_SERIALIZER;
        }

        /** {@inheritDoc} */
        @Override public JsonSerializer<Object> findNullValueSerializer(BeanProperty prop) {
            return NULL_VALUE_SERIALIZER;
        }
    }

    /** Custom serializer for {@link Throwable} */
    private static final JsonSerializer<Throwable> THROWABLE_SERIALIZER = new JsonSerializer<Throwable>() {
        /**
         * @param e Exception to write.
         * @param gen JSON generator.
         * @throws IOException If failed to write.
         */
        private void writeException(Throwable e, JsonGenerator gen) throws IOException {
            if (e instanceof VisorExceptionWrapper) {
                VisorExceptionWrapper wrapper = (VisorExceptionWrapper)e;

                gen.writeStringField("className", wrapper.getClassName());
            }
            else
                gen.writeStringField("className", e.getClass().getName());

            if (e.getMessage() != null)
                gen.writeStringField("message", e.getMessage());

            if (e instanceof SQLException) {
                SQLException sqlE = (SQLException)e;

                gen.writeNumberField("errorCode", sqlE.getErrorCode());
                gen.writeStringField("SQLState", sqlE.getSQLState());
            }
        }

        /** {@inheritDoc} */
        @Override public void serialize(Throwable e, JsonGenerator gen, SerializerProvider ser) throws IOException {
            gen.writeStartObject();

            writeException(e, gen);

            if (e.getCause() != null)
                gen.writeObjectField("cause", e.getCause());

            if (!F.isEmpty(e.getSuppressed())) {
                gen.writeArrayFieldStart("suppressed");

                for (Throwable sup : e.getSuppressed())
                    gen.writeObject(sup);

                gen.writeEndArray();
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

    /** Custom serializer for {@link GridCacheSqlMetadata} */
    private static final JsonSerializer<GridCacheSqlMetadata> IGNITE_SQL_METADATA_SERIALIZER = new JsonSerializer<GridCacheSqlMetadata>() {
        /** {@inheritDoc} */
        @Override public void serialize(GridCacheSqlMetadata m, JsonGenerator gen, SerializerProvider ser) throws IOException {
            gen.writeStartObject();

            gen.writeStringField("cacheName", m.cacheName());
            gen.writeObjectField("types", m.types());
            gen.writeObjectField("keyClasses", m.keyClasses());
            gen.writeObjectField("valClasses", m.valClasses());
            gen.writeObjectField("fields", m.fields());
            gen.writeObjectField("indexes", m.indexes());

            gen.writeEndObject();
        }
    };

    /** Custom serializer for {@link GridCacheSqlIndexMetadata} */
    private static final JsonSerializer<GridCacheSqlIndexMetadata> IGNITE_SQL_INDEX_METADATA_SERIALIZER = new JsonSerializer<GridCacheSqlIndexMetadata>() {
        /** {@inheritDoc} */
        @Override public void serialize(GridCacheSqlIndexMetadata idx, JsonGenerator gen, SerializerProvider ser) throws IOException {
            gen.writeStartObject();

            gen.writeStringField("name", idx.name());
            gen.writeObjectField("fields", idx.fields());
            gen.writeObjectField("descendings", idx.descendings());
            gen.writeBooleanField("unique", idx.unique());

            gen.writeEndObject();
        }
    };

    /** Custom serializer for {@link GridCacheSqlIndexMetadata} */
    private static final JsonSerializer<BinaryObjectImpl> IGNITE_BINARY_OBJECT_SERIALIZER = new JsonSerializer<BinaryObjectImpl>() {
        /** {@inheritDoc} */
        @Override public void serialize(BinaryObjectImpl bin, JsonGenerator gen, SerializerProvider ser) throws IOException {
            try {
                BinaryType meta = bin.rawType();

                // Serialize to JSON if we have metadata.
                if (meta != null && !F.isEmpty(meta.fieldNames())) {
                    gen.writeStartObject();

                    for (String name : meta.fieldNames()) {
                        Object val = bin.field(name);

                        if (val instanceof BinaryObjectImpl) {
                            BinaryObjectImpl ref = (BinaryObjectImpl)val;

                            if (ref.hasCircularReferences())
                                throw ser.mappingException("Failed convert to JSON object for circular references");
                        }

                        if (val instanceof BinaryEnumObjectImpl)
                            gen.writeObjectField(name, ((BinaryObject)val).enumName());
                        else
                            gen.writeObjectField(name, val);
                    }

                    gen.writeEndObject();
                }
                else {
                    // Otherwise serialize as Java object.
                    Object obj = bin.deserialize();

                    gen.writeObject(obj);
                }
            }
            catch (BinaryObjectException ignore) {
                gen.writeNull();
            }
        }
    };

    /** Custom serializer for {@link java.sql.Timestamp}. */
    private static final JsonSerializer<Timestamp> IGNITE_TIMESTAMP_SERIALIZER = new JsonSerializer<Timestamp>() {
        /** {@inheritDoc} */
        @Override public void serialize(Timestamp timestamp, JsonGenerator gen,
            SerializerProvider provider) throws IOException {
            gen.writeString(timestamp.toString());
        }
    };

    /** Custom serializer for {@link java.sql.Date}. */
    private static final JsonSerializer<Date> IGNITE_SQLDATE_SERIALIZER = new JsonSerializer<Date>() {
        /** {@inheritDoc} */
        @Override public void serialize(Date date, JsonGenerator gen, SerializerProvider prov) throws IOException {
            gen.writeString(date.toString());
        }
    };

    /** Custom deserializer for {@link java.sql.Timestamp}. */
    private static final JsonDeserializer<Timestamp> IGNITE_TIMESTAMP_DESERIALIZER = new JsonDeserializer<Timestamp>() {
        /** {@inheritDoc} */
        @Override public Timestamp deserialize(JsonParser parser,
            DeserializationContext ctx) throws IOException, JsonProcessingException {
            return Timestamp.valueOf(parser.getText());
        }
    };

    /** Custom deserializer for {@link java.sql.Date}. */
    private static final JsonDeserializer<Date> IGNITE_SQLDATE_DESERIALIZER = new JsonDeserializer<Date>() {
        /** {@inheritDoc} */
        @Override public Date deserialize(JsonParser parser,
            DeserializationContext ctx) throws IOException, JsonProcessingException {
            return Date.valueOf(parser.getText());
        }
    };

    /** */
    private static class RestrictedTypeResolverBuilder extends DefaultTypeResolverBuilder {
        /** */
        private final IgnitePredicate<String> clsFilter;

        /**
         * @param clsFilter Class name filter.
         */
        public RestrictedTypeResolverBuilder(IgnitePredicate<String> clsFilter) {
            super(DefaultTyping.NON_FINAL);

            this.clsFilter = clsFilter;
        }

        /** {@inheritDoc} */
        @Override public boolean useForType(JavaType type) {
            String clsName = type.getRawClass().getName();

            if (!clsFilter.apply(clsName))
                throw new IgniteException("Deserialization of class " + clsName + " is disallowed.");

            return false;
        }
    }
}
