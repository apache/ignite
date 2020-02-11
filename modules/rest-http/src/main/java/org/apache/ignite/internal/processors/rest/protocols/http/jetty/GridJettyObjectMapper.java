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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
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
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.cache.VisorCacheKeyObject;
import org.apache.ignite.internal.visor.cache.VisorDataType;
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
        module.addSerializer(GridCacheSqlMetadata.class, IGNITE_SQL_METADATA_SERIALIZER);
        module.addSerializer(GridCacheSqlIndexMetadata.class, IGNITE_SQL_INDEX_METADATA_SERIALIZER);
        module.addSerializer(BinaryObjectImpl.class, IGNITE_BINARY_OBJECT_SERIALIZER);
        module.addDeserializer(VisorCacheKeyObject.class, VISOR_CACHE_KEY_OBJECT_DESERIALIZER);

        configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

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

    /** Custom deserializer for {@link VisorCacheKeyObject} */
    private static final JsonDeserializer<VisorCacheKeyObject> VISOR_CACHE_KEY_OBJECT_DESERIALIZER = new JsonDeserializer<VisorCacheKeyObject>() {
        /**
         * Convert string value to specified object type.
         *
         * @param type Object type.
         * @param o String presentation of object.
         * @return
         */
        private Object strToObject(VisorDataType type, String o) {
            switch (type) {
                case STRING:
                    return o;

                case CHARACTER:
                    return o.charAt(0);

                case BYTE:
                    return Byte.valueOf(o);

                case SHORT:
                    return Short.valueOf(o);

                case INT:
                    return Integer.valueOf(o);

                case LONG:
                    return Long.valueOf(o);

                case FLOAT:
                    return Float.valueOf(o);

                case DOUBLE:
                    return Double.valueOf(o);

                case BOOLEAN:
                    return Boolean.valueOf(o);

                case UUID:
                    return UUID.fromString(o);

                case TIMESTAMP:
                    return new Timestamp(Long.parseLong(o));

                case DATE_UTIL:
                    return new java.util.Date(Long.parseLong(o));

                case DATE_SQL:
                    return new Date(Long.parseLong(o));

                case INSTANT:
                    String[] parts = o.split("\\.");

                    return Instant.ofEpochSecond(Long.parseLong(parts[0]), Integer.parseInt(parts[1]));

                case BIG_INTEGER:
                    return new BigInteger(o);

                case BIG_DECIMAL:
                    return new BigDecimal(o);

                default:
                    throw new IllegalArgumentException("Unsupported type convertation: " + type);
            }
        }

        /**
         * Parse field of binary object from JSON string
         *
         * @param p Parsed used for reading JSON content
         * @param ctxt Context that can be used to access information about this deserialization activity.
         * @return Tuple with pair of name and value of binary field.
         */
        private IgniteBiTuple<String, Object> parseField(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonToken jsonToken = p.nextToken();

            if (JsonToken.END_ARRAY == jsonToken)
                return null;

            if (JsonToken.START_OBJECT == jsonToken) {
                String name = null;
                VisorDataType type = null;
                Object val = null;

                while(!p.isClosed() && jsonToken != JsonToken.END_OBJECT) {
                    jsonToken = p.nextToken();

                    if (JsonToken.FIELD_NAME == jsonToken) {
                        String fldName = p.getCurrentName();

                        jsonToken = p.nextToken();

                        switch (fldName) {
                            case "type":
                                type = VisorDataType.valueOf(p.getValueAsString());

                                break;

                            case "name":
                                name = p.getValueAsString();

                                break;

                            case "value":
                                val = JsonToken.START_OBJECT == jsonToken
                                    ? parseObject(p, ctxt)
                                    : p.getValueAsString();

                                break;
                        }
                    }
                    else if (JsonToken.END_OBJECT != jsonToken)
                        return (IgniteBiTuple<String, Object>) ctxt.handleUnexpectedToken(IgniteBiTuple.class, p);
                }

                return new IgniteBiTuple<String, Object>(
                    name,
                    type == VisorDataType.BINARY ? val : strToObject(type, String.valueOf(val))
                );
            }

            return (IgniteBiTuple<String, Object>) ctxt.handleUnexpectedToken(IgniteBiTuple.class, p);
        }

        /**
         * Parse binary object from JSON string.
         *
         * @param p Parsed used for reading JSON content
         * @param ctxt Context that can be used to access information about this deserialization activity.
         * @return Parsed binary object.
         * @throws IOException
         */
        private BinaryObject parseObject(JsonParser p, DeserializationContext ctxt) throws IOException {
            Map<String, Object> flds = new LinkedHashMap<>();
            String clsName = null;

            JsonToken jsonToken = null;

            while(!p.isClosed() && JsonToken.END_OBJECT != jsonToken) {
                jsonToken = p.nextToken();

                if (JsonToken.FIELD_NAME == jsonToken) {
                    String fieldName = p.getCurrentName();

                    jsonToken = p.nextToken();

                    switch (fieldName) {
                        case "className":
                            clsName = p.getValueAsString();
                            break;

                        case "fields":
                            if (JsonToken.START_ARRAY == jsonToken) {
                                IgniteBiTuple<String, Object> fld = null;

                                do {
                                    fld = parseField(p, ctxt);

                                    if (fld != null)
                                        flds.put(fld.get1(), fld.get2());
                                }
                                while (fld != null);
                            }

                            break;
                    }
                }
            }

            if (clsName != null) {
                BinaryObjectBuilder b = Ignition.localIgnite().binary().builder(clsName);

                for (Map.Entry<String, Object> e: flds.entrySet())
                    b.setField(e.getKey(), e.getValue());

                return b.build();
            }

            return (BinaryObject) ctxt.handleUnexpectedToken(BinaryObject.class, p);
        }

        /** {@inheritDoc} */
        @Override public VisorCacheKeyObject deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            JsonToken jsonToken = null;

            VisorDataType type = null;
            Object value = null;

            while(!p.isClosed() && JsonToken.END_OBJECT != jsonToken) {
                jsonToken = p.nextToken();

                if (JsonToken.FIELD_NAME == jsonToken) {
                    String fieldName = p.getCurrentName();

                    jsonToken = p.nextToken();

                    switch (fieldName) {
                        case "type":
                            type = VisorDataType.valueOf(p.getValueAsString());

                            break;

                        case "value":
                            if (JsonToken.START_OBJECT == jsonToken)
                                value = parseObject(p, ctxt);
                            else
                                value = p.getValueAsString();

                            break;
                    }
                }
            }

            return new VisorCacheKeyObject(
                type,
                VisorDataType.BINARY == type ? value : strToObject(type, String.valueOf(value))
            );
        }
    };

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
}
