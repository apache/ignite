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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.jetbrains.annotations.Nullable;

/**
 * JSON deserializer into the Ignite binary object.
 */
public class IgniteBinaryObjectJsonDeserializer extends JsonDeserializer<BinaryObjectImpl> {
    /** Property name to set binary type name. */
    public static final String BINARY_TYPE_PROPERTY = "binaryTypeName";

    /** Property name to set cache name. */
    public static final String CACHE_NAME_PROPERTY = "cacheName";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Kernal context.
     */
    public IgniteBinaryObjectJsonDeserializer(GridKernalContext ctx) {
        assert ctx != null;

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectImpl deserialize(JsonParser parser, DeserializationContext dCtx) throws IOException {
        String type = (String)dCtx.findInjectableValue(BINARY_TYPE_PROPERTY, null, null);
        String cacheName = (String)dCtx.findInjectableValue(CACHE_NAME_PROPERTY, null, null);

        assert type != null;

        if (ctx.marshallerContext().isSystemType(type))
            throw new IllegalArgumentException("Cannot make binary object [type=" + type + "]");

        BinaryTypeImpl binType = (BinaryTypeImpl)ctx.cacheObjects().binary().type(type);

        Deserializer deserializer = new Deserializer(
            type, queryFields(cacheName, type), binaryFields(binType), binaryClass(binType), parser.getCodec());

        return deserializer.deserialize(parser.readValueAsTree());
    }

    /**
     * @param cacheName Cache name.
     * @param type Type name.
     * @return Mapping from field name to its type.
     */
    private Map<String, Class<?>> queryFields(String cacheName, String type) {
        if (ctx.query().moduleEnabled()) {
            GridQueryTypeDescriptor desc = ctx.query().typeDescriptor(cacheName, type);

            if (desc != null)
                return desc.fields();
        }

        return Collections.emptyMap();
    }

    /**
     * @param type Binary type.
     * @return Mapping from field name to its binary type.
     */
    private Map<String, BinaryFieldMetadata> binaryFields(BinaryTypeImpl type) {
        if (type != null)
            return type.metadata().fieldsMap();

        return Collections.emptyMap();
    }

    /**
     * @param type Binary type.
     * @return Resovled class for specified binary type or {@code null} if the class could not be resolved.
     */
    private @Nullable Class<?> binaryClass(BinaryTypeImpl type) {
        try {
            if (type != null)
                return BinaryUtils.resolveClass(type.context(), type.typeId(), null, null, false);
        }
        catch (BinaryInvalidTypeException ignore) {
            // No-op.
        }

        return null;
    }

    /**
     * JSON deserializer into the Ignite binary object.
     */
    private class Deserializer {
        /** New binary type name. */
        private final String type;

        /** Cache query fields. */
        protected final Map<String, Class<?>> qryFields;

        /** Binary object fields. */
        private final Map<String, BinaryFieldMetadata> binFields;

        /** Class described by a binary type. */
        private final Class<?> binCls;

        /** JSON object mapper. */
        protected final ObjectCodec mapper;

        /**
         * @param type Type name.
         * @param qryFields Mapping from field name to its type.
         * @param binFields Mapping from field name to its binary type.
         * @param binCls Class described by the specified type or {@code null} if the class could not be resolved.
         * @param mapper JSON object mapper.
         */
        public Deserializer(
            String type,
            Map<String, Class<?>> qryFields,
            Map<String, BinaryFieldMetadata> binFields,
            @Nullable Class<?> binCls,
            ObjectCodec mapper
        ) {
            this.type = type;
            this.mapper = mapper;
            this.qryFields = qryFields;
            this.binFields = binFields;
            this.binCls = binCls;
        }

        /**
         * Deserialize JSON tree.
         *
         * @param tree JSON tree node.
         * @return Binary object.
         * @throws IOException In case of error.
         */
        public BinaryObjectImpl deserialize(JsonNode tree) throws IOException {
            BinaryObjectBuilder builder = ctx.cacheObjects().builder(type);
            Iterator<Map.Entry<String, JsonNode>> itr = tree.fields();

            while (itr.hasNext()) {
                Map.Entry<String, JsonNode> entry = itr.next();

                String field = entry.getKey();
                JsonNode node = tree.get(field);
                Class<?> fieldCls = fieldClass(field, binFields.get(field));

                builder.setField(field, fieldCls != null ? mapper.treeToValue(node, fieldCls) : readField(field, node));
            }

            return (BinaryObjectImpl)builder.build();
        }

        /**
         * Read JSON field value into required format.
         *
         * @param name Field name.
         * @param node JSON node.
         * @return value.
         * @throws IOException In case of error.
         */
        @Nullable protected Object readField(String name, JsonNode node) throws IOException {
            JsonNodeType nodeType = node.getNodeType();

            if (nodeType == JsonNodeType.BINARY)
                return node.binaryValue();

            if (nodeType == JsonNodeType.BOOLEAN)
                return node.booleanValue();

            Class<?> cls = qryFields.get(QueryUtils.normalizeObjectName(name, true));

            if (cls != null)
                return mapper.treeToValue(node, cls);

            switch (nodeType) {
                case ARRAY:
                    List<Object> list = new ArrayList<>(node.size());
                    Iterator<JsonNode> itr = node.elements();

                    while (itr.hasNext())
                        list.add(readField(name, itr.next()));

                    return list;

                case NUMBER:
                    return node.numberValue();

                case OBJECT:
                    return mapper.treeToValue(node, Map.class);

                case STRING:
                    return node.asText();

                default:
                    return null;
            }
        }

        /**
         * @param name Field name.
         * @param binFieldMeta Binary field metadata.
         * @return Class for the specified field or {@code null} if the class could not be resolved.
         */
        @Nullable private Class<?> fieldClass(String name, @Nullable BinaryFieldMetadata binFieldMeta) {
            try {
                if (binCls != null)
                    return binCls.getDeclaredField(name).getType();
            }
            catch (NoSuchFieldException ignore) {
                // No-op.
            }

            if (binFieldMeta == null)
                return null;

            int typeId = binFieldMeta.typeId();

            assert typeId >= Byte.MIN_VALUE && typeId <= Byte.MAX_VALUE : "field=" + name + ", typeId=" + typeId;

            return BinaryUtils.FLAG_TO_CLASS.get((byte)typeId);
        }
    }
}
