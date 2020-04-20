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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryClassDescriptor;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.IgniteUtils;
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

        ObjectCodec mapper = parser.getCodec();
        JsonNode jsonTree = mapper.readTree(parser);

        return (BinaryObjectImpl)deserialize0(cacheName, type, jsonTree, mapper);
    }

    /**
     * @param cacheName Cache name.
     * @param type Type name.
     * @param node JSON node.
     * @param mapper JSON object mapper.
     * @return Deserialized object.
     * @throws IOException In case of error.
     */
    private Object deserialize0(String cacheName, String type, JsonNode node, ObjectCodec mapper) throws IOException {
        if (ctx.marshallerContext().isSystemType(type)) {
            Class<?> cls = IgniteUtils.classForName(type, null);

            if (cls != null)
                return mapper.treeToValue(node, cls);
        }

        BinaryType binType = ctx.cacheObjects().binary().type(type);

        Deserializer deserializer = binType instanceof BinaryTypeImpl ?
            new BinaryTypeDeserializer(cacheName, (BinaryTypeImpl)binType, mapper) :
            new Deserializer(cacheName, type, mapper);

        return deserializer.deserialize(node);
    }

    /**
     * JSON deserializer creates a new binary type using JSON data types.
     */
    private class Deserializer {
        /** New binary type name. */
        private final String type;

        /** Cache query meta. */
        protected final Map<String, Class<?>> qryFields;

        /** Cache name. */
        protected final String cacheName;

        /** JSON object mapper. */
        protected final ObjectCodec mapper;

        /**
         * @param cacheName Cache name.
         * @param type Type name.
         * @param mapper JSON object mapper.
         */
        public Deserializer(@Nullable String cacheName, String type, ObjectCodec mapper) {
            this.type = type;
            this.mapper = mapper;
            this.cacheName = cacheName;

            qryFields = qryFields();
        }

        /**
         * @return Mapping from field name to its type.
         */
        private Map<String, Class<?>> qryFields() {
            if (ctx.query().moduleEnabled()) {
                QueryTypeDescriptorImpl desc = ctx.query().typeDescriptor(cacheName, type);

                if (desc != null)
                    return desc.fields();
            }

            return Collections.emptyMap();
        }

        /**
         * Deserialize JSON tree.
         *
         * @param tree JSON tree node.
         * @return Binary object.
         * @throws IOException In case of error.
         */
        public BinaryObject deserialize(JsonNode tree) throws IOException {
            return binaryValue(type, tree);
        }

        /**
         * @param type Ignite binary type name.
         * @param tree JSON tree node.
         * @return Binary object.
         * @throws IOException In case of error.
         */
        private BinaryObject binaryValue(String type, JsonNode tree) throws IOException {
            BinaryObjectBuilder builder = ctx.cacheObjects().builder(type);

            Iterator<Map.Entry<String, JsonNode>> itr = tree.fields();

            while (itr.hasNext()) {
                Map.Entry<String, JsonNode> entry = itr.next();

                String fieldName = entry.getKey();
                Object val = readField(fieldName, entry.getValue());

                builder.setField(fieldName, val);
            }

            return builder.build();
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
    }

    /**
     * JSON deserializer using the existing Ignite binary type.
     */
    private class BinaryTypeDeserializer extends Deserializer {
        /** Binary type. */
        private final BinaryTypeImpl binType;

        /** Binary class descriptor. */
        private final BinaryClassDescriptor binClsDesc;

        /**
         * @param cacheName Cache name.
         * @param binaryType Binary type.
         * @param mapper JSON object mapper.
         */
        public BinaryTypeDeserializer(@Nullable String cacheName, BinaryTypeImpl binaryType, ObjectCodec mapper) {
            super(cacheName, binaryType.typeName(), mapper);

            binType = binaryType;
            binClsDesc = binaryClassDescriptor();
        }

        /** {@inheritDoc} */
        @Override public BinaryObject deserialize(JsonNode tree) throws IOException {
            BinaryObjectBuilder builder = ctx.cacheObjects().builder(binType.typeName());
            Map<String, BinaryFieldMetadata> metas = binType.metadata().fieldsMap();

            Iterator<Map.Entry<String, JsonNode>> itr = tree.fields();

            while (itr.hasNext()) {
                Map.Entry<String, JsonNode> entry = itr.next();

                String field = entry.getKey();
                JsonNode node = tree.get(field);
                BinaryFieldMetadata meta = metas.get(field);

                Object val = meta != null ? readField(field, meta.typeId(), node, binType) : readField(field, node);

                builder.setField(field, val);
            }

            return builder.build();
        }

        /**
         * Read JSON field value into required format.
         *
         * @param name Field name.
         * @param typeId Field type ID.
         * @param node JSON node.
         * @param nodeType Object binary type.
         *
         * @return Extracted value.
         * @throws IOException if failed.
         */
        private Object readField(String name, int typeId, JsonNode node, BinaryTypeImpl nodeType) throws IOException {
            Class<?> baseCls;

            switch (typeId) {
                case GridBinaryMarshaller.MAP:
                    baseCls = Map.class;

                    break;

                case GridBinaryMarshaller.COL:
                case GridBinaryMarshaller.OBJ:
                case GridBinaryMarshaller.BINARY_OBJ:
                case GridBinaryMarshaller.OBJ_ARR:
                case GridBinaryMarshaller.ENUM:
                    baseCls = fieldClass(name);

                    if (baseCls == null)
                        return readField(name, node);

                    break;

                default:
                    baseCls = BinaryUtils.FLAG_TO_CLASS.get((byte)typeId);
            }

            if (baseCls != null)
                return mapper.treeToValue(node, baseCls);

            return deserialize0(cacheName, nodeType.fieldTypeName(name), node, mapper);
        }

        /**
         * @return Class descriptor for current binary type or {@code null} if the class was not found.
         */
        private @Nullable BinaryClassDescriptor binaryClassDescriptor() {
            try {
                return binType.context().descriptorForTypeId(false, binType.typeId(), null, false);
            }
            catch (BinaryObjectException ignore) {
                return null;
            }
        }

        /**
         * @param field Field name.
         * @return Class for the specified field or {@code null} if the class was not resolved.
         */
        private @Nullable Class<?> fieldClass(String field) {
            try {
                if (binClsDesc != null)
                    return binClsDesc.describedClass().getDeclaredField(field).getType();
            }
            catch (NoSuchFieldException ignore) {
                // No-op.
            }

            return null;
        }
    }
}
