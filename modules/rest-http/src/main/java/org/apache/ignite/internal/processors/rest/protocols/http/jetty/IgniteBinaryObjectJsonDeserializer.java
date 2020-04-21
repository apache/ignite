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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

        JsonNode tree = parser.readValueAsTree();
        ObjectCodec mapper = parser.getCodec();

        Map<String, BinaryFieldMetadata> binFields = binaryFields(type);
        Map<String, Class<?>> qryFields = queryFields(cacheName, type);

        BinaryObjectBuilder builder = ctx.cacheObjects().builder(type);
        Iterator<Map.Entry<String, JsonNode>> itr = tree.fields();

        while (itr.hasNext()) {
            Map.Entry<String, JsonNode> entry = itr.next();

            String field = entry.getKey();
            JsonNode node = tree.get(field);

            Class<?> fieldCls = qryFields.get(QueryUtils.normalizeObjectName(field, true));

            if (fieldCls == null) {
                BinaryFieldMetadata meta = binFields.get(field);

                fieldCls = meta != null ? BinaryUtils.FLAG_TO_CLASS.get((byte)meta.typeId()) : null;
            }

            builder.setField(field, fieldCls != null ? mapper.treeToValue(node, fieldCls) : readField(node, mapper));
        }

        return (BinaryObjectImpl)builder.build();
    }

    /**
     * Read JSON field value into required format.
     *
     * @param node JSON node.
     * @param mapper JSON object mapper.
     * @return value.
     * @throws IOException In case of error.
     */
    @Nullable private Object readField(JsonNode node, ObjectCodec mapper) throws IOException {
        switch (node.getNodeType()) {
            case BINARY:
                return node.binaryValue();

            case BOOLEAN:
                return node.booleanValue();

            case ARRAY:
                List<Object> list = new ArrayList<>(node.size());
                Iterator<JsonNode> itr = node.elements();

                while (itr.hasNext())
                    list.add(readField(itr.next(), mapper));

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
     * @param type Binary type name.
     * @return Mapping from field name to its binary type.
     */
    private Map<String, BinaryFieldMetadata> binaryFields(String type) {
        BinaryTypeImpl binType = (BinaryTypeImpl)ctx.cacheObjects().binary().type(type);

        if (binType != null)
            return binType.metadata().fieldsMap();

        return Collections.emptyMap();
    }
}
