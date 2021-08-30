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
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;

/**
 * JSON deserializer into the Ignite binary object.
 */
public class IgniteBinaryObjectJsonDeserializer extends JsonDeserializer<BinaryObject> {
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
    @Override public BinaryObject deserialize(JsonParser parser, DeserializationContext dCtx) throws IOException {
        String type = (String)dCtx.findInjectableValue(BINARY_TYPE_PROPERTY, null, null);
        String cacheName = (String)dCtx.findInjectableValue(CACHE_NAME_PROPERTY, null, null);

        assert type != null;

        JsonNode tree = parser.readValueAsTree();
        ObjectCodec mapper = parser.getCodec();

        Map<String, BinaryFieldMetadata> binFields = binaryFields(type);
        Map<String, Class<?>> qryFields = queryFields(cacheName, type);

        BinaryObjectBuilder builder = ctx.cacheObjects().builder(type);
        Iterator<Map.Entry<String, JsonNode>> itr = tree.fields();

        while (itr.hasNext()) {
            Map.Entry<String, JsonNode> entry = itr.next();

            String field = entry.getKey();
            JsonNode node = entry.getValue();

            BinaryFieldMetadata meta = binFields.get(field);

            Class<?> fieldCls = meta != null ? BinaryUtils.FLAG_TO_CLASS.get((byte)meta.typeId()) : null;

            if (fieldCls == null)
                fieldCls = qryFields.getOrDefault(QueryUtils.normalizeObjectName(field, true), Object.class);

            builder.setField(field, mapper.treeToValue(node, fieldCls));
        }

        return builder.build();
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
