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

package org.apache.ignite.internal.binary;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlIndexMetadata;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Custom serializer for {@link GridCacheSqlIndexMetadata}
 */
class BinaryObjectImplSerializer extends JsonSerializer<BinaryObjectImpl> {
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

                        if (ref.hasCircularReferences()) {
                            throw ser.mappingException("Failed convert to JSON object for circular references");
                        }
                    }

                    if (val instanceof BinaryEnumObjectImpl) {
                        gen.writeObjectField(name, ((BinaryObject)val).enumName());
                    }
                    else {
                        gen.writeObjectField(name, val);
                    }
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
}
