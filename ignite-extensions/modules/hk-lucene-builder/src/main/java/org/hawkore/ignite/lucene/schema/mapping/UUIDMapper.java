/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.schema.mapping;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.util.ByteBufferUtils;
import org.hawkore.ignite.lucene.util.UUIDSerializer;

import com.google.common.primitives.Longs;

/**
 * A {@link Mapper} to map a UUID field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class UUIDMapper extends KeywordMapper {

    /**
     * Builds a new {@link UUIDMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     */
    public UUIDMapper(String field, String column, Boolean validated) {
        super(field, column, validated, Arrays.asList(String.class, UUID.class));
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {
        if (value instanceof UUID) {
            UUID uuid = (UUID) value;
            return serialize(uuid);
        } else if (value instanceof String) {
            try {
                String string = (String) value;
                UUID uuid = UUID.fromString(string);
                return serialize(uuid);
            } catch (IllegalArgumentException e) {
                throw new IndexException(e, "Field '{}' with value '{}' can not be parsed as UUID", name, value);
            }
        }
        throw new IndexException("Field '{}' requires an UUID, but found '{}'", name, value);
    }

    /**
     * Returns the {@link String} representation of the specified {@link UUID}. The returned value has the same
     * collation as {@link UUIDType}.
     *
     * @param uuid the {@link UUID} to be serialized
     * @return the {@link String} representation of the specified {@link UUID}
     */
    static String serialize(UUID uuid) {

        StringBuilder sb = new StringBuilder();

        // Get UUID type version
        ByteBuffer bb = UUIDSerializer.instance.serialize(uuid);
        int version = (bb.get(bb.position() + 6) >> 4) & 0x0f;

        // Add version at the beginning
        sb.append(ByteBufferUtils.toHex((byte) version));

        // If it's a time based UUID, add the UNIX timestamp
        if (version == 1) {
            long timestamp = uuid.timestamp();
            String timestampHex = ByteBufferUtils.toHex(Longs.toByteArray(timestamp));
            sb.append(timestampHex);
        }

        // Add the UUID itself
        sb.append(ByteBufferUtils.toHex(bb));
        return sb.toString();
    }

}
