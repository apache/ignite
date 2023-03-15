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

import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.util.ByteBufferUtils;
import org.hawkore.ignite.lucene.util.Hex;

/**
 * A {@link Mapper} to map blob values.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BlobMapper extends KeywordMapper {

    /**
     * Builds a new {@link BlobMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     */
    public BlobMapper(String field, String column, Boolean validated) {
        super(field, column, validated, Arrays.asList(String.class, ByteBuffer.class));
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {
        if (value instanceof ByteBuffer) {
            return base((ByteBuffer) value);
        } else if (value instanceof byte[]) {
            return base((byte[]) value);
        } else if (value instanceof String) {
            return base((String) value);
        }
        throw new IndexException("Field '{}' requires a byte array, but found '{}'", field, value);
    }

    private String base(ByteBuffer value) {
        return ByteBufferUtils.toHex(value);
    }
 
    private String base(byte[] value) {
        return ByteBufferUtils.toHex(value);
    }

    private String base(String value) {
        try {
            byte[] bytes = Hex.hexToBytes(value.replaceFirst("0x", ""));
            return Hex.bytesToHex(bytes);
        } catch (NumberFormatException e) {
            throw new IndexException(e, "Field '{}' requires an hex string, but found '{}'", field, value);
        }
    }
}
