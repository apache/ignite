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

package org.apache.ignite.internal.schema.marshaller;

import java.util.Objects;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.RowAssembler;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.util.Pair;
import org.jetbrains.annotations.Nullable;

/**
 * Base serializer class.
 */
public abstract class AbstractSerializer implements Serializer {
    /** Schema descriptor. */
    protected final SchemaDescriptor schema;

    /**
     * Constructor.
     *
     * @param schema Schema descriptor.
     */
    protected AbstractSerializer(SchemaDescriptor schema) {
        this.schema = schema;
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(Object key, Object val) throws SerializationException {
        final RowAssembler assembler = createAssembler(Objects.requireNonNull(key), val);

        return serialize0(assembler, key, val);
    }

    /** {@inheritDoc} */
    @Override public <K> K deserializeKey(byte[] data) throws SerializationException {
        final Row row = new Row(schema, new ByteBufferRow(data));

        return (K)deserializeKey0(row);
    }

    /** {@inheritDoc} */
    @Override public <V> V deserializeValue(byte[] data) throws SerializationException {
        final Row row = new Row(schema, new ByteBufferRow(data));

        return (V)deserializeValue0(row);
    }

    /** {@inheritDoc} */
    @Override public <K, V> Pair<K, V> deserialize(byte[] data) throws SerializationException {
        final Row row = new Row(schema, new ByteBufferRow(data));

        return new Pair<>((K)deserializeKey0(row), (V)deserializeValue0(row));
    }

    /**
     * Row assembler factory method.
     *
     * @param key Key object.
     * @param val Value object.
     */
    protected abstract RowAssembler createAssembler(Object key, @Nullable Object val);

    /**
     * Internal serialization method.
     *
     * @param asm Row assembler.
     * @param key Key object.
     * @param val Value object.
     * @return Serialized pair.
     * @throws SerializationException If failed.
     */
    protected abstract byte[] serialize0(RowAssembler asm, Object key, Object val) throws SerializationException;

    /**
     * Extract key object from row.
     *
     * @param row Row.
     * @return Deserialized key object.
     * @throws SerializationException If failed.
     */
    protected abstract Object deserializeKey0(Row row) throws SerializationException;

    /**
     * Extract value object from row.
     *
     * @param row Row.
     * @return Deserialized value object.
     * @throws SerializationException If failed.
     */
    protected abstract Object deserializeValue0(Row row) throws SerializationException;
}
