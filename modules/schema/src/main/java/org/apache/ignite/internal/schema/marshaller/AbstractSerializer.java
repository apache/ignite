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
import org.apache.ignite.internal.schema.ByteBufferTuple;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.Tuple;
import org.apache.ignite.internal.schema.TupleAssembler;
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
        final TupleAssembler assembler = createAssembler(Objects.requireNonNull(key), val);

        return serialize0(assembler, key, val);
    }

    /** {@inheritDoc} */
    @Override public <K> K deserializeKey(byte[] data) throws SerializationException {
        final Tuple tuple = new ByteBufferTuple(schema, data);

        return (K)deserializeKey0(tuple);
    }

    /** {@inheritDoc} */
    @Override public <V> V deserializeValue(byte[] data) throws SerializationException {
        final Tuple tuple = new ByteBufferTuple(schema, data);

        return (V)deserializeValue0(tuple);
    }

    /** {@inheritDoc} */
    @Override public <K, V> Pair<K, V> deserialize(byte[] data) throws SerializationException {
        final Tuple tuple = new ByteBufferTuple(schema, data);

        return new Pair<>((K)deserializeKey0(tuple), (V)deserializeValue0(tuple));
    }

    /**
     * Tuple assembler factory method.
     *
     * @param key Key object.
     * @param val Value object.
     */
    protected abstract TupleAssembler createAssembler(Object key, @Nullable Object val);

    /**
     * Internal serialization method.
     *
     * @param asm Tuple assembler.
     * @param key Key object.
     * @param val Value object.
     * @return Serialized pair.
     * @throws SerializationException If failed.
     */
    protected abstract byte[] serialize0(TupleAssembler asm, Object key, Object val) throws SerializationException;

    /**
     * Extract key object from tuple.
     *
     * @param tuple Tuple.
     * @return Deserialized key object.
     * @throws SerializationException If failed.
     */
    protected abstract Object deserializeKey0(Tuple tuple) throws SerializationException;

    /**
     * Extract value object from tuple.
     *
     * @param tuple Tuple.
     * @return Deserialized value object.
     * @throws SerializationException If failed.
     */
    protected abstract Object deserializeValue0(Tuple tuple) throws SerializationException;
}
