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

package org.apache.ignite.internal.schema.marshaller.reflection;

import java.util.Objects;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.AbstractSerializer;
import org.apache.ignite.internal.schema.marshaller.SerializationException;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.util.Pair;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;

/**
 * Reflection based (de)serializer.
 */
public class JavaSerializer extends AbstractSerializer {
    /** Key class. */
    private final Class<?> keyClass;

    /** Value class. */
    private final Class<?> valClass;

    /** Key marshaller. */
    private final Marshaller keyMarsh;

    /** Value marshaller. */
    private final Marshaller valMarsh;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param keyClass Key type.
     * @param valClass Value type.
     */
    public JavaSerializer(SchemaDescriptor schema, Class<?> keyClass, Class<?> valClass) {
        super(schema);
        this.keyClass = keyClass;
        this.valClass = valClass;

        keyMarsh = Marshaller.createMarshaller(schema.keyColumns(), keyClass);
        valMarsh = Marshaller.createMarshaller(schema.valueColumns(), valClass);
    }

    /** {@inheritDoc} */
    @Override public BinaryRow serialize(Object key, @Nullable Object val) throws SerializationException {
        assert keyClass.isInstance(key);
        assert val == null || valClass.isInstance(val);

        final RowAssembler asm = createAssembler(Objects.requireNonNull(key), val);

        keyMarsh.writeObject(key, asm);

        if (val != null)
            valMarsh.writeObject(val, asm);

        return new ByteBufferRow(asm.toBytes());
    }

    /** {@inheritDoc} */
    @Override public <K> K deserializeKey(Row row) throws SerializationException {
        final Object o = keyMarsh.readObject(row);

        assert keyClass.isInstance(o);

        return (K)o;
    }

    /** {@inheritDoc} */
    @Override public <V> V deserializeValue(Row row) throws SerializationException {
        if (!row.hasValue())
            return null;

        final Object o = valMarsh.readObject(row);

        assert o == null || valClass.isInstance(o);

        return (V)o;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Pair<K, V> deserialize(Row row) throws SerializationException {
        return new Pair<>(deserializeKey(row), deserializeValue(row));
    }

    /**
     * Creates {@link RowAssembler} for key-value pair.
     *
     * @param key Key object.
     * @param val Value object.
     * @return Row assembler.
     */
    private RowAssembler createAssembler(Object key, Object val) {
        ObjectStatistic keyStat = collectObjectStats(schema.keyColumns(), keyMarsh, key);
        ObjectStatistic valStat = collectObjectStats(schema.valueColumns(), valMarsh, val);

        return new RowAssembler(schema, keyStat.nonNullColsSize, keyStat.nonNullCols, valStat.nonNullColsSize, valStat.nonNullCols);
    }

    /**
     * Reads object fields and gather statistic.
     *
     * @param cols Schema columns.
     * @param marsh Marshaller.
     * @param obj Object.
     * @return Object statistic.
     */
    private ObjectStatistic collectObjectStats(Columns cols, Marshaller marsh, Object obj) {
        if (obj == null || !cols.hasVarlengthColumns())
            return ObjectStatistic.ZERO_VARLEN_STATISTICS;

        int cnt = 0;
        int size = 0;

        for (int i = cols.firstVarlengthColumn(); i < cols.length(); i++) {
            final Object val = marsh.value(obj, i);

            if (val == null || cols.column(i).type().spec().fixedLength())
                continue;

            size += getValueSize(val, cols.column(i).type());
            cnt++;
        }

        return new ObjectStatistic(cnt, size);
    }

    /**
     * Object statistic.
     */
    private static class ObjectStatistic {
        /** Cached zero statistics. */
        static final ObjectStatistic ZERO_VARLEN_STATISTICS = new ObjectStatistic(0, 0);

        /** Non-null columns of varlen type. */
        int nonNullCols;

        /** Length of all non-null columns of varlen types. */
        int nonNullColsSize;

        /** Constructor. */
        ObjectStatistic(int nonNullCols, int nonNullColsSize) {
            this.nonNullCols = nonNullCols;
            this.nonNullColsSize = nonNullColsSize;
        }
    }
}
