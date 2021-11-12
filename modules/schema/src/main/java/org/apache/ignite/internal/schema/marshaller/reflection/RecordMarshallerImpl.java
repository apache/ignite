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

import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;

import java.util.Objects;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.RecordMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.NotNull;

/**
 * Record marshaller for given schema and mappers.
 *
 * @param <R> Record type.
 */
public class RecordMarshallerImpl<R> implements RecordMarshaller<R> {
    /** Schema. */
    private final SchemaDescriptor schema;
    
    /** Key marshaller. */
    private final Marshaller keyMarsh;
    
    /** Record marshaller. */
    private final Marshaller recMarsh;
    
    /** Record type. */
    private final Class<R> recClass;
    
    /**
     * Creates KV marshaller.
     */
    public RecordMarshallerImpl(SchemaDescriptor schema, Mapper<R> mapper) {
        this.schema = schema;
        
        recClass = mapper.targetType();
        
        keyMarsh = Marshaller.createMarshaller(schema.keyColumns().columns(), mapper);
        
        recMarsh = Marshaller.createMarshaller(
                ArrayUtils.concat(schema.keyColumns().columns(), schema.valueColumns().columns()),
                mapper
        );
    }
    
    /** {@inheritDoc} */
    @Override
    public int schemaVersion() {
        return schema.version();
    }
    
    /** {@inheritDoc} */
    @Override
    public BinaryRow marshal(@NotNull R rec) throws MarshallerException {
        assert recClass.isInstance(rec);
        
        final RowAssembler asm = createAssembler(Objects.requireNonNull(rec), rec);
        
        recMarsh.writeObject(rec, asm);
        
        return new ByteBufferRow(asm.toBytes());
    }
    
    /** {@inheritDoc} */
    @Override
    public BinaryRow marshalKey(@NotNull R rec) throws MarshallerException {
        assert recClass.isInstance(rec);
        
        final RowAssembler asm = createAssembler(Objects.requireNonNull(rec), null);
        
        keyMarsh.writeObject(rec, asm);
        
        return new ByteBufferRow(asm.toBytes());
    }
    
    /** {@inheritDoc} */
    @NotNull
    @Override
    public R unmarshal(@NotNull Row row) throws MarshallerException {
        final Object o = recMarsh.readObject(row);
        
        assert recClass.isInstance(o);
        
        return (R) o;
    }
    
    /**
     * Creates {@link RowAssembler} for key-value pair.
     *
     * @param key Key object.
     * @param val Value object.
     * @return Row assembler.
     */
    private RowAssembler createAssembler(Object key, Object val) {
        ObjectStatistic keyStat = collectObjectStats(schema.keyColumns(), recMarsh, key);
        ObjectStatistic valStat = collectObjectStats(schema.valueColumns(), recMarsh, val);
        
        return new RowAssembler(schema, keyStat.nonNullColsSize, keyStat.nonNullCols,
                valStat.nonNullColsSize, valStat.nonNullCols);
    }
    
    /**
     * Reads object fields and gather statistic.
     *
     * @param cols  Schema columns.
     * @param marsh Marshaller.
     * @param obj   Object.
     * @return Object statistic.
     */
    private ObjectStatistic collectObjectStats(Columns cols, Marshaller marsh, Object obj) {
        if (obj == null || !cols.hasVarlengthColumns()) {
            return ObjectStatistic.ZERO_VARLEN_STATISTICS;
        }
        
        int cnt = 0;
        int size = 0;
        
        for (int i = cols.firstVarlengthColumn(); i < cols.length(); i++) {
            final Column column = cols.column(i);
            final Object val = marsh.value(obj, column.schemaIndex());
            
            if (val == null || column.type().spec().fixedLength()) {
                continue;
            }
            
            size += getValueSize(val, column.type());
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
