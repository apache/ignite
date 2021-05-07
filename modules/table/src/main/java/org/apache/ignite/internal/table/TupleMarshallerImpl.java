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

package org.apache.ignite.internal.table;

import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.RowAssembler;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;

/**
 * Marshaller implementation.
 */
public class TupleMarshallerImpl implements TupleMarshaller {
    /** Schema manager. */
    private final TableSchemaView schemaMgr;

    /**
     * Constructor.
     *
     * @param schemaMgr Schema manager.
     */
    public TupleMarshallerImpl(TableSchemaView schemaMgr) {
        this.schemaMgr = schemaMgr;
    }

    /** {@inheritDoc} */
    @Override public Row marshal(@NotNull Tuple tuple) {
        return marshal(tuple, tuple);
    }

    /** {@inheritDoc} */
    @Override public Row marshal(Tuple keyTuple, Tuple valTuple) {
        final SchemaDescriptor schema = schemaMgr.schema();

        assert keyTuple instanceof TupleBuilderImpl;

        final RowAssembler rowBuilder = createAssembler(schema, keyTuple, valTuple);

        for (int i = 0; i < schema.keyColumns().length(); i++) {
            final Column col = schema.keyColumns().column(i);

            writeColumn(keyTuple, col, rowBuilder);
        }

        if (valTuple != null) {
            for (int i = 0; i < schema.valueColumns().length(); i++) {
                final Column col = schema.valueColumns().column(i);

                writeColumn(valTuple, col, rowBuilder);
            }
        }

        return new Row(schema, new ByteBufferRow(rowBuilder.build()));
    }

    /**
     * Creates {@link RowAssembler} for key-value tuples.
     *
     * @param keyTuple Key tuple.
     * @param valTuple Value tuple.
     * @return Row assembler.
     */
    private RowAssembler createAssembler(SchemaDescriptor schema, Tuple keyTuple, Tuple valTuple) {
        final ObjectStatistic keyStat = collectObjectStats(schema.keyColumns(), keyTuple);
        final ObjectStatistic valStat = collectObjectStats(schema.keyColumns(), valTuple);

        int size = RowAssembler.rowSize(
            schema.keyColumns(), keyStat.nonNullCols, keyStat.nonNullColsSize,
            schema.valueColumns(), valStat.nonNullCols, valStat.nonNullColsSize);

        return new RowAssembler(schema, size, keyStat.nonNullCols, valStat.nonNullCols);
    }

    /**
     * @param tup Tuple.
     * @param col Column.
     * @param rowAsm Row assembler.
     */
    private void writeColumn(Tuple tup, Column col, RowAssembler rowAsm) {
        if (tup.value(col.name()) == null) {
            rowAsm.appendNull();

            return;
        }

        switch (col.type().spec()) {
            case BYTE: {
                rowAsm.appendByte(tup.byteValue(col.name()));

                break;
            }
            case SHORT: {
                rowAsm.appendShort(tup.shortValue(col.name()));

                break;
            }
            case INTEGER: {
                rowAsm.appendInt(tup.intValue(col.name()));

                break;
            }
            case LONG: {
                rowAsm.appendLong(tup.longValue(col.name()));

                break;
            }
            case FLOAT: {
                rowAsm.appendFloat(tup.floatValue(col.name()));

                break;
            }
            case DOUBLE: {
                rowAsm.appendDouble(tup.doubleValue(col.name()));

                break;
            }
            case UUID: {
                rowAsm.appendUuid(tup.value(col.name()));

                break;
            }
            case STRING: {
                rowAsm.appendString(tup.stringValue(col.name()));

                break;
            }
            case BYTES: {
                rowAsm.appendBytes(tup.value(col.name()));

                break;
            }
            case BITMASK: {
                rowAsm.appendBitmask(tup.value(col.name()));

                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + col.type());
        }
    }

    /**
     * Reads object fields and gather statistic.
     *
     * @param cols Schema columns.
     * @param tup Tuple.
     * @return Object statistic.
     */
    private ObjectStatistic collectObjectStats(Columns cols, Tuple tup) {
        if (tup == null || !cols.hasVarlengthColumns())
            return new ObjectStatistic(0, 0);

        int cnt = 0;
        int size = 0;

        for (int i = cols.firstVarlengthColumn(); i < cols.length(); i++) {
            final Object val = tup.value(cols.column(i).name());

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
        /** Non-null fields of varlen type. */
        int nonNullCols;

        /** Length of all non-null fields of varlen types. */
        int nonNullColsSize;

        /** Constructor. */
        ObjectStatistic(int nonNullCols, int nonNullColsSize) {
            this.nonNullCols = nonNullCols;
            this.nonNullColsSize = nonNullColsSize;
        }
    }
}
