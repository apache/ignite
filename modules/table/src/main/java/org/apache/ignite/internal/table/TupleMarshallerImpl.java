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
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.RowAssembler;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

/**
 * Marshaller implementation.
 */
class TupleMarshallerImpl implements TupleMarshaller {
    /** Schema manager. */
    private final TableSchemaManager schemaMgr;

    /**
     * Constructor.
     *
     * @param schemaMgr Schema manager.
     */
    TupleMarshallerImpl(TableSchemaManager schemaMgr) {
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

        final RowAssembler rowBuilder = new RowAssembler(schema, 4096, 0, 0);

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
            case LONG: {
                rowAsm.appendLong(tup.longValue(col.name()));

                break;
            }

            default:
                throw new IllegalStateException("Unexpected value: " + col.type());
        }
    }

}
