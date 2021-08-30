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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaMode;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;

/**
 * Tuple marshaller implementation.
 */
public class TupleMarshallerImpl implements TupleMarshaller {
    /** Poison object. */
    private static final Object POISON_OBJECT = new Object();

    /** Schema manager. */
    private final SchemaRegistry schemaReg;

    /** Table manager. */
    private final TableManager tblMgr;

    /** Internal table. */
    private final InternalTable tbl;

    /**
     * Creates tuple marshaller.
     *
     * @param tblMgr Table manager.
     * @param tbl Internal table.
     * @param schemaReg Schema manager.
     */
    public TupleMarshallerImpl(TableManager tblMgr, InternalTable tbl, SchemaRegistry schemaReg) {
        this.schemaReg = schemaReg;
        this.tblMgr = tblMgr;
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public Row marshal(@NotNull Tuple tuple) {
        SchemaDescriptor schema = schemaReg.schema();

        InternalTuple keyTuple0 = toInternalTuple(schema, schema.keyColumns(), tuple);
        InternalTuple valTuple0 = toInternalTuple(schema, schema.valueColumns(), tuple);

        while (true) {
            if (valTuple0.knownColumns + keyTuple0.knownColumns == tuple.columnCount())
                break; // Nothing to do.

            if (tbl.schemaMode() == SchemaMode.STRICT_SCHEMA)
                throw new SchemaMismatchException("Value doesn't match schema.");

            createColumns(extractColumnsType(tuple, extraColumnNames(tuple, schema)));

            assert schemaReg.lastSchemaVersion() > schema.version();

            schema = schemaReg.schema();

            keyTuple0 = toInternalTuple(schema, schema.keyColumns(), tuple);
            valTuple0 = toInternalTuple(schema, schema.valueColumns(), tuple);
        }

        return buildRow(schema, keyTuple0, valTuple0);
    }

    /** {@inheritDoc} */
    @Override public Row marshal(@NotNull Tuple keyTuple, Tuple valTuple) {
        SchemaDescriptor schema = schemaReg.schema();

        InternalTuple keyTuple0 = toInternalTuple(schema, schema.keyColumns(), keyTuple);
        InternalTuple valTuple0 = toInternalTuple(schema, schema.valueColumns(), valTuple);

        while (true) {
            if (keyTuple0.hasExtraColumns())
                throw new SchemaMismatchException("Key tuple doesn't match schema: extraColumns=" + extraColumnNames(keyTuple, true, schema));

            if (!valTuple0.hasExtraColumns())
                break; // Nothing to do.

            if (tbl.schemaMode() == SchemaMode.STRICT_SCHEMA)
                throw new SchemaMismatchException("Value doesn't match schema.");

            createColumns(extractColumnsType(valTuple, extraColumnNames(valTuple, false, schema)));

            assert schemaReg.lastSchemaVersion() > schema.version();

            schema = schemaReg.schema();

            keyTuple0 = toInternalTuple(schema, schema.keyColumns(), keyTuple);
            valTuple0 = toInternalTuple(schema, schema.valueColumns(), valTuple);
        }

        return buildRow(schema, keyTuple0, valTuple0);
    }

    @NotNull private Row buildRow(SchemaDescriptor schema, InternalTuple keyTuple0, InternalTuple valTuple0) {
        RowAssembler rowBuilder = createAssembler(schema, keyTuple0, valTuple0);

        Columns columns = schema.keyColumns();

        for (int i = 0, len = columns.length(); i < len; i++) {
            final Column col = columns.column(i);

            writeColumn(rowBuilder, col, keyTuple0);
        }

        if (valTuple0.tuple != null) {
            columns = schema.valueColumns();

            for (int i = 0, len = columns.length(); i < len; i++) {
                final Column col = columns.column(i);

                writeColumn(rowBuilder, col, valTuple0);
            }
        }

        return new Row(schema, rowBuilder.build());
    }

    /** {@inheritDoc} */
    @Override public Row marshalKey(@NotNull Tuple keyTuple) {
        final SchemaDescriptor schema = schemaReg.schema();

        InternalTuple keyTuple0 = toInternalTuple(schema, schema.keyColumns(), keyTuple);

        if (keyTuple0.hasExtraColumns())
            throw new SchemaMismatchException("Key tuple doesn't match schema: extraColumns=" + extraColumnNames(keyTuple, true, schema));

        final RowAssembler rowBuilder = createAssembler(schema, keyTuple0, InternalTuple.NO_VALUE);

        Columns cols = schema.keyColumns();

        for (int i = 0, len = cols.length(); i < len; i++) {
            final Column col = cols.column(i);

            writeColumn(rowBuilder, col, keyTuple0);
        }

        return new Row(schema, rowBuilder.build());
    }

    /**
     * Analyze tuple and wrap into internal tuple.
     *
     * @param schema Schema.
     * @param columns Tuple columns.
     * @param tuple Key or value tuple.
     * @return Internal tuple
     */
    private @NotNull InternalTuple toInternalTuple(SchemaDescriptor schema, Columns columns, Tuple tuple) {
        if (tuple == null)
            return InternalTuple.NO_VALUE;

        int nonNullVarlen = 0;
        int nonNullVarlenSize = 0;
        int knownColumns = 0;
        Map<String, Object> defaults = new HashMap<>();

        if (tuple instanceof SchemaAware && Objects.equals(((SchemaAware)tuple).schema(), schema)) {
            for (int i = 0, len = columns.length(); i < len; i++) {
                final Column col = columns.column(i);

                Object val = tuple.valueOrDefault(col.name(), POISON_OBJECT); //TODO: can we access by index here ???

                // TODO: how to detect key/value tuple ???
                // TODO: maybe unwrap to Row of known schema ???
                assert val != POISON_OBJECT;

                if (val == null || columns.firstVarlengthColumn() < i)
                    continue;

                nonNullVarlenSize += getValueSize(val, col.type());
                nonNullVarlen++;
            }
        }
        else {
            for (int i = 0, len = columns.length(); i < len; i++) {
                final Column col = columns.column(i);

                Object val = tuple.valueOrDefault(col.name(), POISON_OBJECT);

                if (val == POISON_OBJECT) {
                    val = col.defaultValue();

                    defaults.put(col.name(), val);
                }
                else
                    knownColumns++;

                col.validate(val);

                if (val == null || columns.isFixedSize(i))
                    continue;

                //TODO: save default value to tuple?

                nonNullVarlenSize += getValueSize(val, col.type());
                nonNullVarlen++;
            }
        }

        return new InternalTuple(tuple, nonNullVarlen, nonNullVarlenSize, defaults, knownColumns);
    }

    /**
     * @param tuple Tuple representing a Row.
     * @param schema Schema
     * @return
     */
    private Set<String> extraColumnNames(Tuple tuple, SchemaDescriptor schema) {
        Set<String> cols = new HashSet<>();

        for (int i = 0, len = tuple.columnCount(); i < len; i++) {
            String colName = tuple.columnName(i);

            if (schema.column(colName) == null)
                cols.add(colName);
        }

        return cols;
    }

    /**
     * Return column names that are missed in schema.
     *
     * @param tuple Key or value tuple.
     * @param keyTuple Key tuple flag. {@code True} if tuple is a key. {@code false} if tuple is value.
     * @param schema Schema to check against.
     * @return Column names.
     */
    @NotNull private Set<String> extraColumnNames(Tuple tuple, boolean keyTuple, SchemaDescriptor schema) {
        Set<String> cols = new HashSet<>();

        for (int i = 0, len = tuple.columnCount(); i < len; i++) {
            String colName = tuple.columnName(i);

            Column col = schema.column(colName);

            if (col == null || schema.isKeyColumn(col.schemaIndex()) ^ keyTuple)
                cols.add(colName);
        }

        return cols;
    }

    /**
     * Extract column types from the tuple that are missed in schema.
     *
     * @param tuple Tuple with column values.
     * @param colNames Column names that type info to be extracted.
     * @return Column types.
     */
    private Set<org.apache.ignite.schema.Column> extractColumnsType(Tuple tuple, Set<String> colNames) {
        Set<org.apache.ignite.schema.Column> extraColumns = new HashSet<>();

        for (String colName : colNames) {
            Object colValue = tuple.value(colName);

            if (colValue == null) // Can't detect type of 'null'
                throw new InvalidTypeException("Live schema upgrade for 'null' value is not supported yet.");

            ColumnType colType = SchemaConfigurationConverter.columnType(colValue.getClass());

            if (colType == null) // No native support for type.
                throw new InvalidTypeException("Live schema upgrade for type [" + colValue.getClass() + "] is not supported.");

            extraColumns.add(SchemaBuilders.column(colName, colType).asNullable().build());
        }

        return extraColumns;
    }

    /**
     * Creates {@link RowAssembler} for key-value tuples.
     *
     * @param schema Schema.
     * @param keyTuple Internal key tuple.
     * @param valTuple Internal value tuple.
     * @return Row assembler.
     */
    private RowAssembler createAssembler(SchemaDescriptor schema, InternalTuple keyTuple, InternalTuple valTuple) {
        return new RowAssembler(
            schema,
            keyTuple.nonNullVarLenSize,
            keyTuple.nonNullVarlen,
            valTuple.nonNullVarLenSize,
            valTuple.nonNullVarlen);
    }

    /**
     * @param rowAsm Row assembler.
     * @param col Column.
     * @param tup Internal tuple.
     */
    private void writeColumn(RowAssembler rowAsm, Column col, InternalTuple tup) {
        RowAssembler.writeValue(rowAsm, col, tup.value(col.name()));
    }

    /**
     * Updates the schema with new columns.
     *
     * @param extraCols Columns to add.
     */
    private void createColumns(Set<org.apache.ignite.schema.Column> newCols) {
        //TODO: Introduce internal TableManager and use UUID instead of names ???
        tblMgr.alterTable(tbl.tableName(), chng -> chng.changeColumns(cols -> {
            int colIdx = chng.columns().size();
            //TODO: avoid 'colIdx' or replace with correct last colIdx.

            for (org.apache.ignite.schema.Column column : newCols) {
                cols.create(String.valueOf(colIdx), colChg -> convert(column, colChg));
                colIdx++;
            }
        }));
    }

    /**
     * Internal tuple enriched original tuple with additional info.
     */
    private static class InternalTuple {
        /** Cached zero statistics. */
        static final InternalTuple NO_VALUE = new InternalTuple(null, 0, 0, null, 0);

        /** Original tuple. */
        private final Tuple tuple;

        /** Number of non-null varlen columns. */
        private final int nonNullVarlen;

        /** Length of all non-null fields of varlen types. */
        private final int nonNullVarLenSize;

        /** Pre-calculated defaults. */
        private final Map<String, Object> defaults;

        /** Schema columns in tuple. */
        private final int knownColumns;

        InternalTuple(Tuple tuple, int nonNullVarlen, int nonNullVarlenSize, Map<String, Object> defaults, int knownColumns) {
            this.nonNullVarlen = nonNullVarlen;
            this.nonNullVarLenSize = nonNullVarlenSize;
            this.tuple = tuple;
            this.defaults = defaults;
            this.knownColumns = knownColumns;
        }

        /**
         * @return {@code True} extra columns was detected, {@code false} otherwise.
         */
        public boolean hasExtraColumns() {
            return tuple != null && knownColumns != tuple.columnCount();
        }

        /**
         * @param columnName Columns name.
         * @return Column value.
         */
        Object value(String columnName) {
            Object val = tuple.valueOrDefault(columnName, POISON_OBJECT);

            if (val == POISON_OBJECT)
                return defaults.get(columnName);

            return val;
        }
    }
}
