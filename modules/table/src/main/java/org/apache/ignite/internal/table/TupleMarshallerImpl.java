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
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.ColumnType;
import org.apache.ignite.schema.definition.SchemaManagementMode;
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

        InternalTuple keyTuple0 = toInternalTuple(schema, tuple, true);
        InternalTuple valTuple0 = toInternalTuple(schema, tuple, false);

        while (valTuple0.knownColumns() + keyTuple0.knownColumns() != tuple.columnCount()) {
            if (tbl.schemaMode() == SchemaManagementMode.STRICT)
                throw new SchemaMismatchException("Value doesn't match schema.");

            createColumns(extractColumnsType(tuple, extraColumnNames(tuple, schema)));

            assert schemaReg.lastSchemaVersion() > schema.version();

            schema = schemaReg.schema();

            keyTuple0 = toInternalTuple(schema, tuple, true);
            valTuple0 = toInternalTuple(schema, tuple, false);
        }

        return buildRow(schema, keyTuple0, valTuple0);
    }

    /** {@inheritDoc} */
    @Override public Row marshal(@NotNull Tuple keyTuple, Tuple valTuple) {
        SchemaDescriptor schema = schemaReg.schema();

        InternalTuple keyTuple0 = toInternalTuple(schema, keyTuple, true);
        InternalTuple valTuple0 = toInternalTuple(schema, valTuple, false);

        while (true) {
            if (keyTuple0.knownColumns() < keyTuple.columnCount())
                throw new SchemaMismatchException("Key tuple contains extra columns: " + extraColumnNames(keyTuple, true, schema));

            if (valTuple == null || valTuple0.knownColumns() == valTuple.columnCount())
                break; // Nothing to do.

            if (tbl.schemaMode() == SchemaManagementMode.STRICT)
                throw new SchemaMismatchException("Value doesn't match schema.");

            createColumns(extractColumnsType(valTuple, extraColumnNames(valTuple, false, schema)));

            assert schemaReg.lastSchemaVersion() > schema.version();

            schema = schemaReg.schema();

            keyTuple0 = toInternalTuple(schema, keyTuple, true);
            valTuple0 = toInternalTuple(schema, valTuple, false);
        }

        return buildRow(schema, keyTuple0, valTuple0);
    }

    /**
     * Marshal tuple to a row.
     *
     * @param schema Schema.
     * @param keyTuple0 Internal key tuple.
     * @param valTuple0 Internal value tuple.
     * @return Row.
     */
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

        InternalTuple keyTuple0 = toInternalTuple(schema, keyTuple, true);

        if (keyTuple0.knownColumns() < keyTuple.columnCount())
            throw new SchemaMismatchException("Key tuple contains extra columns: " + extraColumnNames(keyTuple, true, schema));

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
     * @param tuple Key or value tuple.
     * @param keyFlag If {@code true} marshal key columns, otherwise marshall value columns.
     * @return Internal tuple
     */
    private @NotNull InternalTuple toInternalTuple(SchemaDescriptor schema, Tuple tuple, boolean keyFlag) {
        if (tuple == null)
            return InternalTuple.NO_VALUE;

        Columns columns = keyFlag ? schema.keyColumns() : schema.valueColumns();

        int nonNullVarlen = 0;
        int nonNullVarlenSize = 0;
        int knownColumns = 0;
        Map<String, Object> defaults = new HashMap<>();

        if (tuple instanceof SchemaAware && Objects.equals(((SchemaAware)tuple).schema(), schema)) {
            for (int i = 0, len = columns.length(); i < len; i++) {
                final Column col = columns.column(i);

                Object val = tuple.valueOrDefault(col.name(), POISON_OBJECT);

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
                    if (keyFlag)
                        throw new SchemaMismatchException("Missed key column: " + col.name());

                    val = col.defaultValue();

                    defaults.put(col.name(), val);
                }
                else
                    knownColumns++;

                col.validate(val);

                if (val == null || columns.isFixedSize(i))
                    continue;

                nonNullVarlenSize += getValueSize(val, col.type());
                nonNullVarlen++;
            }
        }

        return new InternalTuple(tuple, nonNullVarlen, nonNullVarlenSize, defaults, knownColumns);
    }

    /**
     * @param tuple Tuple representing a Row.
     * @param schema Schema.
     * @return Extra columns.
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
     * Return column names that are missed in the schema.
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
    private Set<ColumnDefinition> extractColumnsType(Tuple tuple, Set<String> colNames) {
        Set<ColumnDefinition> extraColumns = new HashSet<>();

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
     * @param newCols Columns to add.
     */
    private void createColumns(Set<ColumnDefinition> newCols) {
        tblMgr.alterTable(tbl.tableName(), chng -> chng.changeColumns(cols -> {
            int colIdx = chng.columns().size();
            //TODO: IGNITE-15096 avoid 'colIdx' or replace with correct last colIdx.

            for (ColumnDefinition column : newCols) {
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

        /**
         * Creates internal tuple.
         *
         * @param tuple Tuple.
         * @param nonNullVarlen Non-null varlen values.
         * @param nonNullVarlenSize Total size of non-null varlen values.
         * @param defaults Default values map.
         * @param knownColumns Number of columns that match schema.
         */
        InternalTuple(Tuple tuple, int nonNullVarlen, int nonNullVarlenSize, Map<String, Object> defaults, int knownColumns) {
            this.nonNullVarlen = nonNullVarlen;
            this.nonNullVarLenSize = nonNullVarlenSize;
            this.tuple = tuple;
            this.defaults = defaults;
            this.knownColumns = knownColumns;
        }

        /**
         * @return Number of columns that matches schema.
         */
        public int knownColumns() {
            return knownColumns;
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
