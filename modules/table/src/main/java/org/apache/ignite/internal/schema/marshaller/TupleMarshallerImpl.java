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

import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tuple marshaller implementation.
 */
public class TupleMarshallerImpl implements TupleMarshaller {
    /** Poison object. */
    private static final Object POISON_OBJECT = new Object();
    
    /** Schema manager. */
    private final SchemaRegistry schemaReg;

    /**
     * Creates tuple marshaller.
     *
     * @param schemaReg Schema manager.
     */
    public TupleMarshallerImpl(SchemaRegistry schemaReg) {
        this.schemaReg = schemaReg;

        schemaReg.waitLatestSchema(); //TODO: Fix schema synchronization.
    }
    
    /** {@inheritDoc} */
    @Override
    public Row marshal(@NotNull Tuple tuple) throws TupleMarshallerException {
        try {
            SchemaDescriptor schema = schemaReg.schema();
            
            InternalTuple keyTuple0 = toInternalTuple(schema, tuple, true);
            InternalTuple valTuple0 = toInternalTuple(schema, tuple, false);
            
            if (valTuple0.knownColumns() + keyTuple0.knownColumns() != tuple.columnCount()) {
                throw new SchemaMismatchException(
                        String.format("Tuple doesn't match schema: schemaVersion=%s, extraColumns=%s",
                                schema.version(), extraColumnNames(tuple, schema)));
            }

            return buildRow(schema, keyTuple0, valTuple0);
        } catch (Exception ex) {
            throw new TupleMarshallerException("Failed to marshal tuple.", ex);
        }
    }
    
    /** {@inheritDoc} */
    @Override
    public Row marshal(@NotNull Tuple keyTuple, @Nullable Tuple valTuple) throws TupleMarshallerException {
        try {
            SchemaDescriptor schema = schemaReg.schema();
            
            InternalTuple keyTuple0 = toInternalTuple(schema, keyTuple, true);
            InternalTuple valTuple0 = toInternalTuple(schema, valTuple, false);
            
            if (keyTuple0.knownColumns() != keyTuple.columnCount()) {
                throw new SchemaMismatchException(
                        String.format("Key tuple doesn't match schema: schemaVersion=%s, extraColumns=%s",
                                schema.version(), extraColumnNames(keyTuple, true, schema)));
            }

            if (valTuple != null && valTuple0.knownColumns() != valTuple.columnCount()) {
                throw new SchemaMismatchException(
                        String.format("Value tuple doesn't match schema: schemaVersion=%s, extraColumns=%s",
                                schema.version(), extraColumnNames(valTuple, false, schema)));
            }
            
            return buildRow(schema, keyTuple0, valTuple0);
        } catch (Exception ex) {
            throw new TupleMarshallerException("Failed to marshal tuple.", ex);
        }
    }
    
    /**
     * Marshal tuple to a row.
     *
     * @param schema    Schema.
     * @param keyTuple0 Internal key tuple.
     * @param valTuple0 Internal value tuple.
     * @return Row.
     * @throws SchemaMismatchException If failed to write tuple column.
     */
    @NotNull
    private Row buildRow(SchemaDescriptor schema, InternalTuple keyTuple0, InternalTuple valTuple0) throws SchemaMismatchException {
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
    @Override
    public Row marshalKey(@NotNull Tuple keyTuple) throws TupleMarshallerException {
        try {
            final SchemaDescriptor schema = schemaReg.schema();
            
            InternalTuple keyTuple0 = toInternalTuple(schema, keyTuple, true);

            if (keyTuple0.knownColumns() < keyTuple.columnCount()) {
                throw new SchemaMismatchException("Key tuple contains extra columns: " + extraColumnNames(keyTuple, true, schema));
            }
            
            final RowAssembler rowBuilder = createAssembler(schema, keyTuple0, InternalTuple.NO_VALUE);
            
            Columns cols = schema.keyColumns();
            
            for (int i = 0, len = cols.length(); i < len; i++) {
                final Column col = cols.column(i);
                
                writeColumn(rowBuilder, col, keyTuple0);
            }
            
            return new Row(schema, rowBuilder.build());
        } catch (Exception ex) {
            throw new TupleMarshallerException("Failed to marshal tuple.", ex);
        }
    }
    
    /**
     * Analyze tuple and wrap into internal tuple.
     *
     * @param schema  Schema.
     * @param tuple   Key or value tuple.
     * @param keyFlag If {@code true} marshal key columns, otherwise marshall value columns.
     * @return Internal tuple.
     * @throws SchemaMismatchException If tuple doesn't match the schema.
     */
    private @NotNull InternalTuple toInternalTuple(SchemaDescriptor schema, Tuple tuple, boolean keyFlag) throws SchemaMismatchException {
        if (tuple == null) {
            return InternalTuple.NO_VALUE;
        }
        
        Columns columns = keyFlag ? schema.keyColumns() : schema.valueColumns();
        
        int nonNullVarlen = 0;
        int nonNullVarlenSize = 0;
        int knownColumns = 0;
        Map<String, Object> defaults = new HashMap<>();
        
        if (tuple instanceof SchemaAware && Objects.equals(((SchemaAware) tuple).schema(), schema)) {
            for (int i = 0, len = columns.length(); i < len; i++) {
                final Column col = columns.column(i);
                
                Object val = tuple.valueOrDefault(col.name(), POISON_OBJECT);
                
                assert val != POISON_OBJECT;

                if (val == null || columns.firstVarlengthColumn() < i) {
                    continue;
                }
                
                nonNullVarlenSize += getValueSize(val, col.type());
                nonNullVarlen++;
            }
        } else {
            for (int i = 0, len = columns.length(); i < len; i++) {
                final Column col = columns.column(i);
                
                Object val = tuple.valueOrDefault(col.name(), POISON_OBJECT);

                if (val == POISON_OBJECT) {
                    if (keyFlag) {
                        throw new SchemaMismatchException("Missed key column: " + col.name());
                    }

                    val = col.defaultValue();

                    defaults.put(col.name(), val);
                } else {
                    knownColumns++;
                }
                
                col.validate(val);

                if (val == null || columns.isFixedSize(i)) {
                    continue;
                }
                
                nonNullVarlenSize += getValueSize(val, col.type());
                nonNullVarlen++;
            }
        }
        
        return new InternalTuple(tuple, nonNullVarlen, nonNullVarlenSize, defaults, knownColumns);
    }
    
    /**
     * Extracts columns.
     *
     * @param tuple  Tuple representing a Row.
     * @param schema Schema.
     * @return Extra columns.
     */
    private Set<String> extraColumnNames(Tuple tuple, SchemaDescriptor schema) {
        Set<String> cols = new HashSet<>();
        
        for (int i = 0, len = tuple.columnCount(); i < len; i++) {
            String colName = tuple.columnName(i);

            if (schema.column(colName) == null) {
                cols.add(colName);
            }
        }
        
        return cols;
    }
    
    /**
     * Return column names that are missed in the schema.
     *
     * @param tuple    Key or value tuple.
     * @param keyTuple Key tuple flag. {@code True} if tuple is a key. {@code false} if tuple is value.
     * @param schema   Schema to check against.
     * @return Column names.
     */
    @NotNull
    private Set<String> extraColumnNames(Tuple tuple, boolean keyTuple, SchemaDescriptor schema) {
        Set<String> cols = new HashSet<>();
        
        for (int i = 0, len = tuple.columnCount(); i < len; i++) {
            String colName = tuple.columnName(i);
            
            Column col = schema.column(colName);

            if (col == null || schema.isKeyColumn(col.schemaIndex()) ^ keyTuple) {
                cols.add(colName);
            }
        }
        
        return cols;
    }

    /**
     * Creates {@link RowAssembler} for key-value tuples.
     *
     * @param schema   Schema.
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
     * Writes column.
     *
     * @param rowAsm Row assembler.
     * @param col    Column.
     * @param tup    Internal tuple.
     * @throws SchemaMismatchException If a tuple column value doesn't match the current column type.
     */
    private void writeColumn(RowAssembler rowAsm, Column col, InternalTuple tup) throws SchemaMismatchException {
        RowAssembler.writeValue(rowAsm, col, tup.value(col.name()));
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
         * @param tuple             Tuple.
         * @param nonNullVarlen     Non-null varlen values.
         * @param nonNullVarlenSize Total size of non-null varlen values.
         * @param defaults          Default values map.
         * @param knownColumns      Number of columns that match schema.
         */
        InternalTuple(Tuple tuple, int nonNullVarlen, int nonNullVarlenSize, Map<String, Object> defaults, int knownColumns) {
            this.nonNullVarlen = nonNullVarlen;
            this.nonNullVarLenSize = nonNullVarlenSize;
            this.tuple = tuple;
            this.defaults = defaults;
            this.knownColumns = knownColumns;
        }
        
        /**
         * Returns number of columns that matches schema.
         */
        public int knownColumns() {
            return knownColumns;
        }
        
        /**
         * Returns column value.
         *
         * @param columnName Columns name.
         */
        Object value(String columnName) {
            Object val = tuple.valueOrDefault(columnName, POISON_OBJECT);

            if (val == POISON_OBJECT) {
                return defaults.get(columnName);
            }
            
            return val;
        }
    }
}
