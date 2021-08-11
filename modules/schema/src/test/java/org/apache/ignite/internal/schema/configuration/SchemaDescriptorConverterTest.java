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

package org.apache.ignite.internal.schema.configuration;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.builder.SchemaTableBuilder;
import org.apache.ignite.schema.builder.TableColumnBuilder;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for SchemaDescriptorConverter.
 */
public class SchemaDescriptorConverterTest {
    /** Total number of columns. */
    private static final int columns = 15;

    /**
     * Convert table with complex primary key and check it.
     */
    @Test
    public void testComplexPrimaryIndex() {
        SchemaTableBuilder bldr = getBuilder(false, false);
        SchemaTable tblSchm = bldr.withIndex(SchemaBuilders.pkIndex()
            .addIndexColumn("INT8").done()
            .addIndexColumn("ID").done()
            .build()
        ).build();

        SchemaDescriptor tblDscr = SchemaDescriptorConverter.convert(UUID.randomUUID(), 1, tblSchm);

        assertEquals(2, tblDscr.keyColumns().length());
        assertEquals(2, tblDscr.affinityColumns().length);
        assertEquals(columns - 2, tblDscr.valueColumns().length());
    }

    /**
     * Convert table with complex primary key with affinity column configured and check it.
     */
    @Test
    public void testComplexPrimaryIndexWithAffinity() {
        SchemaTableBuilder bldr = getBuilder(false, false);
        SchemaTable tblSchm = bldr.withIndex(SchemaBuilders.pkIndex()
            .addIndexColumn("INT8").done()
            .addIndexColumn("ID").done()
            .withAffinityColumns("INT8")
            .build()
        ).build();

        SchemaDescriptor tblDscr = SchemaDescriptorConverter.convert(UUID.randomUUID(), 1, tblSchm);

        assertEquals(2, tblDscr.keyColumns().length());
        assertEquals(1, tblDscr.affinityColumns().length);
        assertEquals(columns - 2, tblDscr.valueColumns().length());
    }

    /**
     * Convert table with nullable columns.
     */
    @Test
    public void convertNullable() {
        testConvert(true);
    }

    /**
     * Convert table with non nullable columns.
     */
    @Test
    public void convertTypes() {
        testConvert(false);
    }

    /**
     * Test set of columns.
     *
     * @param nullable Nullable flag.
     */
    private void testConvert(boolean nullable) {
        SchemaTable tblSchm = getBuilder(nullable, true).build();

        SchemaDescriptor tblDscr = SchemaDescriptorConverter.convert(UUID.randomUUID(), 1, tblSchm);

        assertEquals(1, tblDscr.keyColumns().length());
        testCol(tblDscr.keyColumns(), "ID", NativeTypeSpec.UUID, nullable);

        assertEquals(columns - 1, tblDscr.valueColumns().length());
        testCol(tblDscr.valueColumns(), "INT8", NativeTypeSpec.INT8, nullable);
        testCol(tblDscr.valueColumns(), "INT16", NativeTypeSpec.INT16, nullable);
        testCol(tblDscr.valueColumns(), "INT32", NativeTypeSpec.INT32, nullable);
        testCol(tblDscr.valueColumns(), "INT64", NativeTypeSpec.INT64, nullable);
        testCol(tblDscr.valueColumns(), "FLOAT", NativeTypeSpec.FLOAT, nullable);
        testCol(tblDscr.valueColumns(), "DOUBLE", NativeTypeSpec.DOUBLE, nullable);
        testCol(tblDscr.valueColumns(), "UUID", NativeTypeSpec.UUID, nullable);
        testCol(tblDscr.valueColumns(), "STRING", NativeTypeSpec.STRING, nullable);
        testCol(tblDscr.valueColumns(), "STRING_FS10", NativeTypeSpec.STRING, nullable);
        testCol(tblDscr.valueColumns(), "BLOB", NativeTypeSpec.BYTES, nullable);
        testCol(tblDscr.valueColumns(), "DECIMAL", NativeTypeSpec.DECIMAL, nullable);
        testCol(tblDscr.valueColumns(), "NUMBER", NativeTypeSpec.NUMBER, nullable);
        testCol(tblDscr.valueColumns(), "DECIMAL", NativeTypeSpec.DECIMAL, nullable);
        testCol(tblDscr.valueColumns(), "BITMASK_FS10", NativeTypeSpec.BITMASK, nullable);
    }

    /**
     * Get SchemaTableBuilder with default table.
     *
     * @param nullable If all columns should be nullable.
     * @param withPk If builder should contains primary key index.
     * @return SchemaTableBuilder.
     */
    private SchemaTableBuilder getBuilder(boolean nullable, boolean withPk) {
        Function<TableColumnBuilder, org.apache.ignite.schema.Column> postProcess = builder -> {
            if (nullable)
                builder.asNullable();
            else
                builder.asNonNull();
            return builder.build();
        };

        SchemaTableBuilder res = SchemaBuilders.tableBuilder("SCHEMA", "TABLE")
            .columns(
                postProcess.apply(SchemaBuilders.column("ID", ColumnType.UUID)),
                postProcess.apply(SchemaBuilders.column("INT8", ColumnType.INT8)),
                postProcess.apply(SchemaBuilders.column("INT16", ColumnType.INT16)),
                postProcess.apply(SchemaBuilders.column("INT32", ColumnType.INT32)),
                postProcess.apply(SchemaBuilders.column("INT64", ColumnType.INT64)),
                postProcess.apply(SchemaBuilders.column("FLOAT", ColumnType.FLOAT)),
                postProcess.apply(SchemaBuilders.column("DOUBLE", ColumnType.DOUBLE)),
                postProcess.apply(SchemaBuilders.column("UUID", ColumnType.UUID)),
                postProcess.apply(SchemaBuilders.column("STRING", ColumnType.string())),
                postProcess.apply(SchemaBuilders.column("STRING_FS10", ColumnType.stringOf(10))),
                postProcess.apply(SchemaBuilders.column("BLOB", ColumnType.blobOf())),
                postProcess.apply(SchemaBuilders.column("BLOB_FS10", ColumnType.blobOf(10))),
                postProcess.apply(SchemaBuilders.column("DECIMAL", ColumnType.decimalOf(1,1))),
                postProcess.apply(SchemaBuilders.column("NUMBER", ColumnType.numberOf(12))),
                postProcess.apply(SchemaBuilders.column("BITMASK_FS10", ColumnType.bitmaskOf(10)))
                // TODO: IGNITE-13750 uncomment after unsigned types available
                // postProcess.apply(SchemaBuilders.column("UINT8", ColumnType.UINT8)),
                // postProcess.apply(SchemaBuilders.column("UINT16", ColumnType.UINT16)),
                // postProcess.apply(SchemaBuilders.column("UINT32", ColumnType.UINT32)),
                // postProcess.apply(SchemaBuilders.column("UINT64", ColumnType.UINT64)),
            );
        if (withPk)
            res.withPrimaryKey("ID");

        return res;
    }

    /**
     * Check specified column to match other parameters.
     *
     * @param cols Columns to test.
     * @param name Expected column name.
     * @param type Expected column type.
     * @param nullable Expected column nullable flag.
     */
    private static void testCol(Columns cols, String name, NativeTypeSpec type, boolean nullable) {
        int idx = cols.columnIndex(name);
        Column col = cols.column(idx);

        assertEquals(name, col.name());
        assertEquals(type.name(), col.type().spec().name());
        assertEquals(nullable, col.nullable());

        if (col.type().spec().fixedLength())
            assertTrue(col.type().sizeInBytes() >= 0);
    }
}
