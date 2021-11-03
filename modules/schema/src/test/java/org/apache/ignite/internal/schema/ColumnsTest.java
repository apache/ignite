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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 *
 */
public class ColumnsTest {
    /**
     *
     */
    @Test
    public void fixSizedColumnsIndex() {
        Columns cols = new Columns(
                0,
                new Column("intCol2", INT32, false),
                new Column("intCol1", INT32, false),
                new Column("uuidCol", UUID, false)
        );

        assertEquals(3, cols.length());
        assertEquals(-1, cols.firstVarlengthColumn());

        for (int c = 0; c < cols.length(); c++) {
            assertTrue(cols.isFixedSize(c));
        }

        assertEquals(0, cols.nullMapSize());
        assertEquals(3, cols.numberOfFixsizeColumns());
    }

    /**
     *
     */
    @Test
    public void varlenColumnsIndex() {
        Columns cols = new Columns(
                0,
                new Column("stringCol3", STRING, false),
                new Column("stringCol2", STRING, false),
                new Column("stringCol1", STRING, false)
        );

        assertEquals(3, cols.length());
        assertEquals(0, cols.firstVarlengthColumn());

        for (int c = 0; c < cols.length(); c++) {
            assertFalse(cols.isFixedSize(c));
        }

        assertEquals(0, cols.nullMapSize());
        assertEquals(0, cols.numberOfFixsizeColumns());
    }

    /**
     *
     */
    @Test
    public void mixedColumnsIndex() {
        Columns cols = new Columns(
                0,
                new Column("stringCol", STRING, false),
                new Column("intCol2", INT32, false),
                new Column("intCol1", INT32, false),
                new Column("uuidCol", UUID, false)
        );

        assertEquals(4, cols.length());
        assertEquals(3, cols.firstVarlengthColumn());

        for (int c = 0; c < cols.length(); c++) {
            if (c < cols.firstVarlengthColumn()) {
                assertTrue(cols.isFixedSize(c));
            } else {
                assertFalse(cols.isFixedSize(c));
            }
        }

        assertEquals(0, cols.nullMapSize());
        assertEquals(3, cols.numberOfFixsizeColumns());
    }

    /**
     *
     */
    @Test
    public void nullMapSize() {
        assertEquals(1, new Columns(0, columns(1)).nullMapSize());
        assertEquals(1, new Columns(0, columns(7)).nullMapSize());
        assertEquals(1, new Columns(0, columns(8)).nullMapSize());

        assertEquals(2, new Columns(0, columns(9)).nullMapSize());
        assertEquals(2, new Columns(0, columns(10)).nullMapSize());
        assertEquals(2, new Columns(0, columns(15)).nullMapSize());
        assertEquals(2, new Columns(0, columns(16)).nullMapSize());

        assertEquals(3, new Columns(0, columns(17)).nullMapSize());
        assertEquals(3, new Columns(0, columns(18)).nullMapSize());
        assertEquals(3, new Columns(0, columns(23)).nullMapSize());
        assertEquals(3, new Columns(0, columns(24)).nullMapSize());
    }

    /**
     *
     */
    @Test
    public void columnSchemaIndex() {
        Columns cols0 = new Columns(
                0,
                new Column("uuidCol", UUID, false),
                new Column("intCol2", INT32, false),
                new Column("intCol1", INT32, false)
        );

        assertEquals("intCol1", cols0.column(0).name());
        assertEquals(0, cols0.column(0).schemaIndex());

        assertEquals("intCol2", cols0.column(1).name());
        assertEquals(1, cols0.column(1).schemaIndex());

        assertEquals("uuidCol", cols0.column(2).name());
        assertEquals(2, cols0.column(2).schemaIndex());

        Columns cols1 = new Columns(
                3,
                new Column("uuidCol", UUID, false),
                new Column("intCol2", INT32, false),
                new Column("intCol1", INT32, false)
        );

        assertEquals("intCol1", cols1.column(0).name());
        assertEquals(3, cols1.column(0).schemaIndex());

        assertEquals("intCol2", cols1.column(1).name());
        assertEquals(4, cols1.column(1).schemaIndex());

        assertEquals("uuidCol", cols1.column(2).name());
        assertEquals(5, cols1.column(2).schemaIndex());
    }

    /**
     *
     */
    @Test
    public void foldSizeNoVarlenIncomplete1Byte() {
        Column[] colDef = {
                new Column("a", INT16, false), // 2
                new Column("b", INT32, false), // 4
                new Column("c", INT32, false), // 4
                new Column("d", INT64, false), // 8
                new Column("e", INT64, false), // 8
                new Column("f", INT64, false), // 8
                new Column("g", UUID, false)   // 16
        };

        checkColumnFolding(colDef);
    }

    /**
     *
     */
    @Test
    public void foldSizeNoVarlenFull1Byte() {
        Column[] colDef = {
                new Column("a", INT16, false), // 2
                new Column("b", INT32, false), // 4
                new Column("c", INT32, false), // 4
                new Column("d", INT32, false), // 4
                new Column("e", INT64, false), // 8
                new Column("f", INT64, false), // 8
                new Column("g", UUID, false),  // 16
                new Column("h", UUID, false)   // 16
        };

        checkColumnFolding(colDef);
    }

    /**
     *
     */
    @Test
    public void foldSizeNoVarlenIncomplete2Bytes() {
        Column[] colDef = {
                new Column("a", INT16, false), // 2
                new Column("b", INT16, false), // 2
                new Column("c", INT32, false), // 4
                new Column("d", INT32, false), // 4
                new Column("e", INT32, false), // 4
                new Column("f", INT32, false), // 4
                new Column("g", INT64, false), // 8
                new Column("h", INT64, false), // 8
                new Column("i", UUID, false),  // 16
                new Column("j", UUID, false)   // 16
        };

        checkColumnFolding(colDef);
    }

    /**
     *
     */
    @Test
    public void foldSizeNoVarlenFull2Bytes() {
        Column[] colDef = {
                new Column("a", INT16, false), // 2
                new Column("b", INT16, false), // 2
                new Column("c", INT16, false), // 2
                new Column("d", INT16, false), // 2
                new Column("e", INT32, false), // 4
                new Column("f", INT32, false), // 4
                new Column("g", INT32, false), // 4
                new Column("h", INT32, false), // 4
                new Column("i", INT32, false), // 4
                new Column("j", INT32, false), // 4
                new Column("k", INT64, false), // 8
                new Column("l", INT64, false), // 8
                new Column("m", INT64, false), // 8
                new Column("n", UUID, false),  // 16
                new Column("o", UUID, false),  // 16
                new Column("p", UUID, false)   // 16
        };

        checkColumnFolding(colDef);
    }

    /**
     *
     */
    @Test
    public void foldSizeVarlenIncomplete1Byte() {
        Column[] colDef = {
                new Column("a", INT16, false), // 2
                new Column("b", INT32, false), // 4
                new Column("c", INT32, false), // 4
                new Column("d", INT32, false), // 4
                new Column("e", INT64, false), // 8
                new Column("f", STRING, false),
                new Column("g", BYTES, false)
        };

        checkColumnFolding(colDef);
    }

    /**
     *
     */
    @Test
    public void foldSizeVarlenFull1Byte() {
        Column[] colDef = {
                new Column("a", INT16, false), // 2
                new Column("b", INT32, false), // 4
                new Column("c", INT32, false), // 4
                new Column("d", INT32, false), // 4
                new Column("e", INT64, false), // 8
                new Column("f", STRING, false),
                new Column("g", STRING, false),
                new Column("h", BYTES, false)
        };

        checkColumnFolding(colDef);
    }

    /**
     *
     */
    @Test
    public void foldSizeVarlenIncomplete2Bytes1() {
        Column[] colDef = {
                new Column("a", INT16, false), // 2
                new Column("b", INT32, false), // 4
                new Column("c", INT32, false), // 4
                new Column("d", INT32, false), // 4
                new Column("e", INT32, false), // 4
                new Column("f", INT64, false), // 8
                new Column("g", STRING, false),
                new Column("h", STRING, false),
                new Column("i", BYTES, false)
        };

        checkColumnFolding(colDef);
    }

    /**
     *
     */
    @Test
    public void foldSizeVarlenIncomplete2Bytes2() {
        Column[] colDef = {
                new Column("a", INT16, false), // 2
                new Column("b", INT32, false), // 4
                new Column("c", INT32, false), // 4
                new Column("d", INT32, false), // 4
                new Column("e", INT32, false), // 4
                new Column("f", INT32, false), // 4
                new Column("g", INT32, false), // 4
                new Column("h", INT64, false), // 8
                new Column("i", STRING, false),
                new Column("j", STRING, false),
                new Column("k", BYTES, false)
        };

        checkColumnFolding(colDef);
    }

    /**
     *
     */
    @Test
    public void foldSizeVarlenIncomplete2Bytes3() {
        Column[] colDef = {
                new Column("a", INT16, false), // 2
                new Column("b", INT32, false), // 4
                new Column("c", INT32, false), // 4
                new Column("d", INT32, false), // 4
                new Column("e", INT32, false), // 4
                new Column("f", INT32, false), // 4
                new Column("g", INT32, false), // 4
                new Column("h", INT64, false), // 8
                new Column("i", INT64, false), // 8
                new Column("j", STRING, false),
                new Column("k", BYTES, false)
        };

        checkColumnFolding(colDef);
    }

    /**
     *
     */
    @Test
    public void foldSizeVarlenFull2Bytes() {
        Column[] colDef = {
                new Column("a", INT16, false), // 2
                new Column("b", INT32, false), // 4
                new Column("c", INT32, false), // 4
                new Column("d", INT32, false), // 4
                new Column("e", INT32, false), // 4
                new Column("f", INT32, false), // 4
                new Column("g", INT32, false), // 4
                new Column("h", INT32, false), // 4
                new Column("i", INT64, false), // 8
                new Column("j", STRING, false),
                new Column("k", BYTES, false),
                new Column("l", BYTES, false),
                new Column("m", BYTES, false),
                new Column("n", BYTES, false),
                new Column("o", BYTES, false),
                new Column("p", BYTES, false)
        };

        checkColumnFolding(colDef);
    }

    /**
     *
     */
    private void checkColumnFolding(Column[] colDef) {
        Columns cols = new Columns(0, colDef);

        boolean[] nullMasks = new boolean[cols.numberOfFixsizeColumns()];

        for (int i = 0; i < (1 << cols.numberOfFixsizeColumns()); i++) {
            checkSize(cols, colDef, nullMasks);

            incrementMask(nullMasks);
        }
    }

    /**
     *
     */
    private void incrementMask(boolean[] mask) {
        boolean add = true;

        for (int i = 0; i < mask.length && add; i++) {
            add = mask[i];
            mask[i] = !mask[i];
        }
    }

    /**
     *
     */
    private void checkSize(Columns cols, Column[] colDef, boolean[] nullMasks) {
        // Iterate over bytes first
        for (int b = 0; b < (cols.numberOfFixsizeColumns() + 7) / 8; b++) {
            // Start with all non-nulls.
            int mask = 0x00;
            int size = 0;

            for (int bit = 0; bit < 8; bit++) {
                int idx = 8 * b + bit;

                if (idx >= cols.numberOfFixsizeColumns()) {
                    break;
                }

                assertTrue(colDef[idx].type().spec().fixedLength());

                if (nullMasks[idx]) { // set bit in the mask (indicate null value).
                    mask |= (1 << bit);
                } else { // non-null, sum the size.
                    size += colDef[idx].type().sizeInBytes();
                }
            }

            assertEquals(size, cols.foldFixedLength(b, mask), "Failed [b=" + b + ", mask=" + mask + ']');
        }
    }

    /**
     *
     */
    private static Column[] columns(int size) {
        Column[] ret = new Column[size];

        for (int i = 0; i < ret.length; i++) {
            if (i % 3 == 0) {
                ret[i] = new Column("column-" + i, INT64, true);
            } else {
                ret[i] = new Column("column-" + i, STRING, true);
            }
        }

        return ret;
    }
}
