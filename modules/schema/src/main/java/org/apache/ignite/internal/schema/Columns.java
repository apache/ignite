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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.tostring.S;

/**
 * A set of columns representing a key or a value chunk in a row.
 * Provides necessary machinery to locate a column value in a concrete row.
 * <p>
 * <h3>Column ordering.</h3>
 * Column instances are comparable in lexicographic order, native type first and then column name. Nullability
 * flag is not taken into account when columns are compared.
 * Native type order guarantees fixed-len columns will prior to varlen columns,
 * column name order guarantees the same column order (for the same type) on all nodes.
 *
 * @see #COLUMN_COMPARATOR
 */
public class Columns implements Serializable {
    /** */
    public static final int[][] EMPTY_FOLDING_TABLE = new int[0][];

    /** */
    public static final int[] EMPTY_FOLDING_MASK = new int[0];

    /**
     * Lookup table to speed-up calculation of the number of null/non-null columns based on the null map.
     * For a given byte {@code b}, {@code NULL_COLUMNS_LOOKUP[b]} will contain the number of {@code null} columns
     * corresponding to the byte in nullability map.
     * For example, if nullability map is {@code 0b00100001}, then the map encodes nulls for columns 0 and 5 and
     * {@code NULL_COLUMNS_LOOKUP[0b00100001] == 2}.
     */
    private static final int[] NULL_COLUMNS_LOOKUP;

    /**
     * Columns in packed order for this chunk.
     */
    private final Column[] cols;

    /**
     * If the type contains varlength columns, this field will contain an index of the first such column.
     * Otherwise, it will contain {@code -1}.
     */
    private final int firstVarlenColIdx;

    /**
     * Number of bytes required to store the nullability map for this chunk.
     */
    private final int nullMapSize;

    /**
     * Estimated max length of fixed-size columns.
     */
    private int fixedSizeMaxLen;

    /**
     * Fixed-size column length folding table. The table is used to quickly calculate the offset of a fixed-length
     * column based on the nullability map.
     */
    private int[][] foldingTbl;

    /**
     * Additional mask values for folding table to cut off nullability map for columns with larger indexes.
     */
    private int[] foldingMask;

    static {
        NULL_COLUMNS_LOOKUP = new int[256];

        // Each nonzero bit is a null value.
        for (int i = 0; i < 255; i++)
            NULL_COLUMNS_LOOKUP[i] = Integer.bitCount(i);
    }

    /**
     * Column comparator.
     */
    public static final Comparator<Column> COLUMN_COMPARATOR = Comparator.comparing(Column::type).thenComparing(Column::name);

    /**
     * Gets a number of null columns for the given byte from the nullability map (essentially, the number of non-zero
     * bits in the given byte).
     *
     * @param nullMap Byte from a nullability map.
     * @return Number of null columns for the given byte.
     */
    public static int numberOfNullColumns(byte nullMap) {
        return NULL_COLUMNS_LOOKUP[nullMap & 0xFF];
    }

    /**
     * Constructs the columns chunk. The columns will be internally sorted in write-efficient order based on
     * {@link Column} comparison.
     *
     * @param baseSchemaIdx First column absolute index in schema.
     * @param cols Array of columns.
     */
    Columns(int baseSchemaIdx, Column... cols) {
        this.cols = sortedCopy(baseSchemaIdx, cols);

        firstVarlenColIdx = findFirstVarlenColumn();

        nullMapSize = hasNullableColumn() ? (cols.length + 7) / 8 : 0;

        buildFoldingTable();
    }

    /**
     * Calculates a sum of fixed-sized columns lengths given the mask of the present columns, assuming that the
     * {@code maskByte} is an {@code i}-th byte is columns mask.
     *
     * @param i Mask byte index in the nullability map.
     * @param maskByte Mask byte value, where a nonzero bit (counting from LSB to MSB) represents a {@code null} value
     *      and the corresponding column length should be skipped.
     * @return Fixed columns length sizes summed wrt to the mask.
     */
    public int foldFixedLength(int i, int maskByte) {
        return foldingTbl[i][maskByte & foldingMask[i]];
    }

    /**
     * @return Number of bytes required to store the nullability map for these columns.
     */
    public int nullMapSize() {
        return nullMapSize;
    }

    /**
     * @param idx Column index to check.
     * @return {@code true} if the column with the given index is fixed-size.
     */
    public boolean isFixedSize(int idx) {
        return cols[idx].type().spec().fixedLength();
    }

    /**
     * @param idx Column index.
     * @return Column instance.
     */
    public Column column(int idx) {
        return cols[idx];
    }

    /**
     * @return Sorted columns.
     */
    public Column[] columns() {
        return cols;
    }

    /**
     * @return Number of columns in this chunk.
     */
    public int length() {
        return cols.length;
    }

    /**
     * @return The number of varlength columns in this chunk.
     */
    public int numberOfVarlengthColumns() {
        return cols.length - numberOfFixsizeColumns();
    }

    /**
     * @return The number of fixsize columns in this chunk.
     */
    public int numberOfFixsizeColumns() {
        return firstVarlenColIdx == -1 ? cols.length : firstVarlenColIdx;
    }

    /**
     * @return The index of the first varlength column in the sorted order of columns.
     */
    public int firstVarlengthColumn() {
        return firstVarlenColIdx;
    }

    /**
     * @return {@code True} if there is at least one varlength column.
     */
    public boolean hasVarlengthColumns() {
        return firstVarlenColIdx != -1;
    }

    /**
     * @return Fixsize columns size upper bound.
     */
    public int fixsizeMaxLen() {
        return fixedSizeMaxLen;
    }

    /**
     * @param schemaBaseIdx Base index of this columns object in its schema.
     * @param cols User columns.
     * @return A copy of user columns array sorted in column order.
     */
    private Column[] sortedCopy(int schemaBaseIdx, Column[] cols) {
        Column[] cp = Arrays.copyOf(cols, cols.length);

        Arrays.sort(cp, COLUMN_COMPARATOR);

        for (int i = 0; i < cp.length; i++) {
            Column c = cp[i];

            cp[i] = c.copy(schemaBaseIdx + i);
        }

        return cp;
    }

    /**
     * @return Index of the first varlength column or {@code -1} if there are none.
     */
    private int findFirstVarlenColumn() {
        for (int i = 0; i < cols.length; i++) {
            if (!cols[i].type().spec().fixedLength())
                return i;
        }

        return -1;
    }

    /**
     * @return {@code True} if there is one or more nullable columns, {@code false} otherwise.
     */
    private boolean hasNullableColumn() {
        for (int i = 0; i < cols.length; i++) {
            if (cols[i].nullable())
                return true;
        }

        return false;
    }

    /**
     *
     */
    private void buildFoldingTable() {
        int numFixsize = numberOfFixsizeColumns();

        if (numFixsize == 0) {
            foldingTbl = EMPTY_FOLDING_TABLE;
            foldingMask = EMPTY_FOLDING_MASK;
            fixedSizeMaxLen = 0;

            return;
        }

        int fixsizeNullMapSize = (numFixsize + 7) / 8;
        int maxLen = 0;

        int[][] res = new int[fixsizeNullMapSize][];
        int[] resMask = new int[fixsizeNullMapSize];

        for (int b = 0; b < fixsizeNullMapSize; b++) {
            int bitsInMask = b == fixsizeNullMapSize - 1 ?
                (numFixsize - 8 * b) : 8;

            int totalMasks = 1 << bitsInMask;

            resMask[b] = 0xFF >>> (8 - bitsInMask);

            res[b] = new int[totalMasks];

            // Start with all non-nulls.
            int mask = 0x00;

            for (int i = 0; i < totalMasks; i++) {
                res[b][mask] = foldManual(b, mask);

                mask++;
            }

            maxLen += res[b][0];
        }

        foldingTbl = res;
        foldingMask = resMask;
        fixedSizeMaxLen = maxLen;
    }

    /**
     * Manually fold the sizes of the fixed-size columns based on the nullability map byte.
     *
     * @param b Nullability map byte index.
     * @param mask Nullability mask from the map.
     * @return Sum of column sizes based nullability mask.
     */
    private int foldManual(int b, int mask) {
        int size = 0;

        for (int bit = 0; bit < 8; bit++) {
            boolean hasVal = (mask & (1 << bit)) == 0;

            int idx = b * 8 + bit;

            if (hasVal && idx < numberOfFixsizeColumns()) {
                assert cols[idx].type().spec().fixedLength() : "Expected fixed-size column [b=" + b +
                    ", mask=" + mask +
                    ", cols" + Arrays.toString(cols) + ']';

                size += cols[idx].type().sizeInBytes();
            }
        }

        return size;
    }

    /**
     * Returns columns index for given column name.
     *
     * @param colName Column name.
     * @return Column index.
     */
    public int columnIndex(String colName) {
        for (int i = 0; i < cols.length; i++) {
            if (cols[i].name().equalsIgnoreCase(colName))
                return i;
        }

        throw new NoSuchElementException("No field '" + colName + "' defined");
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Columns columns = (Columns)o;

        return Arrays.equals(cols, columns.cols);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Arrays.hashCode(cols);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
       return S.arrayToString(cols);
    }
}
