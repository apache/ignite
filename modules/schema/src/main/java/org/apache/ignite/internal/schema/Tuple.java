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

import java.util.BitSet;
import java.util.UUID;

/**
 * The class contains non-generic methods to read boxed and unboxed primitives based on the schema column types.
 * Any type conversions and coercions should be implemented outside of the tuple by the key-value or query runtime.
 * When a non-boxed primitive is read from a null column value, it is converted to the primitive type default value.
 */
public abstract class Tuple {
    /** */
    public static final int SCHEMA_VERSION_FIELD_SIZE = 2;

    /** */
    public static final int FLAGS_FIELD_SIZE = 2;

    /** */
    public static final int KEY_HASH_FIELD_SIZE = 4;

    /** */
    public static final int TUPLE_HEADER_SIZE = SCHEMA_VERSION_FIELD_SIZE + FLAGS_FIELD_SIZE + KEY_HASH_FIELD_SIZE;

    /** */
    public static final int TOTAL_LEN_FIELD_SIZE = 4;

    /** */
    public static final int VARLEN_TABLE_SIZE_FIELD_SIZE = 2;

    /** */
    public static final int VARLEN_COLUMN_OFFSET_FIELD_SIZE = 2;

    /** Schema descriptor for which this tuple was created. */
    private final SchemaDescriptor schema;

    /**
     * @param schema Schema instance.
     */
    protected Tuple(SchemaDescriptor schema) {
        this.schema = schema;
    }

    /**
     */
    public byte byteValue(int col) {
        long off = findColumn(col, NativeTypeSpec.BYTE);

        return off < 0 ? 0 : readByte(offset(off));
    }

    /**
     */
    public Byte byteValueBoxed(int col) {
        long off = findColumn(col, NativeTypeSpec.BYTE);

        return off < 0 ? null : readByte(offset(off));
    }

    /**
     */
    public short shortValue(int col) {
        long off = findColumn(col, NativeTypeSpec.SHORT);

        return off < 0 ? 0 : readShort(offset(off));
    }

    /**
     */
    public Short shortValueBoxed(int col) {
        long off = findColumn(col, NativeTypeSpec.SHORT);

        return off < 0 ? null : readShort(offset(off));
    }

    /**
     */
    public int intValue(int col) {
        long off = findColumn(col, NativeTypeSpec.INTEGER);

        return off < 0 ? 0 : readInteger(offset(off));
    }

    /**
     */
    public Integer intValueBoxed(int col) {
        long off = findColumn(col, NativeTypeSpec.INTEGER);

        return off < 0 ? null : readInteger(offset(off));
    }

    /**
     */
    public long longValue(int col) {
        long off = findColumn(col, NativeTypeSpec.LONG);

        return off < 0 ? 0 : readLong(offset(off));
    }

    /**
     */
    public Long longValueBoxed(int col) {
        long off = findColumn(col, NativeTypeSpec.LONG);

        return off < 0 ? null : readLong(offset(off));
    }

    /**
     */
    public float floatValue(int col) {
        long off = findColumn(col, NativeTypeSpec.FLOAT);

        return off < 0 ? 0.f : readFloat(offset(off));
    }

    /**
     */
    public Float floatValueBoxed(int col) {
        long off = findColumn(col, NativeTypeSpec.FLOAT);

        return off < 0 ? null : readFloat(offset(off));
    }

    /**
     */
    public double doubleValue(int col) {
        long off = findColumn(col, NativeTypeSpec.DOUBLE);

        return off < 0 ? 0.d : readDouble(offset(off));
    }

    /**
     */
    public Double doubleValueBoxed(int col) {
        long off = findColumn(col, NativeTypeSpec.DOUBLE);

        return off < 0 ? null : readDouble(offset(off));
    }

    /**
     */
    public String stringValue(int col) {
        long offLen = findColumn(col, NativeTypeSpec.STRING);

        if (offLen < 0)
            return null;

        int off = offset(offLen);
        int len = length(offLen);

        return readString(off, len);
    }

    /**
     */
    public byte[] bytesValue(int col) {
        long offLen = findColumn(col, NativeTypeSpec.BYTES);

        if (offLen < 0)
            return null;

        int off = offset(offLen);
        int len = length(offLen);

        return readBytes(off, len);
    }

    /**
     */
    public UUID uuidValue(int col) {
        long found = findColumn(col, NativeTypeSpec.UUID);

        if (found < 0)
            return null;

        int off = offset(found);

        long lsb = readLong(off);
        long msb = readLong(off + 8);

        return new UUID(msb, lsb);
    }

    /**
     */
    public BitSet bitmaskValue(int colIdx) {
        long offLen = findColumn(colIdx, NativeTypeSpec.BITMASK);

        if (offLen < 0)
            return null;

        int off = offset(offLen);

        Column col = schema.column(colIdx);

        return BitSet.valueOf(readBytes(off, col.type().length()));
    }

    /**
     * Gets the column offset and length encoded into a single 8-byte value (4 least significant bytes encoding the
     * offset from the beginning of the tuple and 4 most significant bytes encoding the field length for varlength
     * columns). The offset and length should be extracted using {@link #offset(long)} and {@link #length(long)}
     * methods.
     * Will also validate that the actual column type matches the requested column type, throwing
     * {@link InvalidTypeException} if the types do not match.
     *
     * @param colIdx Column index.
     * @param type Expected column type.
     * @return Encoded offset + length of the column.
     * @see #offset(long)
     * @see #length(long)
     * @see InvalidTypeException If actual column type does not match the requested column type.
     */
    private long findColumn(int colIdx, NativeTypeSpec type) {
        // Get base offset (key start or value start) for the given column.
        boolean keyCol = schema.keyColumn(colIdx);
        Columns cols = keyCol ? schema.keyColumns() : schema.valueColumns();

        int off = TUPLE_HEADER_SIZE;

        if (!keyCol) {
            // Jump to the next chunk, the size of the first chunk is written at the chunk start.
            off += readInteger(off);

            // Adjust the column index according to the number of key columns.
            colIdx -= schema.keyColumns().length();
        }

        Column col = cols.column(colIdx);

        if (col.type().spec() != type)
            throw new InvalidTypeException("Invalid column type requested [requested=" + type +
                ", column=" + col + ']');

        if (isNull(off, colIdx))
            return -1;

        return type.fixedLength() ?
            fixlenColumnOffset(cols, off, colIdx) :
            varlenColumnOffsetAndLength(cols, off, colIdx);
    }

    /**
     * Checks the tuple's null map for the given column index in the chunk.
     *
     * @param baseOff Offset of the chunk start in the tuple.
     * @param idx Offset of the column in the chunk.
     * @return {@code true} if the column value is {@code null}.
     */
    private boolean isNull(int baseOff, int idx) {
        int nullMapOff = nullMapOffset(baseOff);

        int nullByte = idx / 8;
        int posInByte = idx % 8;

        int map = readByte(nullMapOff + nullByte);

        return (map & (1 << posInByte)) != 0;
    }

    /**
     * Utility method to extract the column offset from the {@link #findColumn(int, NativeTypeSpec)} result. The
     * offset is calculated from the beginning of the tuple.
     *
     * @param offLen {@code findColumn} invocation result.
     * @return Column offset from the beginning of the tuple.
     */
    private static int offset(long offLen) {
        return (int)offLen;
    }

    /**
     * Utility method to extract the column length from the {@link #findColumn(int, NativeTypeSpec)} result for
     * varlen columns.
     *
     * @param offLen {@code findColumn} invocation result.
     * @return Length of the column or {@code 0} if the column is fixed-length.
     */
    private static int length(long offLen) {
        return (int)(offLen >>> 32);
    }

    /**
     * Calculates the offset and length of varlen column. First, it calculates the number of non-null columns
     * preceding the requested column by folding the null map bits. This number is used to adjust the column index
     * and find the corresponding entry in the varlen table. The length of the column is calculated either by
     * subtracting two adjacent varlen table offsets, or by subtracting the last varlen table offset from the chunk
     * length.
     *
     * @param cols Columns chunk.
     * @param baseOff Chunk base offset.
     * @param idx Column index in the chunk.
     * @return Encoded offset (from the tuple start) and length of the column with the given index.
     */
    private long varlenColumnOffsetAndLength(Columns cols, int baseOff, int idx) {
        int nullMapOff = nullMapOffset(baseOff);

        int nullStartByte = cols.firstVarlengthColumn() / 8;
        int startBitInByte = cols.firstVarlengthColumn() % 8;

        int nullEndByte = idx / 8;
        int endBitInByte = idx % 8;
        int numNullsBefore = 0;

        for (int i = nullStartByte; i <= nullEndByte; i++) {
            byte nullmapByte = readByte(nullMapOff + i);

            if (i == nullStartByte)
                // We need to clear startBitInByte least significant bits
                nullmapByte &= (0xFF << startBitInByte);

            if (i == nullEndByte)
                // We need to clear 8-endBitInByte most significant bits
                nullmapByte &= (0xFF >> (8 - endBitInByte));

            numNullsBefore += Columns.numberOfNullColumns(nullmapByte);
        }

        idx -= cols.numberOfFixsizeColumns() + numNullsBefore;
        int vartableSize = readShort(baseOff + TOTAL_LEN_FIELD_SIZE);

        int vartableOff = vartableOffset(baseOff);
        // Offset of idx-th column is from base offset.
        int resOff = readShort(vartableOff + VARLEN_COLUMN_OFFSET_FIELD_SIZE * idx);

        long len = idx == vartableSize - 1 ?
            // totalLength - columnStartOffset
            readInteger(baseOff) - resOff :
            // nextColumnStartOffset - columnStartOffset
            readShort(vartableOff + VARLEN_COLUMN_OFFSET_FIELD_SIZE * (idx + 1)) - resOff;

        return (len << 32) | (resOff + baseOff);
    }

    /**
     * Calculates the offset of the fixlen column with the given index in the tuple. It essentially folds the null map
     * with the column lengths to calculate the size of non-null columns preceding the requested column.
     *
     * @param cols Columns chunk.
     * @param baseOff Chunk base offset.
     * @param idx Column index in the chunk.
     * @return Encoded offset (from the tuple start) of the requested fixlen column.
     */
    int fixlenColumnOffset(Columns cols, int baseOff, int idx) {
        int nullMapOff = nullMapOffset(baseOff);

        int off = 0;
        int nullMapIdx = idx / 8;

        // Fold offset based on the whole map bytes in the schema
        for (int i = 0; i < nullMapIdx; i++)
            off += cols.foldFixedLength(i, readByte(nullMapOff + i));

        // Set bits starting from posInByte, inclusive, up to either the end of the byte or the last column index, inclusive
        int startBit = idx % 8;
        int endBit = nullMapIdx == cols.nullMapSize() - 1 ? ((cols.numberOfFixsizeColumns() - 1) % 8) : 7;
        int mask = (0xFF >> (7 - endBit)) & (0xFF << startBit);

        off += cols.foldFixedLength(nullMapIdx, readByte(nullMapOff + nullMapIdx) | mask);

        return nullMapOff + cols.nullMapSize() + off;
    }

    /**
     * @param baseOff Chunk base offset.
     * @return Null map offset from the tuple start for the chunk with the given base.
     */
    private int nullMapOffset(int baseOff) {
        int varlenTblSize = readShort(baseOff + TOTAL_LEN_FIELD_SIZE);

        return vartableOffset(baseOff) + varlenTblSize * VARLEN_COLUMN_OFFSET_FIELD_SIZE;
    }

    /**
     * @param baseOff Chunk base offset.
     * @return Offset of the varlen table from the tuple start for the chunk with the given base.
     */
    private int vartableOffset(int baseOff) {
        return baseOff + TOTAL_LEN_FIELD_SIZE + VARLEN_TABLE_SIZE_FIELD_SIZE;
    }

    /**
     */
    protected abstract byte readByte(int off);

    /**
     */
    protected abstract short readShort(int off);

    /**
     */
    protected abstract int readInteger(int off);

    /**
     */
    protected abstract long readLong(int off);

    /**
     */
    protected abstract float readFloat(int off);

    /**
     */
    protected abstract double readDouble(int off);

    /**
     */
    protected abstract String readString(int off, int len);

    /**
     */
    protected abstract byte[] readBytes(int off, int len);
}
