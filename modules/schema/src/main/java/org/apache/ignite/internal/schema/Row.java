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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.UUID;

/**
 * Schema-aware row.
 * <p>
 * The class contains non-generic methods to read boxed and unboxed primitives based on the schema column types.
 * Any type conversions and coercions should be implemented outside the row by the key-value or query runtime.
 * When a non-boxed primitive is read from a null column value, it is converted to the primitive type default value.
 */
public class Row implements BinaryRow {
    /** Schema descriptor. */
    private final SchemaDescriptor schema;

    /** Binary row. */
    private final BinaryRow row;

    /**
     * @param itemIdx Varlen table item index.
     * @return Varlen item offset.
     */
    public static int varlenItemOffset(int itemIdx) {
        return VARLEN_TABLE_SIZE_FIELD_SIZE + itemIdx * VARLEN_COLUMN_OFFSET_FIELD_SIZE;
    }

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param row Binary row representation.
     */
    public Row(SchemaDescriptor schema, BinaryRow row) {
        assert row.schemaVersion() == schema.version();

        this.row = row;
        this.schema = schema;
    }

    /**
     * @return Row schema.
     */
    public SchemaDescriptor rowSchema() {
        return schema;
    }

    /**
     * @return {@code True} if row has non-null value, {@code false} otherwise.
     */
    @Override public boolean hasValue() {
        return row.hasValue();
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public byte byteValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.BYTE);

        return off < 0 ? 0 : readByte(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Byte byteValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.BYTE);

        return off < 0 ? null : readByte(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public short shortValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.SHORT);

        return off < 0 ? 0 : readShort(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Short shortValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.SHORT);

        return off < 0 ? null : readShort(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public int intValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.INTEGER);

        return off < 0 ? 0 : readInteger(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Integer intValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.INTEGER);

        return off < 0 ? null : readInteger(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public long longValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.LONG);

        return off < 0 ? 0 : readLong(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Long longValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.LONG);

        return off < 0 ? null : readLong(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public float floatValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.FLOAT);

        return off < 0 ? 0.f : readFloat(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Float floatValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.FLOAT);

        return off < 0 ? null : readFloat(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public double doubleValue(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.DOUBLE);

        return off < 0 ? 0.d : readDouble(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Double doubleValueBoxed(int col) throws InvalidTypeException {
        long off = findColumn(col, NativeTypeSpec.DOUBLE);

        return off < 0 ? null : readDouble(offset(off));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public String stringValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.STRING);

        if (offLen < 0)
            return null;

        int off = offset(offLen);
        int len = length(offLen);

        return readString(off, len);
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public byte[] bytesValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.BYTES);

        if (offLen < 0)
            return null;

        int off = offset(offLen);
        int len = length(offLen);

        return readBytes(off, len);
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public UUID uuidValue(int col) throws InvalidTypeException {
        long found = findColumn(col, NativeTypeSpec.UUID);

        if (found < 0)
            return null;

        int off = offset(found);

        long lsb = readLong(off);
        long msb = readLong(off + 8);

        return new UUID(msb, lsb);
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public BitSet bitmaskValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.BITMASK);

        if (offLen < 0)
            return null;

        int off = offset(offLen);
        int len = columnLength(col);

        return BitSet.valueOf(readBytes(off, len));
    }

    /**
     * @return Row flags.
     */
    private boolean hasFlag(int flag) {
        return ((readShort(FLAGS_FIELD_OFFSET) & flag)) != 0;
    }

    /**
     * Gets the column offset and length encoded into a single 8-byte value (4 least significant bytes encoding the
     * offset from the beginning of the row and 4 most significant bytes encoding the field length for varlength
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
    protected long findColumn(int colIdx, NativeTypeSpec type) throws InvalidTypeException {
        // Get base offset (key start or value start) for the given column.
        boolean keyCol = schema.isKeyColumn(colIdx);

        final short flags = readShort(FLAGS_FIELD_OFFSET);

        int off = KEY_CHUNK_OFFSET;

        if (!keyCol) {
            assert (flags & RowFlags.NO_VALUE_FLAG) == 0;

            // Jump to the next chunk, the size of the first chunk is written at the chunk start.
            off += readInteger(off);

            // Adjust the column index according to the number of key columns.
            colIdx -= schema.keyColumns().length();
        }

        Columns cols = keyCol ? schema.keyColumns() : schema.valueColumns();

        if (cols.column(colIdx).type().spec() != type)
            throw new InvalidTypeException("Invalid column type requested [requested=" + type +
                ", column=" + cols.column(colIdx) + ']');

        boolean hasVarTable = ((keyCol ? RowFlags.OMIT_KEY_VARTBL_FLAG : RowFlags.OMIT_VAL_VARTBL_FLAG) & flags) == 0;
        boolean hasNullMap = ((keyCol ? RowFlags.OMIT_KEY_NULL_MAP_FLAG : RowFlags.OMIT_VAL_NULL_MAP_FLAG) & flags) == 0;

        if (hasNullMap && isNull(off, colIdx))
            return -1;

        assert hasVarTable || type.fixedLength();

        return type.fixedLength() ?
            fixlenColumnOffset(cols, off, colIdx, hasVarTable, hasNullMap) :
            varlenColumnOffsetAndLength(cols, off, colIdx, hasNullMap);
    }

    /**
     * @param colIdx Column index.
     * @return Column length.
     */
    private int columnLength(int colIdx) {
        Column col = schema.column(colIdx);

        return col.type().length();
    }

    /**
     * Checks the row's null map for the given column index in the chunk.
     *
     * @param baseOff Offset of the chunk start in the row.
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
     * offset is calculated from the beginning of the row.
     *
     * @param offLen {@code findColumn} invocation result.
     * @return Column offset from the beginning of the row.
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
     * @param hasNullMap Has null map flag.
     * @return Encoded offset (from the row start) and length of the column with the given index.
     */
    private long varlenColumnOffsetAndLength(Columns cols, int baseOff, int idx, boolean hasNullMap) {
        int vartableOff = baseOff + CHUNK_LEN_FIELD_SIZE;

        int numNullsBefore = 0;

        if (hasNullMap) {
            vartableOff += cols.nullMapSize();

            int nullMapOff = nullMapOffset(baseOff);

            int nullStartByte = cols.firstVarlengthColumn() / 8;
            int startBitInByte = cols.firstVarlengthColumn() % 8;

            int nullEndByte = idx / 8;
            int endBitInByte = idx % 8;

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
        }

        idx -= cols.numberOfFixsizeColumns() + numNullsBefore;
        int vartableSize = readShort(vartableOff);

        // Offset of idx-th column is from base offset.
        int resOff = readShort(vartableOff + varlenItemOffset(idx));

        long len = (idx == vartableSize - 1) ?
            // totalLength - columnStartOffset
            readInteger(baseOff) - resOff :
            // nextColumnStartOffset - columnStartOffset
            readShort(vartableOff + varlenItemOffset(idx + 1)) - resOff;

        return (len << 32) | (resOff + baseOff);
    }

    /**
     * Calculates the offset of the fixlen column with the given index in the row. It essentially folds the null map
     * with the column lengths to calculate the size of non-null columns preceding the requested column.
     *
     * @param cols Columns chunk.
     * @param baseOff Chunk base offset.
     * @param idx Column index in the chunk.
     * @param hasVarTbl Has varlen table flag.
     * @param hasNullMap Has null map flag.
     * @return Encoded offset (from the row start) of the requested fixlen column.
     */
    int fixlenColumnOffset(Columns cols, int baseOff, int idx, boolean hasVarTbl, boolean hasNullMap) {
        int off = 0;

        int payloadOff = baseOff + CHUNK_LEN_FIELD_SIZE;

        if (hasNullMap) {
            payloadOff += cols.nullMapSize();

            int nullMapOff = nullMapOffset(baseOff);

            int nullMapIdx = idx / 8;

            // Fold offset based on the whole map bytes in the schema
            for (int i = 0; i < nullMapIdx; i++)
                off += cols.foldFixedLength(i, readByte(nullMapOff + i));

            // Set bits starting from posInByte, inclusive, up to either the end of the byte or the last column index, inclusive
            int startBit = idx % 8;
            int endBit = nullMapIdx == cols.nullMapSize() - 1 ? ((cols.numberOfFixsizeColumns() - 1) % 8) : 7;
            int mask = (0xFF >> (7 - endBit)) & (0xFF << startBit);

            off += cols.foldFixedLength(nullMapIdx, readByte(nullMapOff + nullMapIdx) | mask);
        }

        if (hasVarTbl)
            payloadOff += varlenItemOffset(readShort(payloadOff));

        return payloadOff + off;
    }

    /**
     * @param baseOff Chunk base offset.
     * @return Null map offset from the row start for the chunk with the given base.
     */
    private int nullMapOffset(int baseOff) {
        return baseOff + CHUNK_LEN_FIELD_SIZE;
    }

    /** {@inheritDoc} */
    @Override public int schemaVersion() {
        return row.schemaVersion();
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        return row.hash();
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer keySlice() {
        return row.keySlice();
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer valueSlice() {
        return row.valueSlice();
    }

    /** {@inheritDoc} */
    @Override public void writeTo(OutputStream stream) throws IOException {
        row.writeTo(stream);
    }

    /** {@inheritDoc} */
    @Override public byte readByte(int off) {
        return row.readByte(off);
    }

    /** {@inheritDoc} */
    @Override public short readShort(int off) {
        return row.readShort(off);
    }

    /** {@inheritDoc} */
    @Override public int readInteger(int off) {
        return row.readInteger(off);
    }

    /** {@inheritDoc} */
    @Override public long readLong(int off) {
        return row.readLong(off);
    }

    /** {@inheritDoc} */
    @Override public float readFloat(int off) {
        return row.readFloat(off);
    }

    /** {@inheritDoc} */
    @Override public double readDouble(int off) {
        return row.readDouble(off);
    }

    /** {@inheritDoc} */
    @Override public String readString(int off, int len) {
        return row.readString(off, len);
    }

    /** {@inheritDoc} */
    @Override public byte[] readBytes(int off, int len) {
        return row.readBytes(off, len);
    }
}
