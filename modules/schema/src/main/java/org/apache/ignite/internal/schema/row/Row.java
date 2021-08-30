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

package org.apache.ignite.internal.schema.row;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.TemporalNativeType;

/**
 * Schema-aware row.
 * <p>
 * The class contains non-generic methods to read boxed and unboxed primitives based on the schema column types.
 * Any type conversions and coercions should be implemented outside the row by the key-value or query runtime.
 * <p>
 * When a non-boxed primitive is read from a null column value, it is converted to the primitive type default value.
 * <p>
 * Natively supported temporal types are decoded automatically after read.
 *
 * @see TemporalTypesHelper
 */
public class Row implements BinaryRow, SchemaAware {
    /** Schema descriptor. */
    protected final SchemaDescriptor schema;

    /** Binary row. */
    private final BinaryRow row;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param row Binary row representation.
     */
    public Row(SchemaDescriptor schema, BinaryRow row) {
        this.row = row;
        this.schema = schema;
    }

    /**
     * @return Row schema.
     */
    @Override public SchemaDescriptor schema() {
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
        long off = findColumn(col, NativeTypeSpec.INT8);

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
        long off = findColumn(col, NativeTypeSpec.INT8);

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
        long off = findColumn(col, NativeTypeSpec.INT16);

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
        long off = findColumn(col, NativeTypeSpec.INT16);

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
        long off = findColumn(col, NativeTypeSpec.INT32);

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
        long off = findColumn(col, NativeTypeSpec.INT32);

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
        long off = findColumn(col, NativeTypeSpec.INT64);

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
        long off = findColumn(col, NativeTypeSpec.INT64);

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
     * Reads value from specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public BigDecimal decimalValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.DECIMAL);

        if (offLen < 0)
            return null;

        int off = offset(offLen);
        int len = length(offLen);

        DecimalNativeType type = (DecimalNativeType)schema.column(col).type();

        byte[] bytes = readBytes(off, len);

        return new BigDecimal(new BigInteger(bytes), type.scale());
    }

    /**
     * Reads value from specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public BigInteger numberValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.NUMBER);

        if (offLen < 0)
            return null;

        int off = offset(offLen);
        int len = length(offLen);

        return new BigInteger(readBytes(off, len));
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
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public LocalDate dateValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.DATE);

        if (offLen < 0)
            return null;

        int off = offset(offLen);

        return readDate(off);
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public LocalTime timeValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.TIME);

        if (offLen < 0)
            return null;

        int off = offset(offLen);

        TemporalNativeType type = (TemporalNativeType)schema.column(col).type();

        return readTime(off, type);
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public LocalDateTime dateTimeValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.DATETIME);

        if (offLen < 0)
            return null;

        int off = offset(offLen);

        TemporalNativeType type = (TemporalNativeType)schema.column(col).type();

        return LocalDateTime.of(readDate(off), readTime(off + 3, type));
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     * @throws InvalidTypeException If actual column type does not match the requested column type.
     */
    public Instant timestampValue(int col) throws InvalidTypeException {
        long offLen = findColumn(col, NativeTypeSpec.TIMESTAMP);

        if (offLen < 0)
            return null;

        int off = offset(offLen);

        TemporalNativeType type = (TemporalNativeType)schema.column(col).type();

        long seconds = readLong(off);
        int nanos = 0;

        if (type.precision() != 0)
            nanos = readInteger(off + 8);

        return Instant.ofEpochSecond(seconds, nanos);
    }

    /**
     * Reads and decode time column value.
     *
     * @param off Offset
     * @param type Temporal type precision.
     * @return LocalTime value.
     */
    private LocalTime readTime(int off, TemporalNativeType type) {
        long time = Integer.toUnsignedLong(readInteger(off));

        if (type.precision() > 3) {
            time <<= 16;
            time |= Short.toUnsignedLong(readShort(off + 4));
            time = (time >>> TemporalTypesHelper.NANOSECOND_PART_LEN) << 32 | (time & TemporalTypesHelper.NANOSECOND_PART_MASK);
        }
        else // Decompress
            time = (time >>> TemporalTypesHelper.MILLISECOND_PART_LEN) << 32 | (time & TemporalTypesHelper.MILLISECOND_PART_MASK);

        return TemporalTypesHelper.decodeTime(type, time);
    }

    /**
     * Reads and decode date column value.
     *
     * @param off Offset
     * @return LocalDate value.
     */
    private LocalDate readDate(int off) {
        int date = Short.toUnsignedInt(readShort(off)) << 8;
        date |= Byte.toUnsignedInt(readByte(off + 2));

        return TemporalTypesHelper.decodeDate(date);
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
     * @return {@code -1} if value is {@code null} for a column,
     * or {@link Long#MAX_VALUE} if column is unknown,
     * otherwise encoded offset + length of the column.
     * @see #offset(long)
     * @see #length(long)
     * @see InvalidTypeException If actual column type does not match the requested column type.
     * @see org.apache.ignite.internal.schema.registry.UpgradingRowAdapter
     */
    protected long findColumn(int colIdx, NativeTypeSpec type) throws InvalidTypeException {
        // Get base offset (key start or value start) for the given column.
        boolean isKeyCol = schema.isKeyColumn(colIdx);

        Columns cols;
        int chunkBaseOff;
        int flags;

        if (isKeyCol) {
            cols = schema.keyColumns();

            chunkBaseOff = KEY_CHUNK_OFFSET;

            flags = keyFlags();
        }
        else {        // Adjust the column index according to the number of key columns.
            if (!hasValue())
                throw new IllegalStateException("Row has no value.");

            colIdx -= schema.keyColumns().length();

            cols = schema.valueColumns();

            chunkBaseOff = KEY_CHUNK_OFFSET + readInteger(KEY_CHUNK_OFFSET);

            flags = valueFlags();
        }

        if (cols.column(colIdx).type().spec() != type)
            throw new InvalidTypeException("Invalid column type requested [requested=" + type +
                ", column=" + cols.column(colIdx) + ']');

        int nullMapLen = (flags & VarTableFormat.OMIT_NULL_MAP_FLAG) == 0 ? cols.nullMapSize() : 0;

        VarTableFormat format = (flags & VarTableFormat.OMIT_VARTBL_FLAG) == 0 ? VarTableFormat.fromFlags(flags) : null;

        if (nullMapLen > 0 && isNull(chunkBaseOff, colIdx))
            return -1;

        int dataOffset = varTableOffset(chunkBaseOff, nullMapLen);

        if (format != null)
            dataOffset += format.vartableLength(format.readVartableSize(row, dataOffset));

        return type.fixedLength() ?
            fixedSizeColumnOffset(chunkBaseOff, dataOffset, cols, colIdx, nullMapLen > 0) :
            varlenColumnOffsetAndLength(chunkBaseOff, dataOffset, cols, colIdx, nullMapLen, format);
    }

    /**
     * Calculates the offset of the fixed-size column with the given index in the row. It essentially folds the null-map
     * with the column lengths to calculate the size of non-null columns preceding the requested column.
     *
     * @param chunkBaseOff Chunk base offset.
     * @param dataOffset Chunk data offset.
     * @param cols Columns chunk.
     * @param idx Column index in the chunk.
     * @param hasNullmap {@code true} if chunk has null-map, {@code false} otherwise.
     * @return Encoded offset (from the row start) of the requested fixlen column.
     */
    int fixedSizeColumnOffset(int chunkBaseOff, int dataOffset, Columns cols, int idx, boolean hasNullmap) {
        int colOff = 0;

        // Calculate fixlen column offset.
        int colByteIdx = idx >> 3; // Equivalent expression for: idx / 8

        // Set bits starting from posInByte, inclusive, up to either the end of the byte or the last column index, inclusive
        int startBit = idx & 7; // Equivalent expression for: idx % 8
        int endBit = (colByteIdx == (cols.length() + 7) >> 3 - 1) /* last byte */ ?
            ((cols.numberOfFixsizeColumns() - 1) & 7) : 7; // Equivalent expression for: (expr) % 8
        int mask = (0xFF >> (7 - endBit)) & (0xFF << startBit);

        if (hasNullmap) {
            // Fold offset based on the whole map bytes in the schema
            for (int i = 0; i < colByteIdx; i++)
                colOff += cols.foldFixedLength(i, Byte.toUnsignedInt(row.readByte(nullMapOffset(chunkBaseOff) + i)));

            colOff += cols.foldFixedLength(colByteIdx, Byte.toUnsignedInt(row.readByte(nullMapOffset(chunkBaseOff) + colByteIdx)) | mask);
        }
        else {
            for (int i = 0; i < colByteIdx; i++)
                colOff += cols.foldFixedLength(i, 0);

            colOff += cols.foldFixedLength(colByteIdx, mask);
        }

        return dataOffset + colOff;
    }

    /**
     * Calculates the offset and length of varlen column. First, it calculates the number of non-null columns
     * preceding the requested column by folding the null-map bits. This number is used to adjust the column index
     * and find the corresponding entry in the varlen table. The length of the column is calculated either by
     * subtracting two adjacent varlen table offsets, or by subtracting the last varlen table offset from the chunk
     * length.
     * <p>
     * Note: Offset for the very fisrt varlen is skipped in vartable and calculated from fixlen columns sizes.
     *
     * @param baseOff Chunk base offset.
     * @param dataOff Chunk data offset.
     * @param cols Columns chunk.
     * @param idx Column index in the chunk.
     * @param nullMapLen Null-map length or {@code 0} if null-map is omitted.
     * @param format Vartable format helper or {@code null} if vartable is omitted.
     * @return Encoded offset (from the row start) and length of the column with the given index.
     */
    long varlenColumnOffsetAndLength(
        int baseOff,
        int dataOff,
        Columns cols,
        int idx,
        int nullMapLen,
        VarTableFormat format
    ) {
        assert cols.hasVarlengthColumns() && cols.firstVarlengthColumn() <= idx : "Invalid varlen column index: colId=" + idx;

        if (nullMapLen > 0) { // Calculates fixlen columns chunk size regarding the 'null' flags.
            int nullStartByte = cols.firstVarlengthColumn() >> 3; // Equivalent expression for: idx / 8
            int startBitInByte = cols.firstVarlengthColumn() & 7; // Equivalent expression for: (expr) % 8

            int nullEndByte = idx >> 3; // Equivalent expression for: idx / 8
            int endBitInByte = idx & 7; // Equivalent expression for: idx % 8

            int numNullsBefore = 0;

            for (int i = nullStartByte; i <= nullEndByte; i++) {
                byte nullmapByte = row.readByte(nullMapOffset(baseOff) + i);

                if (i == nullStartByte)
                    // We need to clear startBitInByte least significant bits
                    nullmapByte &= (0xFF << startBitInByte);

                if (i == nullEndByte)
                    // We need to clear 8-endBitInByte most significant bits
                    nullmapByte &= (0xFF >> (8 - endBitInByte));

                numNullsBefore += Columns.numberOfNullColumns(nullmapByte);
            }

            idx -= numNullsBefore;
        }

        idx -= cols.numberOfFixsizeColumns();

        // Calculate length and offset for very first (non-null) varlen column
        // as vartable don't store the offset for the first varlen.
        if (idx == 0) {
            int off = cols.numberOfFixsizeColumns() == 0 ? dataOff : fixedSizeColumnOffset(baseOff, dataOff, cols, cols.numberOfFixsizeColumns(), nullMapLen > 0);

            long len = format != null ? // Length is either diff between current offset and next varlen offset or end-of-chunk.
                dataOff + format.readVarlenOffset(row, varTableOffset(baseOff, nullMapLen), 0) - off :
                (baseOff + chunkLength(baseOff)) - off;

            return (len << 32) | off;
        }

        final int varTblOff = varTableOffset(baseOff, nullMapLen);
        final int vartblSize = format.readVartableSize(row, varTblOff);

        assert idx > 0 && vartblSize >= idx : "Vartable index is out of bound: colId=" + idx;

        // Offset of idx-th column is from base offset.
        int resOff = dataOff + format.readVarlenOffset(row, varTblOff, idx - 1);

        long len = (vartblSize == idx) ?
            // totalLength - columnStartOffset
            (baseOff + chunkLength(baseOff)) - resOff :
            // nextColumnStartOffset - columnStartOffset
            dataOff + format.readVarlenOffset(row, varTblOff, idx) - resOff;

        return (len << 32) | resOff;
    }

    /**
     * @param baseOff Chunk base offset.
     * @return Chunk length.
     */
    private int chunkLength(int baseOff) {
        return readInteger(baseOff);
    }

    /**
     * @param baseOff Chunk base offset.
     * @param nullMapLen Null-map length.
     * @return Vartable offset.
     */
    private int varTableOffset(int baseOff, int nullMapLen) {
        return baseOff + BinaryRow.CHUNK_LEN_FLD_SIZE + nullMapLen;
    }

    /**
     * Checks the row's null-map for the given column index in the chunk.
     *
     * @param baseOff Chunk base offset.
     * @param idx Offset of the column in the chunk.
     * @return {@code true} if the column value is {@code null}, {@code false} otherwise.
     */
    protected boolean isNull(int baseOff, int idx) {
        int nullByte = idx >> 3; // Equivalent expression for: idx / 8
        int posInByte = idx & 7; // Equivalent expression for: idx % 8

        int map = row.readByte(baseOff + BinaryRow.CHUNK_LEN_FLD_SIZE + nullByte) & 0xFF;

        return (map & (1 << posInByte)) != 0;
    }

    /**
     * @return Key flags.
     */
    private int keyFlags() {
        short flags = readShort(FLAGS_FIELD_OFFSET);

        return (flags >> RowFlags.KEY_FLAGS_OFFSET) & RowFlags.CHUNK_FLAGS_MASK;
    }

    /**
     * @return Value flags.
     */
    private int valueFlags() {
        short flags = readShort(FLAGS_FIELD_OFFSET);

        return (flags >> RowFlags.VAL_FLAGS_OFFSET) & RowFlags.CHUNK_FLAGS_MASK;
    }

    /**
     * @param baseOff Chunk base offset.
     * @return Null-map offset.
     */
    int nullMapOffset(int baseOff) {
        return baseOff + BinaryRow.CHUNK_LEN_FLD_SIZE;
    }

    /**
     * @param colIdx Column index.
     * @return Column length.
     */
    private int columnLength(int colIdx) {
        Column col = schema.column(colIdx);

        return col.type().sizeInBytes();
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

    @Override public byte[] bytes() {
        return row.bytes();
    }
}
