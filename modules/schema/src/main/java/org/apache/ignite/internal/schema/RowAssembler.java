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

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow.RowFlags;

import static org.apache.ignite.internal.schema.BinaryRow.VARLEN_COLUMN_OFFSET_FIELD_SIZE;
import static org.apache.ignite.internal.schema.BinaryRow.VARLEN_TABLE_SIZE_FIELD_SIZE;

/**
 * Utility class to build rows using column appending pattern. The external user of this class must consult
 * with the schema and provide the columns in strict internal column sort order during the row construction.
 * Additionally, the user of this class should pre-calculate the resulting row size when possible to avoid
 * unnecessary data copies. The assembler provides some utility methods to calculate the resulting row size
 * based on the number of null columns and size calculation for strings.
 *
 * @see #rowSize(Columns, int, int, Columns, int, int)
 * @see #rowChunkSize(Columns, int, int)
 * @see #utf8EncodedLength(CharSequence)
 */
public class RowAssembler {
    /** Schema. */
    private final SchemaDescriptor schema;

    /** The number of non-null varlen columns in values chunk. */
    private final int nonNullVarlenValCols;

    /** Target byte buffer to write to. */
    private final ExpandableByteBuf buf;

    /** Current columns chunk. */
    private Columns curCols;

    /** Current field index (the field is unset). */
    private int curCol;

    /** Index of the current varlen table entry. Incremented each time non-null varlen column is appended. */
    private int curVarlenTblEntry;

    /** Current offset for the next column to be appended. */
    private int curOff;

    /** Base offset of the current chunk */
    private int baseOff;

    /** Offset of the null map for current chunk. */
    private int nullMapOff;

    /** Offset of the varlen table for current chunk. */
    private int varlenTblChunkOff;

    /** Flags. */
    private short flags;

    /** Charset encoder for strings. Initialized lazily. */
    private CharsetEncoder strEncoder;

    /**
     * @param nonNullVarlenCols Number of non-null varlen columns.
     * @return Total size of the varlen table.
     */
    public static int varlenTableChunkSize(int nonNullVarlenCols) {
        return nonNullVarlenCols == 0 ? 0 :
            VARLEN_TABLE_SIZE_FIELD_SIZE + nonNullVarlenCols * VARLEN_COLUMN_OFFSET_FIELD_SIZE;
    }

    /**
     * Calculates encoded string length.
     *
     * @param seq Char sequence.
     * @return Encoded string length.
     * @implNote This implementation is not tolerant to malformed char sequences.
     */
    public static int utf8EncodedLength(CharSequence seq) {
        int cnt = 0;

        for (int i = 0, len = seq.length(); i < len; i++) {
            char ch = seq.charAt(i);

            if (ch <= 0x7F)
                cnt++;
            else if (ch <= 0x7FF)
                cnt += 2;
            else if (Character.isHighSurrogate(ch)) {
                cnt += 4;
                ++i;
            }
            else
                cnt += 3;
        }

        return cnt;
    }

    /**
     * @param keyCols Key columns.
     * @param nonNullVarlenKeyCols Number of non-null varlen columns in key chunk.
     * @param nonNullVarlenKeySize Size of non-null varlen columns in key chunk.
     * @param valCols Value columns.
     * @param nonNullVarlenValCols Number of non-null varlen columns in value chunk.
     * @param nonNullVarlenValSize Size of non-null varlen columns in value chunk.
     * @return Total row size.
     */
    public static int rowSize(
        Columns keyCols,
        int nonNullVarlenKeyCols,
        int nonNullVarlenKeySize,
        Columns valCols,
        int nonNullVarlenValCols,
        int nonNullVarlenValSize
    ) {
        return BinaryRow.KEY_CHUNK_OFFSET /* Header size */ +
            rowChunkSize(keyCols, nonNullVarlenKeyCols, nonNullVarlenKeySize) +
            rowChunkSize(valCols, nonNullVarlenValCols, nonNullVarlenValSize);
    }

    /**
     * @param cols Columns.
     * @param nonNullVarlenCols Number of non-null varlen columns in chunk.
     * @param nonNullVarlenSize Size of non-null varlen columns in chunk.
     * @return Row's chunk size.
     */
    static int rowChunkSize(Columns cols, int nonNullVarlenCols, int nonNullVarlenSize) {
        int size = BinaryRow.CHUNK_LEN_FIELD_SIZE + cols.nullMapSize() +
            varlenTableChunkSize(nonNullVarlenCols);

        for (int i = 0; i < cols.numberOfFixsizeColumns(); i++)
            size += cols.column(i).type().length();

        return size + nonNullVarlenSize;
    }

    /**
     * @param schema Row schema.
     * @param size Target row size. If the row size is known in advance, it should be provided upfront to avoid
     * unnecessary arrays copy.
     * @param nonNullVarlenKeyCols Number of non-null varlen columns in key chunk.
     * @param nonNullVarlenValCols Number of non-null varlen columns in value chunk.
     */
    public RowAssembler(
        SchemaDescriptor schema,
        int size,
        int nonNullVarlenKeyCols,
        int nonNullVarlenValCols
    ) {
        this.schema = schema;
        this.nonNullVarlenValCols = nonNullVarlenValCols;

        curCols = schema.keyColumns();
        flags = 0;
        strEncoder = null;

        initOffsets(BinaryRow.KEY_CHUNK_OFFSET, nonNullVarlenKeyCols);

        if (schema.keyColumns().nullMapSize() == 0)
            flags |= RowFlags.OMIT_KEY_NULL_MAP_FLAG;

        if (schema.valueColumns().nullMapSize() == 0)
            flags |= RowFlags.OMIT_VAL_NULL_MAP_FLAG;

        buf = new ExpandableByteBuf(size);

        buf.putShort(0, (short)schema.version());

        if (nonNullVarlenKeyCols == 0)
            flags |= RowFlags.OMIT_KEY_VARTBL_FLAG;
        else
            buf.putShort(varlenTblChunkOff, (short)nonNullVarlenKeyCols);
    }

    /**
     * Appends {@code null} value for the current column to the chunk.
     */
    public void appendNull() {
        Column col = curCols.column(curCol);

        if (!col.nullable())
            throw new IllegalArgumentException("Failed to set column (null was passed, but column is not nullable): " +
                col);

        setNull(curCol);

        shiftColumn(0, false);
    }

    /**
     * Appends byte value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendByte(byte val) {
        checkType(NativeType.BYTE);

        buf.put(curOff, val);

        shiftColumn(NativeType.BYTE);
    }

    /**
     * Appends short value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendShort(short val) {
        checkType(NativeType.SHORT);

        buf.putShort(curOff, val);

        shiftColumn(NativeType.SHORT);
    }

    /**
     * Appends int value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendInt(int val) {
        checkType(NativeType.INTEGER);

        buf.putInt(curOff, val);

        shiftColumn(NativeType.INTEGER);
    }

    /**
     * Appends long value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendLong(long val) {
        checkType(NativeType.LONG);

        buf.putLong(curOff, val);

        shiftColumn(NativeType.LONG);
    }

    /**
     * Appends float value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendFloat(float val) {
        checkType(NativeType.FLOAT);

        buf.putFloat(curOff, val);

        shiftColumn(NativeType.FLOAT);
    }

    /**
     * Appends double value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendDouble(double val) {
        checkType(NativeType.DOUBLE);

        buf.putDouble(curOff, val);

        shiftColumn(NativeType.DOUBLE);
    }

    /**
     * Appends UUID value for the current column to the chunk.
     *
     * @param uuid Column value.
     */
    public void appendUuid(UUID uuid) {
        checkType(NativeType.UUID);

        buf.putLong(curOff, uuid.getLeastSignificantBits());
        buf.putLong(curOff + 8, uuid.getMostSignificantBits());

        shiftColumn(NativeType.UUID);
    }

    /**
     * Appends String value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendString(String val) {
        checkType(NativeType.STRING);

        try {
            int written = buf.putString(curOff, val, encoder());

            writeOffset(curVarlenTblEntry, curOff - baseOff);

            shiftColumn(written, true);
        }
        catch (CharacterCodingException e) {
            throw new AssemblyException("Failed to encode string", e);
        }
    }

    /**
     * Appends byte[] value for the current column to the chunk.
     *
     * @param val Column value.
     */
    public void appendBytes(byte[] val) {
        checkType(NativeType.BYTES);

        buf.putBytes(curOff, val);

        writeOffset(curVarlenTblEntry, curOff - baseOff);

        shiftColumn(val.length, true);
    }

    /**
     * Appends BitSet value for the current column to the chunk.
     *
     * @param bitSet Column value.
     */
    public void appendBitmask(BitSet bitSet) {
        Column col = curCols.column(curCol);

        checkType(NativeTypeSpec.BITMASK);

        Bitmask maskType = (Bitmask)col.type();

        if (bitSet.length() > maskType.bits())
            throw new IllegalArgumentException("Failed to set bitmask for column '" + col.name() + "' " +
                "(mask size exceeds allocated size) [mask=" + bitSet + ", maxSize=" + maskType.bits() + "]");

        byte[] arr = bitSet.toByteArray();

        buf.putBytes(curOff, arr);

        for (int i = 0; i < maskType.length() - arr.length; i++)
            buf.put(curOff + arr.length + i, (byte)0);

        shiftColumn(maskType);
    }

    /**
     * @return Serialized row.
     */
    public byte[] build() {
        if (schema.keyColumns() == curCols)
            throw new AssemblyException("Key column missed: colIdx=" + curCol);
        else {
            if (curCol == 0)
                flags |= RowFlags.NO_VALUE_FLAG;
            else if (schema.valueColumns().length() != curCol)
                throw new AssemblyException("Value column missed: colIdx=" + curCol);
        }

        buf.putShort(BinaryRow.FLAGS_FIELD_OFFSET, flags);

        return buf.toArray();
    }

    /**
     * @return UTF-8 string encoder.
     */
    private CharsetEncoder encoder() {
        if (strEncoder == null)
            strEncoder = StandardCharsets.UTF_8.newEncoder();

        return strEncoder;
    }

    /**
     * Writes the given offset to the varlen table entry with the given index.
     *
     * @param tblEntryIdx Varlen table entry index.
     * @param off Offset to write.
     */
    private void writeOffset(int tblEntryIdx, int off) {
        assert (flags & (baseOff == BinaryRow.KEY_CHUNK_OFFSET ? RowFlags.OMIT_KEY_VARTBL_FLAG : RowFlags.OMIT_VAL_VARTBL_FLAG)) == 0;

        buf.putShort(varlenTblChunkOff + Row.varlenItemOffset(tblEntryIdx), (short)off);
    }

    /**
     * Checks that the type being appended matches the column type.
     *
     * @param type Type spec that is attempted to be appended.
     */
    private void checkType(NativeTypeSpec type) {
        Column col = curCols.column(curCol);

        if (col.type().spec() != type)
            throw new IllegalArgumentException("Failed to set column (int was passed, but column is of different " +
                "type): " + col);
    }

    /**
     * Checks that the type being appended matches the column type.
     *
     * @param type Type that is attempted to be appended.
     */
    private void checkType(NativeType type) {
        checkType(type.spec());
    }

    /**
     * Sets null flag in the null map for the given column.
     *
     * @param colIdx Column index.
     */
    private void setNull(int colIdx) {
        assert (flags & (baseOff == BinaryRow.KEY_CHUNK_OFFSET ? RowFlags.OMIT_KEY_NULL_MAP_FLAG : RowFlags.OMIT_VAL_NULL_MAP_FLAG)) == 0;

        int byteInMap = colIdx / 8;
        int bitInByte = colIdx % 8;

        buf.ensureCapacity(nullMapOff + byteInMap + 1);

        buf.put(nullMapOff + byteInMap, (byte)(buf.get(nullMapOff + byteInMap) | (1 << bitInByte)));
    }

    /**
     * Must be called after an append of fixlen column.
     *
     * @param type Type of the appended column.
     */
    private void shiftColumn(NativeType type) {
        assert type.spec().fixedLength() : "Varlen types should provide field length to shift column: " + type;

        shiftColumn(type.length(), false);
    }

    /**
     * Shifts current offsets and column indexes as necessary, also changes the chunk base offsets when
     * moving from key to value columns.
     *
     * @param size Size of the appended column.
     * @param varlen {@code true} if appended column was varlen.
     */
    private void shiftColumn(int size, boolean varlen) {
        curCol++;
        curOff += size;

        if (varlen)
            curVarlenTblEntry++;

        if (curCol == curCols.length()) {
            int chunkLen = curOff - baseOff;

            buf.putInt(baseOff, chunkLen);

            if (schema.valueColumns() == curCols)
                return; // No more columns.

            curCols = schema.valueColumns(); // Switch key->value columns.

            initOffsets(baseOff + chunkLen, nonNullVarlenValCols);

            if (nonNullVarlenValCols == 0)
                flags |= RowFlags.OMIT_VAL_VARTBL_FLAG;
            else
                buf.putShort(varlenTblChunkOff, (short)nonNullVarlenValCols);
        }
    }

    /**
     * @param base Chunk base offset.
     * @param nonNullVarlenCols Number of non-null varlen columns.
     */
    private void initOffsets(int base, int nonNullVarlenCols) {
        baseOff = base;

        curCol = 0;
        curVarlenTblEntry = 0;

        nullMapOff = baseOff + BinaryRow.CHUNK_LEN_FIELD_SIZE;
        varlenTblChunkOff = nullMapOff + curCols.nullMapSize();

        curOff = varlenTblChunkOff + varlenTableChunkSize(nonNullVarlenCols);
    }
}
