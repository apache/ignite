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

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.util.Constants;

/**
 * Vartable format helper provides methods for reading/writing the chunk vartable content.
 *
 * Once total data size of the chunk is known then vartable could be (re)written in a compact way
 * using proper format {{@link #compactVarTable(ExpandableByteBuf, int, int)}}.
 * The vartable format is coded into chunk flags.
 *
 * @see #format(int, int)
 * @see #fromFlags(int)
 */
abstract class VarTableFormat {
    /** First two flag bits reserved for format code. */
    public static final int FORMAT_CODE_MASK = 0x03;

    /** Flag indicates key chunk omits vartable. */
    public static final int OMIT_NULL_MAP_FLAG = 1 << 2;

    /** Flag indicates value chunk omits null map. */
    public static final int OMIT_VARTBL_FLAG = 1 << 3;

    /** Writer factory for tiny-sized chunks. */
    static final VarTableFormat TINY = new TinyFormat();

    /** Writer factory for med-sized chunks. */
    static final VarTableFormat MEDIUM = new MediumFormat();

    /** Writer factory for large-sized chunks. */
    static final VarTableFormat LARGE = new LargeFormat();

    /**
     * Return vartable format helper for data of given size
     * to write vartable in a compact way.
     *
     * @param payloadLen Payload size in bytes.
     * @param vartblSize Number of items in the vartable.
     * @return Vartable format helper.
     */
    static VarTableFormat format(int payloadLen, int vartblSize) {
        if (payloadLen > 0) {
            if (payloadLen < 256 && vartblSize < 256)
                return TINY;

            if (payloadLen < 64 * Constants.KiB)
                return MEDIUM;
        }

        return LARGE;
    }

    /**
     * Returns vartable format helper depending on chunk flags.
     *
     * @param chunkFlags Chunk specific flags. Only first 4-bits are meaningful.
     * @return Vartable format helper.
     */
    public static VarTableFormat fromFlags(int chunkFlags) {
        int formatId = chunkFlags & FORMAT_CODE_MASK;

        switch (formatId) {
            case TinyFormat.FORMAT_ID:
                return TINY;
            case MediumFormat.FORMAT_ID:
                return MEDIUM;
            default:
                return LARGE;
        }
    }

    /** Size of vartable entry. */
    private final int vartblEntrySize;

    /** Size of cartable size field. */
    private final int vartblSizeFieldSize;

    /** Format id. */
    private final byte formatId;

    /**
     * @param vartblSizeFieldSize Size of vartalble size field (in bytes).
     * @param vartblEntrySize Size of vartable entry (in bytes).
     * @param formatId Format specific flags.
     */
    VarTableFormat(int vartblSizeFieldSize, int vartblEntrySize, byte formatId) {
        this.vartblEntrySize = vartblEntrySize;
        this.vartblSizeFieldSize = vartblSizeFieldSize;
        this.formatId = formatId;
    }

    /**
     * @return Format id.
     */
    public byte formatId() {
        return formatId;
    }

    /**
     * Calculates vartable size in bytes.
     *
     * @param entries Vartable entries.
     * @return Vartable size in bytes.
     */
    int vartableLength(int entries) {
        return entries <= 0 ? 0 : vartblSizeFieldSize /* Table size */ + entries * vartblEntrySize;
    }

    /**
     * Calculates vartable entry offset.
     *
     * @param idx Vartable entry idx.
     * @return Vartable entry offset.
     */
    int vartableEntryOffset(int idx) {
        return vartblSizeFieldSize /* Table size */ + idx * vartblEntrySize;
    }

    /**
     * Reads varlen offset from vartable.
     *
     * @param row Row.
     * @param vartblOff Vartable offset.
     * @param entryIdx Vartable entry index.
     * @return Varlen offset.
     */
    abstract int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx);

    /**
     * Reads vartable size.
     *
     * @param row Row.
     * @param vartblOff Vartable offset.
     * @return Number of entries in the vartable.
     */
    abstract int readVartableSize(BinaryRow row, int vartblOff);

    /**
     * Convert vartable inplace to the current format.
     *
     * @param buf Row buffer.
     * @param vartblOff Vartable offset.
     * @param entries Number of entries in the vartable.
     * @return Number of bytes vartable was shrinked by.
     */
    public abstract int compactVarTable(ExpandableByteBuf buf, int vartblOff, int entries);

    /**
     * Chunk format for small rows (with payload size less 256 bytes).
     */
    private static class TinyFormat extends VarTableFormat {
        private static final byte FORMAT_ID = 1;

        /**
         * Creates chunk format.
         */
        TinyFormat() {
            super(Byte.BYTES, Byte.BYTES, FORMAT_ID);
        }

        /** {@inheritDoc} */
        @Override int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx) {
            return Byte.toUnsignedInt(row.readByte(vartblOff + vartableEntryOffset(entryIdx)));
        }

        /** {@inheritDoc} */
        @Override int readVartableSize(BinaryRow row, int vartblOff) {
            return Byte.toUnsignedInt(row.readByte(vartblOff));
        }

        /** {@inheritDoc} */
        @Override public int compactVarTable(ExpandableByteBuf buf, int vartblOff, int entres) {
            assert entres > 0 && entres < 0xFFFF;

            buf.put(vartblOff, (byte)entres);

            int dstOff = vartblOff + Byte.BYTES;
            int srcOff = vartblOff + Short.BYTES;

            for (int i = 0; i < entres; i++, srcOff += Integer.BYTES, dstOff++)
                buf.put(dstOff, buf.get(srcOff));

            buf.shift(srcOff, dstOff);

            return srcOff - dstOff;
        }
    }

    /**
     * Chunk format for rows of medium size (with payload size up to 64Kb).
     */
    private static class MediumFormat extends VarTableFormat {
        private static final byte FORMAT_ID = 2;

        /**
         * Creates chunk format.
         */
        MediumFormat() {
            super(Short.BYTES, Short.BYTES, FORMAT_ID);
        }

        /** {@inheritDoc} */
        @Override int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx) {
            return Short.toUnsignedInt(row.readShort(vartblOff + vartableEntryOffset(entryIdx)));
        }

        /** {@inheritDoc} */
        @Override int readVartableSize(BinaryRow row, int vartblOff) {
            return Short.toUnsignedInt(row.readShort(vartblOff));
        }

        /** {@inheritDoc} */
        @Override public int compactVarTable(ExpandableByteBuf buf, int vartblOff, int entries) {
            buf.putShort(vartblOff, (short)entries);

            int dstOff = vartblOff + Short.BYTES;
            int srcOff = vartblOff + Short.BYTES;

            for (int i = 0; i < entries; i++, srcOff += Integer.BYTES, dstOff += Short.BYTES)
                buf.putShort(dstOff, buf.getShort(srcOff));

            buf.shift(srcOff, dstOff);

            return srcOff - dstOff;
        }
    }

    /**
     * Chunk format for large rows (with payload size 64+Kb).
     */
    private static class LargeFormat extends VarTableFormat {
        private static byte FORMAT_ID = 0;

        /**
         * Creates chunk format.
         */
        LargeFormat() {
            super(Short.BYTES, Integer.BYTES, FORMAT_ID);
        }

        /** {@inheritDoc} */
        @Override int readVarlenOffset(BinaryRow row, int vartblOff, int entryIdx) {
            return row.readInteger(vartblOff + vartableEntryOffset(entryIdx));
        }

        /** {@inheritDoc} */
        @Override int readVartableSize(BinaryRow row, int vartblOff) {
            return Short.toUnsignedInt(row.readShort(vartblOff));
        }

        /** {@inheritDoc} */
        @Override public int compactVarTable(ExpandableByteBuf buf, int vartblOff, int entries) {
            buf.putShort(vartblOff, (short)entries);

            return 0; // Nothing to do.
        }
    }
}
