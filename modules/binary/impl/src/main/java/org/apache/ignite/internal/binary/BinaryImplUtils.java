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
package org.apache.ignite.internal.binary;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.binary.BinaryUtils.length;

/**
 * Binary utils used only in implementation.
 */
public class BinaryImplUtils {
    /** Flag: user type. */
    static final short FLAG_USR_TYP = 0x0001;

    /** Flag: only raw data exists. */
    static final short FLAG_HAS_SCHEMA = 0x0002;

    /** Flag indicating that object has raw data. */
    static final short FLAG_HAS_RAW = 0x0004;

    /** Flag: offsets take 1 byte. */
    static final short FLAG_OFFSET_ONE_BYTE = 0x0008;

    /** Flag: offsets take 2 bytes. */
    static final short FLAG_OFFSET_TWO_BYTES = 0x0010;

    /** Flag: compact footer, no field IDs. */
    public static final short FLAG_COMPACT_FOOTER = 0x0020;

    /** Flag: raw data contains .NET type information. Always 0 in Java. Keep it here for information only. */
    @SuppressWarnings("unused")
    public static final short FLAG_CUSTOM_DOTNET_TYPE = 0x0040;

    /** Offset which fits into 1 byte. */
    static final int OFFSET_1 = 1;

    /** Offset which fits into 2 bytes. */
    static final int OFFSET_2 = 2;

    /** Offset which fits into 4 bytes. */
    static final int OFFSET_4 = 4;

    /** Field ID length. */
    static final int FIELD_ID_LEN = 4;

    /** {@code true} if serialized value of this type cannot contain references to objects. */
    private static final boolean[] PLAIN_TYPE_FLAG = new boolean[102];

    static {
        for (byte b : new byte[] {
            GridBinaryMarshaller.BYTE, GridBinaryMarshaller.SHORT, GridBinaryMarshaller.INT, GridBinaryMarshaller.LONG,
            GridBinaryMarshaller.FLOAT, GridBinaryMarshaller.DOUBLE, GridBinaryMarshaller.CHAR, GridBinaryMarshaller.BOOLEAN,
            GridBinaryMarshaller.DECIMAL, GridBinaryMarshaller.STRING, GridBinaryMarshaller.UUID, GridBinaryMarshaller.DATE,
            GridBinaryMarshaller.TIMESTAMP, GridBinaryMarshaller.TIME, GridBinaryMarshaller.BYTE_ARR, GridBinaryMarshaller.SHORT_ARR,
            GridBinaryMarshaller.INT_ARR, GridBinaryMarshaller.LONG_ARR, GridBinaryMarshaller.FLOAT_ARR, GridBinaryMarshaller.DOUBLE_ARR,
            GridBinaryMarshaller.TIME_ARR, GridBinaryMarshaller.CHAR_ARR, GridBinaryMarshaller.BOOLEAN_ARR,
            GridBinaryMarshaller.DECIMAL_ARR, GridBinaryMarshaller.STRING_ARR, GridBinaryMarshaller.UUID_ARR, GridBinaryMarshaller.DATE_ARR,
            GridBinaryMarshaller.TIMESTAMP_ARR, GridBinaryMarshaller.ENUM, GridBinaryMarshaller.ENUM_ARR, GridBinaryMarshaller.NULL}) {

            PLAIN_TYPE_FLAG[b] = true;
        }
    }

    /**
     * Check if user type flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    static boolean isUserType(short flags) {
        return isFlagSet(flags, FLAG_USR_TYP);
    }

    /**
     * Check if raw-only flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    public static boolean hasSchema(short flags) {
        return isFlagSet(flags, FLAG_HAS_SCHEMA);
    }

    /**
     * Check if raw-only flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    static boolean hasRaw(short flags) {
        return isFlagSet(flags, FLAG_HAS_RAW);
    }

    /**
     * Check if "no-field-ids" flag is set.
     *
     * @param flags Flags.
     * @return {@code True} if set.
     */
    static boolean isCompactFooter(short flags) {
        return isFlagSet(flags, FLAG_COMPACT_FOOTER);
    }

    /**
     * Check whether particular flag is set.
     *
     * @param flags Flags.
     * @param flag Flag.
     * @return {@code True} if flag is set in flags.
     */
    static boolean isFlagSet(short flags, short flag) {
        return (flags & flag) == flag;
    }

    /** */
    static int dataStartRelative(BinaryPositionReadable in, int start) {
        int typeId = in.readIntPositioned(start + GridBinaryMarshaller.TYPE_ID_POS);

        if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID) {
            // Gets the length of the type name which is stored as string.
            int len = in.readIntPositioned(start + GridBinaryMarshaller.DFLT_HDR_LEN + /** object type */1);

            return GridBinaryMarshaller.DFLT_HDR_LEN + /** object type */1 + /** string length */ 4 + len;
        }
        else
            return GridBinaryMarshaller.DFLT_HDR_LEN;
    }

    /**
     * Get footer start of the object.
     *
     * @param in Input stream.
     * @param start Object start position inside the stream.
     * @return Footer start.
     */
    private static int footerStartRelative(BinaryPositionReadable in, int start) {
        short flags = in.readShortPositioned(start + GridBinaryMarshaller.FLAGS_POS);

        if (hasSchema(flags))
            // Schema exists, use offset.
            return in.readIntPositioned(start + GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS);
        else
            // No schema, footer start equals to object end.
            return length(in, start);
    }

    /**
     * Get object's footer.
     *
     * @param in Input stream.
     * @param start Start position.
     * @return Footer start.
     */
    public static int footerStartAbsolute(BinaryPositionReadable in, int start) {
        return footerStartRelative(in, start) + start;
    }

    /**
     * Get object's footer.
     *
     * @param in Input stream.
     * @param start Start position.
     * @return Footer.
     */
    public static IgniteBiTuple<Integer, Integer> footerAbsolute(BinaryPositionReadable in, int start) {
        short flags = in.readShortPositioned(start + GridBinaryMarshaller.FLAGS_POS);

        int footerEnd = length(in, start);

        if (hasSchema(flags)) {
            // Schema exists.
            int footerStart = in.readIntPositioned(start + GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS);

            if (hasRaw(flags))
                footerEnd -= 4;

            assert footerStart <= footerEnd;

            return F.t(start + footerStart, start + footerEnd);
        }
        else
            // No schema.
            return F.t(start + footerEnd, start + footerEnd);
    }

    /**
     * Get relative raw offset of the object.
     *
     * @param in Input stream.
     * @param start Object start position inside the stream.
     * @return Raw offset.
     */
    private static int rawOffsetRelative(BinaryPositionReadable in, int start) {
        short flags = in.readShortPositioned(start + GridBinaryMarshaller.FLAGS_POS);

        int len = length(in, start);

        if (hasSchema(flags)) {
            // Schema exists.
            if (hasRaw(flags))
                // Raw offset is set, it is at the very end of the object.
                return in.readIntPositioned(start + len - 4);
            else
                // Raw offset is not set, so just return schema offset.
                return in.readIntPositioned(start + GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS);
        }
        else
            // No schema, raw offset is located on schema offset position.
            return in.readIntPositioned(start + GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS);
    }

    /**
     * Get absolute raw offset of the object.
     *
     * @param in Input stream.
     * @param start Object start position inside the stream.
     * @return Raw offset.
     */
    public static int rawOffsetAbsolute(BinaryPositionReadable in, int start) {
        return start + rawOffsetRelative(in, start);
    }

    /**
     * Get offset length for the given flags.
     *
     * @param flags Flags.
     * @return Offset size.
     */
    public static int fieldOffsetLength(short flags) {
        if ((flags & FLAG_OFFSET_ONE_BYTE) == FLAG_OFFSET_ONE_BYTE)
            return OFFSET_1;
        else if ((flags & FLAG_OFFSET_TWO_BYTES) == FLAG_OFFSET_TWO_BYTES)
            return OFFSET_2;
        else
            return OFFSET_4;
    }

    /**
     * Get field ID length.
     *
     * @param flags Flags.
     * @return Field ID length.
     */
    public static int fieldIdLength(short flags) {
        return isCompactFooter(flags) ? 0 : FIELD_ID_LEN;
    }

    /**
     * Get relative field offset.
     *
     * @param stream Stream.
     * @param pos Position.
     * @param fieldOffsetSize Field offset size.
     * @return Relative field offset.
     */
    public static int fieldOffsetRelative(BinaryPositionReadable stream, int pos, int fieldOffsetSize) {
        int res;

        if (fieldOffsetSize == OFFSET_1)
            res = (int)stream.readBytePositioned(pos) & 0xFF;
        else if (fieldOffsetSize == OFFSET_2)
            res = (int)stream.readShortPositioned(pos) & 0xFFFF;
        else
            res = stream.readIntPositioned(pos);

        return res;
    }

    /**
     * @return {@code true} if content of serialized value cannot contain references to other object.
     */
    public static boolean isPlainType(int type) {
        return type > 0 && type < PLAIN_TYPE_FLAG.length && PLAIN_TYPE_FLAG[type];
    }

    /**
     * Checks whether an array type values can or can not contain references to other object.
     *
     * @param type Array type.
     * @return {@code true} if content of serialized array value cannot contain references to other object.
     */
    public static boolean isPlainArrayType(int type) {
        return (type >= GridBinaryMarshaller.BYTE_ARR && type <= GridBinaryMarshaller.DATE_ARR)
            || type == GridBinaryMarshaller.TIMESTAMP_ARR || type == GridBinaryMarshaller.TIME_ARR;
    }
}
