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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

/** */
class RawBinaryObjectExtractor implements BinaryPositionReadable {
    /** */
    private final BinaryInputStream in;

    /** */
    RawBinaryObjectExtractor(BinaryInputStream in) {
        this.in = in;
    }

    /** */
    byte[] extractObject() {
        int startPos = in.position();

        skipObject();

        int endPos = in.position();

        in.position(startPos);

        return in.readByteArray(endPos - startPos);
    }

    /** */
    void copyObject(BinaryOutputStream out) {
        int startPos = in.position();

        skipObject();

        out.writeByteArray(in.array(), startPos, in.position() - startPos);
    }

    /** */
    void copyTypeId(BinaryOutputStream out) {
        int startPos = in.position();

        skipTypeId();

        out.writeByteArray(in.array(), startPos, in.position() - startPos);
    }

    /** */
    void copyBytes(int cnt, BinaryOutputStream out) {
        assert cnt >= 0;

        if (cnt == 0)
            return;

        out.writeByteArray(in.array(), in.position(), cnt);

        skipBytes(cnt);
    }

    /** */
    int readInt() {
        return in.readInt();
    }

    /** */
    void skipObject() {
        int objStartPos = in.position();

        byte type = in.readByte();

        switch (type) {
            case GridBinaryMarshaller.NULL:
                break;

            case GridBinaryMarshaller.OBJ:
                skipBytes(BinaryUtils.length(in, objStartPos) - /** Object type. */ Byte.BYTES);

                break;

            case GridBinaryMarshaller.BINARY_OBJ:
                skipBytes(in.readInt());

                skipBytes(Integer.BYTES); // Offset.

                break;

            case GridBinaryMarshaller.BYTE:
            case GridBinaryMarshaller.BOOLEAN:
                skipBytes(Byte.BYTES);

                break;

            case GridBinaryMarshaller.CHAR:
                skipBytes(Character.BYTES);

                break;

            case GridBinaryMarshaller.SHORT:
                skipBytes(Short.BYTES);

                break;

            case GridBinaryMarshaller.FLOAT:
                skipBytes(Float.BYTES);

                break;

            case GridBinaryMarshaller.HANDLE:
            case GridBinaryMarshaller.INT:
                skipBytes(Integer.BYTES);

                break;

            case GridBinaryMarshaller.ENUM:
            case GridBinaryMarshaller.BINARY_ENUM: {
                skipTypeId();

                skipBytes(Integer.BYTES); // Ordinal.

                break;
            }

            case GridBinaryMarshaller.LONG:
            case GridBinaryMarshaller.DATE:
            case GridBinaryMarshaller.TIME:
                skipBytes(Long.BYTES);

                break;

            case GridBinaryMarshaller.DOUBLE:
                skipBytes(Double.BYTES);

                break;

            case GridBinaryMarshaller.OPTM_MARSH:
            case GridBinaryMarshaller.STRING:
                skipBytes(in.readInt());

                break;

            case GridBinaryMarshaller.DECIMAL:
                skipBytes(Integer.BYTES); // Scale.
                skipBytes(in.readInt());

                break;

            case GridBinaryMarshaller.UUID:
                skipBytes(Long.BYTES + Long.BYTES);

                break;

            case GridBinaryMarshaller.TIMESTAMP:
                skipBytes(Long.BYTES + Integer.BYTES);

                break;

            case GridBinaryMarshaller.BYTE_ARR:
            case GridBinaryMarshaller.BOOLEAN_ARR:
                skipBytes(in.readInt() * Byte.BYTES);

                break;

            case GridBinaryMarshaller.CHAR_ARR:
                skipBytes(in.readInt() * Character.BYTES);

                break;

            case GridBinaryMarshaller.SHORT_ARR:
                skipBytes(in.readInt() * Short.BYTES);

                break;

            case GridBinaryMarshaller.INT_ARR:
                skipBytes(in.readInt() * Integer.BYTES);

                break;

            case GridBinaryMarshaller.FLOAT_ARR:
                skipBytes(in.readInt() * Float.BYTES);

                break;

            case GridBinaryMarshaller.LONG_ARR:
                skipBytes(in.readInt() * Long.BYTES);

                break;

            case GridBinaryMarshaller.DOUBLE_ARR:
                skipBytes(in.readInt() * Double.BYTES);

                break;

            case GridBinaryMarshaller.DECIMAL_ARR:
            case GridBinaryMarshaller.DATE_ARR:
            case GridBinaryMarshaller.TIMESTAMP_ARR:
            case GridBinaryMarshaller.TIME_ARR:
            case GridBinaryMarshaller.UUID_ARR:
            case GridBinaryMarshaller.STRING_ARR: {
                skipCortege();

                break;
            }

            case GridBinaryMarshaller.ENUM_ARR:
            case GridBinaryMarshaller.OBJ_ARR: {
                skipTypeId();

                skipCortege();

                break;
            }

            case GridBinaryMarshaller.COL: {
                int size = in.readInt();

                skipBytes(Byte.BYTES); // Collection type.

                skipCortege(size);

                break;
            }

            case GridBinaryMarshaller.MAP: {
                int size = in.readInt() * 2;

                skipBytes(Byte.BYTES); // Map type.

                skipCortege(size);

                break;
            }

            case GridBinaryMarshaller.CLASS: {
                skipTypeId();

                break;
            }

            case GridBinaryMarshaller.PROXY: {
                int size = in.readInt();

                for (int i = 0; i < size; i++)
                    skipTypeId(); // Interface type.

                skipObject();

                break;
            }

            default:
                throw new BinaryObjectException("Unsupported binary type [type=" + type + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public byte readBytePositioned(int pos) {
        return in.readBytePositioned(pos);
    }

    /** {@inheritDoc} */
    @Override public short readShortPositioned(int pos) {
        return in.readShortPositioned(pos);
    }

    /** {@inheritDoc} */
    @Override public int readIntPositioned(int pos) {
        return in.readIntPositioned(pos);
    }

    /** */
    int position() {
        return in.position();
    }

    /** */
    void position(int position) {
        in.position(position);
    }

    /** */
    byte peekByte() {
        return in.readBytePositioned(in.position());
    }

    /** */
    int peekInt() {
        return in.readIntPositioned(in.position());
    }

    /** */
    void skipBytes(int count) {
        int curPos = in.position();

        in.position(curPos + count);
    }

    /** */
    void skipTypeId() {
        int typeId = in.readInt();

        if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            skipObject();
    }

    /** */
    byte[] array() {
        return in.array();
    }

    /** */
    private void skipCortege() {
        skipCortege(in.readInt());
    }

    /** */
    private void skipCortege(int size) {
        for (int i = 0; i < size; i++)
            skipObject();
    }
}
