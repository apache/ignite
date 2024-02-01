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

/** */
public class RawBytesReader {
    /** */
    private final BinaryInputStream in;

    /** */
    public RawBytesReader(BinaryInputStream in) {
        this.in = in;
    }

    /** */
    public byte[] readObject() {
        int startPos = in.position();

        skipObject();

        int endPos = in.position();

        in.position(startPos);

        return in.readByteArray(endPos - startPos);
    }
    
    /** */
    private void skipObject() {
        byte type = in.readByte();

        switch (type) {
            case GridBinaryMarshaller.NULL:
                break;

            case GridBinaryMarshaller.OBJ:
                skipBinaryObject();

                break;

            case GridBinaryMarshaller.BINARY_OBJ:
                skipBytes(in.readInt());

                skipBytes(4); // Offset.

                break;


            case GridBinaryMarshaller.BOOLEAN:
            case GridBinaryMarshaller.BYTE:
                skipBytes(1);

                break;

            case GridBinaryMarshaller.CHAR:
            case GridBinaryMarshaller.SHORT:
                skipBytes(2);

                break;

            case GridBinaryMarshaller.HANDLE:
            case GridBinaryMarshaller.FLOAT:
            case GridBinaryMarshaller.INT:
                skipBytes(4);

                break;

            case GridBinaryMarshaller.ENUM:
            case GridBinaryMarshaller.BINARY_ENUM: {
                skipTypeId();

                skipBytes(4); // Ordinal.

                break;
            }

            case GridBinaryMarshaller.LONG:
            case GridBinaryMarshaller.DOUBLE:
            case GridBinaryMarshaller.DATE:
            case GridBinaryMarshaller.TIME:
                skipBytes(8);

                break;

            case GridBinaryMarshaller.BYTE_ARR:
            case GridBinaryMarshaller.BOOLEAN_ARR:
            case GridBinaryMarshaller.STRING:
            case GridBinaryMarshaller.OPTM_MARSH:
                skipBytes(in.readInt());

                break;

            case GridBinaryMarshaller.DECIMAL:
                skipBytes(4); // scale
                skipBytes(in.readInt());

                break;

            case GridBinaryMarshaller.UUID:
                skipBytes(8 + 8);

                break;

            case GridBinaryMarshaller.TIMESTAMP:
                skipBytes(8 + 4);

                break;

            case GridBinaryMarshaller.CHAR_ARR:
            case GridBinaryMarshaller.SHORT_ARR:
                skipBytes(in.readInt() * 2);

                break;

            case GridBinaryMarshaller.INT_ARR:
            case GridBinaryMarshaller.FLOAT_ARR:
                skipBytes(in.readInt() * 4);

                break;

            case GridBinaryMarshaller.LONG_ARR:
            case GridBinaryMarshaller.DOUBLE_ARR:
                skipBytes(in.readInt() * 8);

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

                skipBytes(1); // Collection type.

                skipCortege(size);

                break;
            }

            case GridBinaryMarshaller.MAP: {
                int size = in.readInt() * 2;

                skipBytes(1); // Map type.

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

                skipBinaryObject();
            }

            default:
                throw new BinaryObjectException("Unsupported binary type [type=" + type + ']');
        }
    }

    /** */
    private void skipBytes(int count) {
        in.position(in.position() + count);
    }

    /** */
    private void skipString() {
        skipBytes(in.readInt());
    }

    /** */
    private void skipBinaryObject() {
        skipBytes(in.readIntPositioned(in.position() + GridBinaryMarshaller.TOTAL_LEN_POS));
    }

    /** */
    private void skipTypeId() {
        int typeId = in.readInt();

        if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
            skipString();
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
