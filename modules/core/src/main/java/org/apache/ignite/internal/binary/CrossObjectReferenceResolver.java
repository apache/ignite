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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

import static org.apache.ignite.internal.binary.BinaryArrayIdentityResolver.instance;
import static org.apache.ignite.internal.binary.BinaryUtils.FLAG_OFFSET_ONE_BYTE;
import static org.apache.ignite.internal.binary.BinaryUtils.FLAG_OFFSET_TWO_BYTES;
import static org.apache.ignite.internal.binary.BinaryUtils.OFFSET_1;
import static org.apache.ignite.internal.binary.BinaryUtils.OFFSET_2;
import static org.apache.ignite.internal.binary.BinaryUtils.dataStartRelative;
import static org.apache.ignite.internal.binary.BinaryUtils.fieldOffsetLength;
import static org.apache.ignite.internal.binary.BinaryUtils.footerStartAbsolute;
import static org.apache.ignite.internal.binary.BinaryUtils.hasRaw;
import static org.apache.ignite.internal.binary.BinaryUtils.hasSchema;
import static org.apache.ignite.internal.binary.BinaryUtils.isCompactFooter;
import static org.apache.ignite.internal.binary.BinaryUtils.length;
import static org.apache.ignite.internal.binary.BinaryUtils.rawOffsetAbsolute;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.DFLT_HDR_LEN;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.FLAGS_POS;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.HASH_CODE_POS;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.SCHEMA_OR_RAW_OFF_POS;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TOTAL_LEN_POS;

/** */
public class CrossObjectReferenceResolver {
    /** */
    private final RawBytesObjectReader reader;

    /** */
    private final BinaryOutputStream writer;

    /** */
    private final int readerRootObjStartPos;

    /** */
    private final Map<Integer, Integer> objPosTranslation = new HashMap<>();

    /** */
    private final BinaryWriterSchemaHolder schema = new BinaryWriterSchemaHolder();

    /** */
    private CrossObjectReferenceResolver(RawBytesObjectReader reader, BinaryOutputStream out) {
        this.reader = reader;

        readerRootObjStartPos = reader.position();

        writer = out;
    }

    /** */
    static void copyObject(RawBytesObjectReader reader, BinaryOutputStream out) {
        CrossObjectReferenceResolver resolver = new CrossObjectReferenceResolver(reader, out);

        resolver.reassembleNextObject();
    }

    /** */
    private void reassembleNextObject() {
        int readerObjStartPos = reader.position();
        int writerObjStartPos = writer.position();

        byte objType = reader.readBytePositioned(readerObjStartPos);

        switch (objType) {
            case GridBinaryMarshaller.OBJ: {
                doObjectProcessing();

                break;
            }

            case GridBinaryMarshaller.HANDLE: {
                doHandleProcessing();

                break;
            }

            case GridBinaryMarshaller.OBJ_ARR: {
                objPosTranslation.put(readerObjStartPos, writerObjStartPos);

                copyBytes(Byte.BYTES); // Object type.

                reader.copyTypeId(writer);

                int size = copyInt();

                reassembleNextCortege(size);

                break;
            }

            case GridBinaryMarshaller.COL: {
                objPosTranslation.put(readerObjStartPos, writerObjStartPos);

                copyBytes(Byte.BYTES); // Object type.

                int size = copyInt();

                copyBytes(Byte.BYTES); // Collection type.

                reassembleNextCortege(size);

                break;
            }

            case GridBinaryMarshaller.MAP: {
                objPosTranslation.put(readerObjStartPos, writerObjStartPos);

                copyBytes(Byte.BYTES); // Object type.

                int size = copyInt() * 2;

                copyBytes(Byte.BYTES); // Map type.

                reassembleNextCortege(size);

                break;
            }

            default:
                reader.copyObject(writer);
        }
    }

    /** */
    private void doObjectProcessing() {
        int readerObjStartPos = reader.position();
        int writerObjStartPos = writer.position();

        objPosTranslation.put(readerObjStartPos, writerObjStartPos);

        BinaryObjectDescriptor readObjDesc = BinaryObjectDescriptor.parse(reader, readerObjStartPos);

        int fieldsCnt = 0;

        try {
            // Copy object's header. Update with actual data length and field offsets will be performed later.
            copyBytes(dataStartRelative(reader, readerObjStartPos));

            // Process object fields.
            while (reader.position() < readObjDesc.rawDataStartPos)
                reassembleField(fieldsCnt++, offset(writerObjStartPos, writer.position()), readObjDesc);

            int writeRawDataStartPos = -1;

            if (readObjDesc.hasRaw) {
                writeRawDataStartPos = writer.position();

                copyBytes(readObjDesc.footerStartPos - reader.position());
            }

            int writeFooterStartPos = writer.position();

            int schemaOrRawOffsetPos;
            int footerFieldOffsetLen;

            // Write footer and raw data offset if required.
            if (readObjDesc.hasSchema) {
                schemaOrRawOffsetPos = offset(writerObjStartPos, writeFooterStartPos);
                footerFieldOffsetLen = schema.write(writer, fieldsCnt, readObjDesc.isCompactFooter);

                if (readObjDesc.hasRaw)
                    writer.writeInt(offset(writerObjStartPos, writeRawDataStartPos));
            }
            else {
                schemaOrRawOffsetPos = readObjDesc.hasRaw ? offset(writerObjStartPos, writeRawDataStartPos) : DFLT_HDR_LEN;
                footerFieldOffsetLen = 0;
            }

            // Update header to reflect changes after cross object references are resolved.
            overrideHeader(
                writerObjStartPos,
                /** flags */ setFieldOffsetFlag(readObjDesc.flags, footerFieldOffsetLen),
                /** hash */ instance().hashCode(writer.array(), writerObjStartPos + DFLT_HDR_LEN, writeFooterStartPos),
                /** total length */ writer.position() - writerObjStartPos,
                schemaOrRawOffsetPos
            );

            reader.position(readObjDesc.endPos);
        }
        finally {
            schema.pop(fieldsCnt);
        }
    }

    /** */
    private void doHandleProcessing() {
        int readerObjStartPos = reader.position();

        reader.skipBytes(1); // Object type.

        int offset = reader.readInt();

        int readerHandleObjPos = readerObjStartPos - offset;

        Integer writerObjPos = objPosTranslation.get(readerHandleObjPos);

        if (writerObjPos != null) {
            int writerObjStartPos = writer.position();

            writer.writeByte(GridBinaryMarshaller.HANDLE);
            writer.writeInt(offset(writerObjPos, writerObjStartPos));
        }
        else {
            assert readerHandleObjPos < readerRootObjStartPos;

            objPosTranslation.put(readerHandleObjPos, writer.position());

            copyObjectPositioned(readerHandleObjPos);
        }
    }

    /** */
    private void overrideHeader(int writeObjStartPos, short flags, int hashCode, int totalLen, int schemaOrRawOffsetPos) {
        writer.unsafeWriteShort(writeObjStartPos + FLAGS_POS, flags);
        writer.unsafeWriteInt(writeObjStartPos + HASH_CODE_POS, hashCode);
        writer.unsafeWriteInt(writeObjStartPos + TOTAL_LEN_POS, totalLen);
        writer.unsafeWriteInt(writeObjStartPos + SCHEMA_OR_RAW_OFF_POS, schemaOrRawOffsetPos);
    }

    /** */
    private short setFieldOffsetFlag(short flags, int fieldOffsetLength) {
        if (fieldOffsetLength == 0)
            return flags;

        flags &= ~FLAG_OFFSET_ONE_BYTE;
        flags &= ~FLAG_OFFSET_TWO_BYTES;

        if (fieldOffsetLength == OFFSET_1)
            flags |= FLAG_OFFSET_ONE_BYTE;
        else if (fieldOffsetLength == OFFSET_2)
            flags |= FLAG_OFFSET_TWO_BYTES;

        return flags;
    }

    /** */
    private void reassembleField(int fieldOrder, int fieldOffset, BinaryObjectDescriptor binObjDesc) {
        int fieldId = binObjDesc.isCompactFooter
            ? -1
            : reader.readIntPositioned(binObjDesc.fieldIdPosition(fieldOrder));

        schema.push(fieldId, fieldOffset);

        reassembleNextObject();
    }

    /** */
    private void reassembleNextCortege(int cortegeSize) {
        for (int elemIdx = 0; elemIdx < cortegeSize; elemIdx++)
            reassembleNextObject();
    }

    /** */
    private void copyObjectPositioned(int readerPos) {
        int readerRetPos = reader.position();

        reader.position(readerPos);

        ObjectDetachHelper detachHelper = ObjectDetachHelper.create(reader.array(), readerPos);

        if (detachHelper.isCrossObjectReferencesPresent())
            detachHelper.detach(writer);
        else
            reader.copyObject(writer);

        reader.position(readerRetPos);
    }

    /** */
    private int copyInt() {
        int res = reader.peekInt();

        copyBytes(Integer.BYTES);

        return res;
    }

    /** */
    private void copyBytes(int cnt) {
        reader.copyBytes(cnt, writer);
    }

    /** */
    private int offset(int startPos, int pos) {
        assert pos - startPos >= 0;

        return pos - startPos;
    }

    /** */
    private static class BinaryObjectDescriptor {
        /** */
        private final int rawDataStartPos;

        /** */
        private final int footerStartPos;

        /** */
        private final int endPos;

        /** */
        private final short flags;

        /** */
        private final boolean hasRaw;

        /** */
        private final boolean hasSchema;

        /** */
        private final boolean isCompactFooter;

        /** */
        private final int fieldOffsetLength;

        /** */
        private BinaryObjectDescriptor(BinaryPositionReadable reader, int startPos) {
            rawDataStartPos = rawOffsetAbsolute(reader, startPos);
            footerStartPos = footerStartAbsolute(reader, startPos);
            endPos = startPos + length(reader, startPos);

            flags = reader.readShortPositioned(startPos + FLAGS_POS);

            hasRaw = hasRaw(flags);
            hasSchema = hasSchema(flags);
            isCompactFooter = isCompactFooter(flags);
            fieldOffsetLength = fieldOffsetLength(flags);
        }

        /** */
        private static BinaryObjectDescriptor parse(BinaryPositionReadable reader, int startPos) {
            return new BinaryObjectDescriptor(reader, startPos);
        }

        /** */
        private int fieldIdPosition(int fieldOrder) {
            return footerStartPos + (/** field ID */ 4 + fieldOffsetLength) * fieldOrder;
        }
    }
}
