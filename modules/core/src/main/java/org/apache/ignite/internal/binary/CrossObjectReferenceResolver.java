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
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
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
    private final BinaryWriterSchemaHolder schema;

    /** */
    private final Map<Integer, Integer> objPosTranslation;

    /** */
    private int leftBoundPos;

    /** */
    public CrossObjectReferenceResolver(BinaryInputStream in, BinaryOutputStream out) {
        reader = new RawBytesObjectReader(in);

        writer = out;

        schema = new BinaryWriterSchemaHolder();

        objPosTranslation = new HashMap<>();
    }

    /** */
    public void resolveCrossObjectReferences() {
        leftBoundPos = reader.position();

        objPosTranslation.clear();

        reassembleNextObject();
    }

    /** */
    private void reassembleNextObject() {
        int readerObjStartPos = reader.position();
        int writerObjStartPos = writer.position();

        byte objType = reader.readBytePositioned(readerObjStartPos);

        switch (objType) {
            case GridBinaryMarshaller.OBJ: {
                processObject();

                break;
            }

            case GridBinaryMarshaller.HANDLE: {
                processHandle();

                break;
            }

            case GridBinaryMarshaller.OBJ_ARR: {
                objPosTranslation.put(readerObjStartPos, writerObjStartPos);

                copyBytes(1); // Object type.

                copyTypeId();

                int size = readAndCopyInt();

                reassembleNextCortege(size);

                break;
            }

            case GridBinaryMarshaller.COL: {
                objPosTranslation.put(readerObjStartPos, writerObjStartPos);

                copyBytes(1); // Object type.

                int size = readAndCopyInt();

                copyBytes(1); // Collection type.

                reassembleNextCortege(size);

                break;
            }

            case GridBinaryMarshaller.MAP: {
                objPosTranslation.put(readerObjStartPos, writerObjStartPos);

                copyBytes(1); // Object type.

                int size = readAndCopyInt() * 2;

                copyBytes(1); // Map type.

                reassembleNextCortege(size);

                break;
            }

            default:
                writer.writeByteArray(reader.readObject());
        }
    }

    /** */
    private void processObject() {
        int readerObjStartPos = reader.position();
        int writerObjStartPos = writer.position();

        objPosTranslation.put(readerObjStartPos, writerObjStartPos);

        BinaryObjectStructure readObjStruct = BinaryObjectStructure.parse(reader, readerObjStartPos);

        int fieldsCnt = 0;

        try {
            // Copy object's header. Update with actual data length and field offsets will be performed later.
            copyBytes(dataStartRelative(reader, readerObjStartPos));

            // Process object fields.
            while (reader.position() < readObjStruct.rawDataStartPos)
                reassembleField(fieldsCnt++, offset(writerObjStartPos, writer.position()), readObjStruct);

            int writeRawDataStartPos = -1;

            if (readObjStruct.hasRaw) {
                writeRawDataStartPos = writer.position();

                while (reader.position() < readObjStruct.footerStartPos)
                    reassembleNextObject();
            }

            int writeFooterStartPos = writer.position();

            int schemaOrRawOffsetPos;
            int footerFieldOffsetLen;

            // Write footer and raw data offset if required.
            if (readObjStruct.hasSchema) {
                schemaOrRawOffsetPos = offset(writerObjStartPos, writeFooterStartPos);
                footerFieldOffsetLen = schema.write(writer, fieldsCnt, readObjStruct.isCompactFooter);

                if (readObjStruct.hasRaw)
                    writer.writeInt(offset(writerObjStartPos, writeRawDataStartPos));
            }
            else {
                schemaOrRawOffsetPos = readObjStruct.hasRaw ? offset(writerObjStartPos, writeRawDataStartPos) : DFLT_HDR_LEN;
                footerFieldOffsetLen = 0;
            }

            // Update header to reflect changes after cross object references are resolved.
            overrideHeader(
                writerObjStartPos,
                /** flags */ setFieldOffsetFlag(readObjStruct.flags, footerFieldOffsetLen),
                /** hash */ instance().hashCode(writer.array(), writerObjStartPos + DFLT_HDR_LEN, writeFooterStartPos),
                /** total length */ writer.position() - writerObjStartPos,
                schemaOrRawOffsetPos
            );

            reader.position(readObjStruct.endPos);
        }
        finally {
            schema.pop(fieldsCnt);
        }
    }

    /** */
    private void processHandle() {
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
            assert readerHandleObjPos < leftBoundPos;

            objPosTranslation.put(readerHandleObjPos, writer.position());

            writer.writeByteArray(reader.readObjectPositioned(readerHandleObjPos));
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
    private void reassembleField(int fieldOrder, int fieldOffset, BinaryObjectStructure objStruct) {
        int fieldId = objStruct.isCompactFooter
            ? -1
            : reader.readIntPositioned(objStruct.fieldIdPosition(fieldOrder));

        schema.push(fieldId, fieldOffset);

        reassembleNextObject();
    }

    /** */
    void reassembleNextCortege(int cortegeSize) {
        for (int elemIdx = 0; elemIdx < cortegeSize; elemIdx++)
            reassembleNextObject();
    }

    /** */
    private void copyTypeId() {
        int typeId = readAndCopyInt();

        if (typeId == GridBinaryMarshaller.UNREGISTERED_TYPE_ID) {
            copyBytes(1); // String type.

            int strLen = readAndCopyInt();

            copyBytes(strLen);
        }
    }

    /** */
    private int readAndCopyInt() {
        int res = reader.peekInt();

        copyBytes(4);

        return res;
    }

    /** */
    private void copyBytes(int cnt) {
        assert cnt >= 0;

        if (cnt == 0)
            return;

        writer.writeByteArray(reader.readByteArray(cnt));
    }

    /** */
    private int offset(int startPos, int pos) {
        assert pos - startPos >= 0;

        return pos - startPos;
    }

    /** */
    private static class BinaryObjectStructure {
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
        private BinaryObjectStructure(BinaryPositionReadable reader, int startPos) {
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
        private static BinaryObjectStructure parse(BinaryPositionReadable reader, int startPos) {
            return new BinaryObjectStructure(reader, startPos);
        }

        /** */
        private int fieldIdPosition(int fieldOrder) {
            return footerStartPos + (/** field ID */ 4 + fieldOffsetLength) * fieldOrder;
        }
    }
}
