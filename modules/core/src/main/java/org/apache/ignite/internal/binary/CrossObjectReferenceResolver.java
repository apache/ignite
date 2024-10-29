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
    private final RawBinaryObjectExtractor in;

    /** */
    private final BinaryOutputStream out;

    /** */
    private final int inRootObjStartPos;

    /** */
    private final Map<Integer, Integer> objPosTranslation = new HashMap<>();

    /** */
    private final BinaryWriterSchemaHolder schema = new BinaryWriterSchemaHolder();

    /** */
    private CrossObjectReferenceResolver(RawBinaryObjectExtractor in, BinaryOutputStream out) {
        this.in = in;

        inRootObjStartPos = in.position();

        this.out = out;
    }

    /** */
    static void copyObject(RawBinaryObjectExtractor in, BinaryOutputStream out) {
        CrossObjectReferenceResolver resolver = new CrossObjectReferenceResolver(in, out);

        resolver.reassembleNextObject();
    }

    /** */
    private void reassembleNextObject() {
        int inObjStartPos = in.position();
        int outObjStartPos = out.position();

        byte objType = in.readBytePositioned(inObjStartPos);

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
                objPosTranslation.put(inObjStartPos, outObjStartPos);

                copyBytes(Byte.BYTES); // Object type.

                in.copyTypeId(out);

                int size = copyInt();

                reassembleNextCortege(size);

                break;
            }

            case GridBinaryMarshaller.COL: {
                objPosTranslation.put(inObjStartPos, outObjStartPos);

                copyBytes(Byte.BYTES); // Object type.

                int size = copyInt();

                copyBytes(Byte.BYTES); // Collection type.

                reassembleNextCortege(size);

                break;
            }

            case GridBinaryMarshaller.MAP: {
                objPosTranslation.put(inObjStartPos, outObjStartPos);

                copyBytes(Byte.BYTES); // Object type.

                int size = copyInt() * 2;

                copyBytes(Byte.BYTES); // Map type.

                reassembleNextCortege(size);

                break;
            }

            default:
                in.copyObject(out);
        }
    }

    /** */
    private void doObjectProcessing() {
        int inObjStartPos = in.position();
        int outObjStartPos = out.position();

        objPosTranslation.put(inObjStartPos, outObjStartPos);

        BinaryObjectDescriptor inObjDesc = BinaryObjectDescriptor.parse(in, inObjStartPos);

        int fieldsCnt = 0;

        try {
            // Copy object's header. Update with actual data length and field offsets will be performed later.
            copyBytes(dataStartRelative(in, inObjStartPos));

            // Process object fields.
            while (in.position() < inObjDesc.rawDataStartPos)
                reassembleField(fieldsCnt++, offset(outObjStartPos, out.position()), inObjDesc);

            int outRawDataStartPos = -1;

            if (inObjDesc.hasRaw) {
                outRawDataStartPos = out.position();

                copyBytes(inObjDesc.footerStartPos - in.position());
            }

            int outFooterStartPos = out.position();

            int schemaOrRawOffsetPos;
            int footerFieldOffsetLen;

            // Write footer and raw data offset if required.
            if (inObjDesc.hasSchema) {
                schemaOrRawOffsetPos = offset(outObjStartPos, outFooterStartPos);
                footerFieldOffsetLen = schema.write(out, fieldsCnt, inObjDesc.isCompactFooter);

                if (inObjDesc.hasRaw)
                    out.writeInt(offset(outObjStartPos, outRawDataStartPos));
            }
            else {
                schemaOrRawOffsetPos = inObjDesc.hasRaw ? offset(outObjStartPos, outRawDataStartPos) : DFLT_HDR_LEN;
                footerFieldOffsetLen = 0;
            }

            // Update header to reflect changes after cross object references are resolved.
            overrideHeader(
                outObjStartPos,
                /** flags */ setFieldOffsetFlag(inObjDesc.flags, footerFieldOffsetLen),
                /** hash */ BinaryArrayIdentityResolver.instance().hashCode(out.array(), outObjStartPos + DFLT_HDR_LEN, outFooterStartPos),
                /** total length */ out.position() - outObjStartPos,
                schemaOrRawOffsetPos
            );

            in.position(inObjDesc.endPos);
        }
        finally {
            schema.pop(fieldsCnt);
        }
    }

    /** */
    private void doHandleProcessing() {
        int inObjStartPos = in.position();

        in.skipBytes(1); // Object type.

        int offset = in.readInt();

        int inHandleObjPos = inObjStartPos - offset;

        Integer outHandleObjPos = objPosTranslation.get(inHandleObjPos);

        if (outHandleObjPos != null) {
            int outObjStartPos = out.position();

            out.writeByte(GridBinaryMarshaller.HANDLE);
            out.writeInt(offset(outHandleObjPos, outObjStartPos));
        }
        else {
            assert inHandleObjPos < inRootObjStartPos;

            objPosTranslation.put(inHandleObjPos, out.position());

            copyObjectPositioned(inHandleObjPos);
        }
    }

    /** */
    private void overrideHeader(int writeObjStartPos, short flags, int hashCode, int totalLen, int schemaOrRawOffsetPos) {
        out.unsafeWriteShort(writeObjStartPos + FLAGS_POS, flags);
        out.unsafeWriteInt(writeObjStartPos + HASH_CODE_POS, hashCode);
        out.unsafeWriteInt(writeObjStartPos + TOTAL_LEN_POS, totalLen);
        out.unsafeWriteInt(writeObjStartPos + SCHEMA_OR_RAW_OFF_POS, schemaOrRawOffsetPos);
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
            : in.readIntPositioned(binObjDesc.fieldIdPosition(fieldOrder));

        schema.push(fieldId, fieldOffset);

        reassembleNextObject();
    }

    /** */
    private void reassembleNextCortege(int cortegeSize) {
        for (int elemIdx = 0; elemIdx < cortegeSize; elemIdx++)
            reassembleNextObject();
    }

    /** */
    private void copyObjectPositioned(int inPos) {
        int inRetPos = in.position();

        in.position(inPos);

        ObjectDetachHelper detachHelper = ObjectDetachHelper.create(in.array(), inPos);

        if (detachHelper.isCrossObjectReferencesDetected())
            detachHelper.detach(out);
        else
            in.copyObject(out);

        in.position(inRetPos);
    }

    /** */
    private int copyInt() {
        int res = in.peekInt();

        copyBytes(Integer.BYTES);

        return res;
    }

    /** */
    private void copyBytes(int cnt) {
        in.copyBytes(cnt, out);
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
        private BinaryObjectDescriptor(BinaryPositionReadable in, int startPos) {
            rawDataStartPos = rawOffsetAbsolute(in, startPos);
            footerStartPos = footerStartAbsolute(in, startPos);
            endPos = startPos + length(in, startPos);

            flags = in.readShortPositioned(startPos + FLAGS_POS);

            hasRaw = hasRaw(flags);
            hasSchema = hasSchema(flags);
            isCompactFooter = isCompactFooter(flags);
            fieldOffsetLength = fieldOffsetLength(flags);
        }

        /** */
        private static BinaryObjectDescriptor parse(BinaryPositionReadable in, int startPos) {
            return new BinaryObjectDescriptor(in, startPos);
        }

        /** */
        private int fieldIdPosition(int fieldOrder) {
            return footerStartPos + (/** field ID */ 4 + fieldOffsetLength) * fieldOrder;
        }
    }
}
