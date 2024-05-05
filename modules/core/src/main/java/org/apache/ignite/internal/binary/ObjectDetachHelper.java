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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.binary.BinaryArrayIdentityResolver.instance;
import static org.apache.ignite.internal.binary.BinaryUtils.dataStartRelative;
import static org.apache.ignite.internal.binary.BinaryUtils.footerStartAbsolute;
import static org.apache.ignite.internal.binary.BinaryUtils.length;
import static org.apache.ignite.internal.binary.BinaryUtils.rawOffsetAbsolute;
import static org.apache.ignite.internal.binary.GridBinaryMarshaller.HASH_CODE_POS;

/** */
public class ObjectDetachHelper {
    /** */
    private final RawBytesObjectReader reader;

    /** */
    private final int rootObjStartPos;

    /** */
    private final Deque<ObjectDescriptor> objStack = new ArrayDeque<>();

    /** */
    private final Map<Integer, Collection<Integer>> crossObjReferences = new HashMap<>();

    /** */
    private final TreeSet<ObjectDescriptor> rehashRequireObjects = new TreeSet<>();

    /** */
    private ObjectDetachHelper(BinaryInputStream in) {
        reader = new RawBytesObjectReader(in);

        rootObjStartPos = in.position();
    }

    /** */
    static ObjectDetachHelper create(byte[] data, int offset) {
        ObjectDetachHelper res = new ObjectDetachHelper(BinaryHeapInputStream.create(data, offset));

        res.readNextObject();

        return res;
    }

    /** */
    public boolean isCrossObjectReferencesDetected() {
        return !crossObjReferences.isEmpty();
    }

    /** */
    public int detach(BinaryOutputStream writer) {
        reader.position(rootObjStartPos);

        int writerRootObjStartPos = writer.position();

        reader.copyObject(writer);

        crossObjReferences.forEach((readerObjStartPos, handleOffsets) -> {
            int writerObjStartPos = copyObject(readerObjStartPos, writer);

            for (int handleOffset : handleOffsets) {
                int writerHandleStartPos = writerRootObjStartPos + handleOffset;

                writer.unsafeWriteInt(writerHandleStartPos + /* skip handle type */ 1, writerHandleStartPos - writerObjStartPos);
            }
        });

        for (ObjectDescriptor objDesc : rehashRequireObjects.descendingSet())
            overrideHash(writer, writerRootObjStartPos, objDesc);

        return writerRootObjStartPos;
    }

    /** */
    private void readNextObject() {
        int objStartPos = reader.position();

        byte objType = reader.readBytePositioned(objStartPos);

        switch (objType) {
            case GridBinaryMarshaller.OBJ: {
                int objDataStartPos = objStartPos + dataStartRelative(reader, objStartPos);
                int objDataEndPos = rawOffsetAbsolute(reader, objStartPos);
                int objFooterStartPos = footerStartAbsolute(reader, objStartPos);
                int objEndPos = objStartPos + length(reader, objStartPos);

                reader.position(objDataStartPos);

                objStack.push(new ObjectDescriptor(
                    rootStartOffset(objStartPos),
                    rootStartOffset(objDataStartPos),
                    rootStartOffset(objFooterStartPos))
                );

                while (reader.position() < objDataEndPos)
                    readNextObject();

                objStack.pop();

                reader.position(objEndPos);

                break;
            }

            case GridBinaryMarshaller.HANDLE: {
                reader.skipBytes(1); // Object type.

                int offset = reader.readInt();

                int handleObjPos = objStartPos - offset;

                if (handleObjPos < rootObjStartPos)
                    saveCrossObjectReferenceData(handleObjPos, objStartPos);

                break;
            }

            case GridBinaryMarshaller.OBJ_ARR: {
                reader.skipBytes(1); // Object type.

                reader.skipTypeId();

                int size = reader.readInt();

                readCortege(size);

                break;
            }

            case GridBinaryMarshaller.COL: {
                reader.skipBytes(1); // Object type.

                int size = reader.readInt();

                reader.skipBytes(1); // Collection type.

                readCortege(size);

                break;
            }

            case GridBinaryMarshaller.MAP: {
                reader.skipBytes(1); // Object type.

                int size = reader.readInt() * 2;

                reader.skipBytes(1); // Map type.

                readCortege(size);

                break;
            }

            default: {
                reader.skipObject();
            }
        }
    }

    /** */
    private void readCortege(int cortegeSize) {
        for (int elemIdx = 0; elemIdx < cortegeSize; elemIdx++)
            readNextObject();
    }

    /** */
    private void saveCrossObjectReferenceData(int objPos, int handlePos) {
        crossObjReferences.computeIfAbsent(objPos, k -> new ArrayList<>()).add(rootStartOffset(handlePos));

        rehashRequireObjects.addAll(objStack);
    }

    /** */
    private int rootStartOffset(int pos) {
        assert pos - rootObjStartPos >= 0;

        return pos - rootObjStartPos;
    }

    /** */
    private void overrideHash(BinaryOutputStream writer, int writerRootObjStartPos, ObjectDescriptor objDesc) {
        int hashCode = instance().hashCode(
            writer.array(),
            writerRootObjStartPos + objDesc.objectDataStartOffset(),
            writerRootObjStartPos + objDesc.objectFooterStartOffset()
        );

        writer.unsafeWriteInt(writerRootObjStartPos + objDesc.objectStartOffset() + HASH_CODE_POS, hashCode);
    }

    /** */
    private int copyObject(int readerObjStartPos, BinaryOutputStream writer) {
        int readerRetPos = reader.position();
        int writerObjStartPos = writer.position();

        reader.position(readerObjStartPos);

        ObjectDetachHelper detachHelper = create(reader.array(), readerObjStartPos);

        if (detachHelper.isCrossObjectReferencesDetected())
            detachHelper.detach(writer);
        else
            reader.copyObject(writer);

        reader.position(readerRetPos);

        return writerObjStartPos;
    }

    /** */
    private static class ObjectDescriptor implements Comparable<ObjectDescriptor> {
        /** */
        private final int objStartOffset;

        /** */
        private final int objDataStartOffset;

        /** */
        private final int objFooterStartOffset;

        /** */
        public ObjectDescriptor(int objStartOffset, int objDataStartOffset, int objFooterStartOffset) {
            this.objStartOffset = objStartOffset;
            this.objDataStartOffset = objDataStartOffset;
            this.objFooterStartOffset = objFooterStartOffset;
        }

        /** */
        public int objectFooterStartOffset() {
            return objFooterStartOffset;
        }

        /** */
        public int objectDataStartOffset() {
            return objDataStartOffset;
        }

        /** */
        public int objectStartOffset() {
            return objStartOffset;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull ObjectDetachHelper.ObjectDescriptor other) {
            return Integer.compare(objStartOffset, other.objDataStartOffset);
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof ObjectDescriptor))
                return false;

            ObjectDescriptor that = (ObjectDescriptor)o;

            return objStartOffset == that.objStartOffset;
        }
    }
}
