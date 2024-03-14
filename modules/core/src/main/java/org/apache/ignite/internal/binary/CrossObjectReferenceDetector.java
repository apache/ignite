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

import org.apache.ignite.internal.binary.streams.BinaryInputStream;

import static org.apache.ignite.internal.binary.BinaryUtils.dataStartRelative;
import static org.apache.ignite.internal.binary.BinaryUtils.length;
import static org.apache.ignite.internal.binary.BinaryUtils.rawOffsetAbsolute;

/** */
class CrossObjectReferenceDetector {
    /** */
    private final RawBytesObjectReader reader;

    /** */
    private int objLeftBoundaryPos;

    /** */
    CrossObjectReferenceDetector(BinaryInputStream in) {
        reader = new RawBytesObjectReader(in);
    }

    /** */
    boolean checkObject() {
        objLeftBoundaryPos = reader.position();

        assert reader.peekByte() == GridBinaryMarshaller.OBJ;

        return findInNextObject();
    }

    /** */
    private boolean findInNextObject() {
        int objStartPos = reader.position();

        byte objType = reader.readBytePositioned(objStartPos);

        switch (objType) {
            case GridBinaryMarshaller.OBJ: {
                int objDataStartPos = objStartPos + dataStartRelative(reader, objStartPos);
                int objDataEndPos = rawOffsetAbsolute(reader, objStartPos);
                int objEndPos = objStartPos + length(reader, objStartPos);

                reader.position(objDataStartPos);

                boolean isFound = false;

                while (reader.position() < objDataEndPos) {
                    if (findInNextObject()) {
                        isFound = true;

                        break;
                    }
                }

                reader.position(objEndPos);

                return isFound;
            }

            case GridBinaryMarshaller.HANDLE: {
                reader.skipBytes(1); // Object type.

                int offset = reader.readInt();

                return objStartPos - offset < objLeftBoundaryPos;
            }

            case GridBinaryMarshaller.OBJ_ARR: {
                reader.skipBytes(1); // Object type.

                reader.skipTypeId();

                int size = reader.readInt();

                return findInNextCortege(size);
            }

            case GridBinaryMarshaller.COL: {
                reader.skipBytes(1); // Object type.

                int size = reader.readInt();

                reader.skipBytes(1); // Collection type.

                return findInNextCortege(size);
            }

            case GridBinaryMarshaller.MAP: {
                reader.skipBytes(1); // Object type.

                int size = reader.readInt() * 2;

                reader.skipBytes(1); // Map type.

                return findInNextCortege(size);
            }

            default: {
                reader.skipObject();

                return false;
            }
        }
    }

    /** */
    private boolean findInNextCortege(int cortegeSize) {
        for (int elemIdx = 0; elemIdx < cortegeSize; elemIdx++) {
            if (findInNextObject())
                return true;
        }

        return false;
    }
}
