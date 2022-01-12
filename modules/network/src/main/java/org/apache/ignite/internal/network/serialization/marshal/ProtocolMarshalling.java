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

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Protocol-wide elements marshalling.
 */
class ProtocolMarshalling {
    /** Maximum number of bytes needed to encode a container length. */
    static final int MAX_LENGTH_BYTE_COUNT = 4;

    static void writeDescriptorOrCommandId(int id, DataOutput output) throws IOException {
        VarInts.writeUnsignedInt(id, output);
    }

    static int readDescriptorOrCommandId(DataInput input) throws IOException {
        return VarInts.readUnsignedInt(input);
    }

    static void writeObjectId(int id, DataOutput output) throws IOException {
        VarInts.writeUnsignedInt(id, output);
    }

    static int readObjectId(DataInput input) throws IOException {
        return VarInts.readUnsignedInt(input);
    }


    static void writeLength(int length, DataOutput output) throws IOException {
        VarInts.writeUnsignedInt(length, output);
    }

    static int readLength(DataInput input) throws IOException {
        return VarInts.readUnsignedInt(input);
    }

    private ProtocolMarshalling() {
    }
}
