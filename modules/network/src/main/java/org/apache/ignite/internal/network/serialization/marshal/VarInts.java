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
 * Utils to read/write variable length ints.
 */
class VarInts {
    private VarInts() {
    }

    /**
     * Writes a unsigned int using variable length format. If it's less than 0xFF, it's written as one byte.
     * If it's more than 0xFE, but less than 0xFFFF, it's written as 3 bytes: first one byte equal to 0xFF, then 2 bytes
     * as {@link DataOutput#writeShort(int)} writes them. Otherwise, it writes 3 0xFF bytes, then writes
     * {@link DataOutput#writeInt(int)}.
     * This may be beneficial for the cases when we need to write an unsigned int, but most of the time the values
     * are small.
     *
     * @param value  value to write
     * @param output where to write to value to
     * @throws IOException if an I/O error occurs
     */
    static void writeUnsignedInt(int value, DataOutput output) throws IOException {
        if (value < 0) {
            throw new IllegalArgumentException(value + " is negative");
        }

        if (value < 0xFF) {
            output.writeByte(value);
        } else if (value < 0xFFFF) {
            output.writeByte(0xFF);
            output.writeShort(value);
        } else {
            output.writeByte(0xFF);
            output.writeShort(0xFFFF);
            output.writeInt(value);
        }
    }

    /**
     * Reads an unsigned int written using {@link #writeUnsignedInt(int, DataOutput)}.
     *
     * @param input from where to read
     * @return the unsigned int value
     * @throws IOException if an I/O error occurs
     * @see #writeUnsignedInt(int, DataOutput)
     */
    static int readUnsignedInt(DataInput input) throws IOException {
        int first = input.readUnsignedByte();
        if (first < 0xFF) {
            return first;
        }

        int second = input.readUnsignedShort();
        if (second < 0xFFFF) {
            return second;
        }

        int third = input.readInt();
        if (third < 0) {
            throw new IllegalStateException(third + " is negative");
        }

        return third;
    }
}
