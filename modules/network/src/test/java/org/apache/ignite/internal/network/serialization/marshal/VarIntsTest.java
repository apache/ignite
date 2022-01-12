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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class VarIntsTest {
    @ParameterizedTest
    @MethodSource("intRangeBorders")
    void writesIntsWithCorrectLengths(IntWriteSpec spec) throws Exception {
        byte[] bytes = writeToBytes(output -> VarInts.writeUnsignedInt(spec.value, output));

        assertThat(bytes.length, is(spec.expectedLength));
    }

    private static Stream<Arguments> intRangeBorders() {
        return Stream.of(
                new IntWriteSpec(0, 1),
                new IntWriteSpec(0xFE, 1),
                new IntWriteSpec(0xFF, 3),
                new IntWriteSpec(0xFFFE, 3),
                new IntWriteSpec(0xFFFF, 7),
                new IntWriteSpec(Integer.MAX_VALUE, 7)
        ).map(Arguments::of);
    }

    private byte[] writeToBytes(StreamWriter streamWriter) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (var dos = new DataOutputStream(baos)) {
            streamWriter.write(dos);
        }
        return baos.toByteArray();
    }

    private int readIntFromBytesConsuming(byte[] bytes, StreamReader streamReader) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (var dos = new DataInputStream(bais)) {
            int result = streamReader.read(dos);
            assertThat("Stream was not fully consumed", dos.read(), is(lessThan(0)));
            return result;
        }
    }

    @ParameterizedTest
    @MethodSource("intRangeBorders")
    void writesAndReadsInts(IntWriteSpec spec) throws Exception {
        byte[] bytes = writeToBytes(output -> VarInts.writeUnsignedInt(spec.value, output));
        int result = readIntFromBytesConsuming(bytes, VarInts::readUnsignedInt);

        assertThat(result, is(spec.value));
    }

    private interface StreamWriter {
        void write(DataOutput output) throws IOException;
    }

    private interface StreamReader {
        int read(DataInput input) throws IOException;
    }

    private static class IntWriteSpec {
        private final int value;
        private final int expectedLength;

        private IntWriteSpec(int value, int expectedLength) {
            this.value = value;
            this.expectedLength = expectedLength;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return Integer.toString(value);
        }
    }
}
