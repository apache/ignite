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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.BitSet;
import org.junit.jupiter.api.Test;

class ProtocolMarshallingTest {
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    @Test
    void writesFixedLengthBitSetOf1ZeroBitCorrectly() throws Exception {
        BitSet bitSet = new BitSet(1);

        try (DataOutputStream dos = new DataOutputStream(baos)) {
            ProtocolMarshalling.writeFixedLengthBitSet(bitSet, 1, dos);
        }

        assertThat(baos.toByteArray(), is(new byte[]{0}));
    }

    @Test
    void readFixedLengthBitSetOf1ZeroBitCorrectly() throws Exception {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(new byte[]{0}));
        BitSet bitset = ProtocolMarshalling.readFixedLengthBitSet(1, dis);

        assertThat(bitset.cardinality(), is(0));
    }

    @Test
    void writesFixedLengthBitSetOf1OneBitCorrectly() throws Exception {
        BitSet bitSet = new BitSet(1);
        bitSet.set(0);

        try (DataOutputStream dos = new DataOutputStream(baos)) {
            ProtocolMarshalling.writeFixedLengthBitSet(bitSet, 1, dos);
        }

        assertThat(baos.toByteArray(), is(new byte[]{1}));
    }

    @Test
    void readFixedLengthBitSetOf1OneBitCorrectly() throws Exception {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(new byte[]{1}));
        BitSet bitset = ProtocolMarshalling.readFixedLengthBitSet(1, dis);

        assertThat(bitset.cardinality(), is(1));
        assertThat(bitset.get(0), is(true));
    }

    @Test
    void writesFixedLengthBitSetOf9OneBitsCorrectly() throws Exception {
        BitSet bitSet = new BitSet(9);
        for (int i = 0; i < 9; i++) {
            bitSet.set(i);
        }

        try (DataOutputStream dos = new DataOutputStream(baos)) {
            ProtocolMarshalling.writeFixedLengthBitSet(bitSet, 9, dos);
        }

        assertThat(baos.toByteArray(), is(new byte[]{(byte) 255, 1}));
    }

    @Test
    void readFixedLengthBitSetOf9OneBitsCorrectly() throws Exception {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(new byte[]{(byte) 255, 1}));
        BitSet bitset = ProtocolMarshalling.readFixedLengthBitSet(9, dis);

        assertThat(bitset.cardinality(), is(9));
        assertThat(bitset.length(), is(9));
    }

    @Test
    void writesFixedLengthBitSetWithHoleCorrectly() throws Exception {
        BitSet bitSet = new BitSet(9);
        for (int i = 0; i < 9; i++) {
            if (i != 1) {
                bitSet.set(i);
            }
        }

        try (DataOutputStream dos = new DataOutputStream(baos)) {
            ProtocolMarshalling.writeFixedLengthBitSet(bitSet, 9, dos);
        }

        assertThat(baos.toByteArray(), is(new byte[]{(byte) 253, 1}));
    }

    @Test
    void readFixedLengthBitSetWithHoleCorrectly() throws Exception {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(new byte[]{(byte) 253, 1}));
        BitSet bitset = ProtocolMarshalling.readFixedLengthBitSet(9, dis);

        assertThat(bitset.cardinality(), is(8));
        assertThat(bitset.length(), is(9));
        assertThat(bitset.get(1), is(false));
    }
}
