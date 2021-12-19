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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link BuiltInMarshalling}.
 */
class BuiltInMarshallingTest {
    @Test
    void writeAndReadOfBigDecimalProducesSameValueWithNonNegativeScale() throws Exception {
        BigDecimal original = new BigDecimal("1.23");

        byte[] bytes = write(original, BuiltInMarshalling::writeBigDecimal);
        BigDecimal writeReadResult = BuiltInMarshalling.readBigDecimal(new DataInputStream(new ByteArrayInputStream(bytes)));

        assertThat(writeReadResult, is(equalTo(original)));
    }

    private <T> byte[] write(T object, OutputWriter<T> writer) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            writer.write(object, dos);
        }
        return baos.toByteArray();
    }

    @Test
    void writeAndReadOfBigDecimalProducesSameValueEvenWithNegativeScale() throws Exception {
        BigDecimal bdWithNegativeScale = new BigDecimal("123.45").setScale(-1, RoundingMode.HALF_UP);

        byte[] bytes = write(bdWithNegativeScale, BuiltInMarshalling::writeBigDecimal);
        BigDecimal writeReadResult = BuiltInMarshalling.readBigDecimal(new DataInputStream(new ByteArrayInputStream(bytes)));

        assertThat(writeReadResult, is(equalTo(bdWithNegativeScale)));
    }

    private interface OutputWriter<T> {
        void write(T object, DataOutput output) throws IOException;
    }
}
