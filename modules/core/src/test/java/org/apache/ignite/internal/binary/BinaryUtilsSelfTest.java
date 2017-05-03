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

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link BinaryUtils}.
 */
public class BinaryUtilsSelfTest extends GridCommonAbstractTest {
    /** */
    private int[] ints;

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ints = new int[] {Integer.MIN_VALUE, Short.MIN_VALUE, -90_000, Byte.MIN_VALUE, -5678, -234, -11, 0, 10, 200, 3000, Byte.MAX_VALUE, Short.MAX_VALUE, 40_000, 123_545, Integer.MAX_VALUE};
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnsignedVarint() throws Exception {
        BinaryWriterExImpl writer = createWriter();

        int len = 0;

        for (int i = 0; i < ints.length; i++) {
            writer.doWriteUnsignedVarint(ints[i]);

            assertEquals((len += BinaryUtils.sizeInUnsignedVarint(ints[i])), writer.array().length);
        }

        BinaryHeapInputStream in = new BinaryHeapInputStream(writer.array());

        for (int i = 0; i < ints.length; i++)
            assertEquals(ints[i], BinaryUtils.doReadUnsignedVarint(in));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSignedVarint() throws Exception {
        BinaryWriterExImpl writer = createWriter();

        int len = 0;

        for (int i = 0; i < ints.length; i++) {
            writer.doWriteSignedVarint(ints[i]);

            assertEquals((len += BinaryUtils.sizeInSignedVarint(ints[i])), writer.array().length);
        }

        BinaryHeapInputStream in = new BinaryHeapInputStream(writer.array());

        for (int i = 0; i < ints.length; i++)
            assertEquals(ints[i], BinaryUtils.doReadSignedVarint(in));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSizeInUnsignedVarint() throws Exception {
        assertEquals(1, BinaryUtils.sizeInUnsignedVarint(0));
        assertEquals(1, BinaryUtils.sizeInUnsignedVarint(Byte.MAX_VALUE));
        assertEquals(2, BinaryUtils.sizeInUnsignedVarint(Byte.MAX_VALUE + 1));

        assertEquals(2, BinaryUtils.sizeInUnsignedVarint(128 * 128 - 1));
        assertEquals(3, BinaryUtils.sizeInUnsignedVarint(128 * 128));

        assertEquals(3, BinaryUtils.sizeInUnsignedVarint(128 * 128 * 128 - 1));
        assertEquals(4, BinaryUtils.sizeInUnsignedVarint(128 * 128 * 128));

        assertEquals(4, BinaryUtils.sizeInUnsignedVarint(128 * 128 * 128 * 128 - 1));
        assertEquals(5, BinaryUtils.sizeInUnsignedVarint(128 * 128 * 128 * 128));
        assertEquals(5, BinaryUtils.sizeInUnsignedVarint(Integer.MAX_VALUE));

        // negative values
        assertEquals(5, BinaryUtils.sizeInUnsignedVarint(-1));
        assertEquals(5, BinaryUtils.sizeInUnsignedVarint(Byte.MIN_VALUE));
        assertEquals(5, BinaryUtils.sizeInUnsignedVarint(Short.MIN_VALUE));
        assertEquals(5, BinaryUtils.sizeInUnsignedVarint(Integer.MIN_VALUE));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSizeInSignedVarint() throws Exception {
        // positive values
        assertEquals(1, BinaryUtils.sizeInSignedVarint(0));
        assertEquals(1, BinaryUtils.sizeInSignedVarint(63));
        assertEquals(2, BinaryUtils.sizeInSignedVarint(64));
        assertEquals(2, BinaryUtils.sizeInSignedVarint(64 * 128 - 1));
        assertEquals(3, BinaryUtils.sizeInSignedVarint(64 * 128));
        assertEquals(3, BinaryUtils.sizeInSignedVarint(64 * 128 * 128 - 1));
        assertEquals(4, BinaryUtils.sizeInSignedVarint(64 * 128 * 128));
        assertEquals(4, BinaryUtils.sizeInSignedVarint(64 * 128 * 128 * 128 - 1));
        assertEquals(5, BinaryUtils.sizeInSignedVarint(64 * 128 * 128 * 128));
        assertEquals(5, BinaryUtils.sizeInSignedVarint(Integer.MAX_VALUE));

        // negative values
        assertEquals(1, BinaryUtils.sizeInSignedVarint(-0));
        assertEquals(1, BinaryUtils.sizeInSignedVarint(-64));
        assertEquals(2, BinaryUtils.sizeInSignedVarint(-65));
        assertEquals(2, BinaryUtils.sizeInSignedVarint(-64 * 128));
        assertEquals(3, BinaryUtils.sizeInSignedVarint(-64 * 128 - 1));
        assertEquals(3, BinaryUtils.sizeInSignedVarint(-64 * 128 * 128));
        assertEquals(4, BinaryUtils.sizeInSignedVarint(-64 * 128 * 128 - 1));
        assertEquals(4, BinaryUtils.sizeInSignedVarint(-64 * 128 * 128 * 128));
        assertEquals(5, BinaryUtils.sizeInSignedVarint(-64 * 128 * 128 * 128 - 1));
        assertEquals(5, BinaryUtils.sizeInSignedVarint(Integer.MIN_VALUE));
    }

    /** */
    private BinaryWriterExImpl createWriter() {
        return new BinaryWriterExImpl(new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), new NullLogger()));
    }
}
