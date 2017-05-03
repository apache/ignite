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
    /**
     * @throws Exception If failed.
     */
    public void testUnsignedVarint() throws Exception {
        BinaryWriterExImpl writer = createWriter();

        int[] ints = new int[] {0, 10, 200, 3000, 40_000, 123_545, Integer.MAX_VALUE};

        for (int i = 0; i < ints.length; i++)
            writer.doWriteUnsignedVarint(ints[i]);

        BinaryHeapInputStream in = new BinaryHeapInputStream(writer.array());

        for (int i = 0; i < ints.length; i++)
            assertEquals(ints[i], BinaryUtils.doReadUnsignedVarint(in));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSignedVarint() throws Exception {
        BinaryWriterExImpl writer = createWriter();

        int[] ints = new int[] {Integer.MIN_VALUE, -90_000, -5678, -234, -11, 0, 10, 200, 3000, 40_000, 123_545, Integer.MAX_VALUE};

        for (int i = 0; i < ints.length; i++)
            writer.doWriteSignedVarint(ints[i]);

        BinaryHeapInputStream in = new BinaryHeapInputStream(writer.array());

        for (int i = 0; i < ints.length; i++)
            assertEquals(ints[i], BinaryUtils.doReadSignedVarint(in));
    }

    /** */
    private BinaryWriterExImpl createWriter() {
        return new BinaryWriterExImpl(new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), new NullLogger()));
    }
}
