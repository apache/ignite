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

package org.apache.ignite.internal.processors.cache.persistence.wal.crc;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class FastCrcTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testCrcSameSimple() {
        FastCrc crc = new FastCrc();

        int sz = 1024 * 1024;

        byte[] data = new byte[sz];

        ThreadLocalRandom.current().nextBytes(data);

        ByteBuffer bufA = ByteBuffer.wrap(data);

        crc.reset();

        crc.update(bufA, sz);

        int crc1 = crc.getValue();

        int l1 = 100;

        int l2 = sz - l1;

        crc.reset();

        crc.update(ByteBuffer.wrap(data, 0, l1), l1);

        crc.update(ByteBuffer.wrap(data, l1, l2), l2);

        int crc2 = crc.getValue();

        assertEquals(crc1, crc2);
    }

    /** */
    @Test
    public void testCrcSameExtended() {
        FastCrc crc = new FastCrc();

        Random rand = ThreadLocalRandom.current();

        IntStream.range(0, 100).forEach(i -> {
            int sz = 10 + rand.nextInt(1024 * 1024);

            byte[] data = new byte[sz];

            rand.nextBytes(data);

            ByteBuffer bufA = ByteBuffer.wrap(data);

            crc.reset();

            crc.update(bufA, sz);

            int crc1 = crc.getValue();

            crc.reset();

            Stream.concat(
                Stream.of(0, sz),
                rand.ints(0, sz).boxed().limit(1 + rand.nextInt(10))
                ).distinct().sorted()
                .reduce((from, to) -> {
                    int len = to - from;

                    crc.update(ByteBuffer.wrap(data, from, len), len);

                    return to;
                });

            int crc2 = crc.getValue();

            assertEquals(crc1, crc2);
        });
    }
}
