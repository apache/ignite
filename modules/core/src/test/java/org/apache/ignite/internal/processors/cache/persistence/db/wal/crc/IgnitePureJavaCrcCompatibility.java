/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal.crc;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * PureJavaCrc32 previous crc algo realization vs java.util.zip.crc32 test.
 */
public class IgnitePureJavaCrcCompatibility {
    /**
     * Test crc algo equality results.
     * @throws Exception
     */
    @Test
    public void testAlgoEqual() throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(1024);

        ThreadLocalRandom curr = ThreadLocalRandom.current();

        for (int i = 0; i < 1024; i += 16) {
            buf.putInt(curr.nextInt());
            buf.putInt(curr.nextInt());
            buf.putInt(curr.nextInt());
            buf.position(i);

            buf.position(i);
            int crc0 = FastCrc.calcCrc(buf, 12);

            buf.position(i);
            int crc1 = PureJavaCrc32.calcCrc32(buf, 12);

            assertEquals(crc0, crc1);
        }
    }
}
