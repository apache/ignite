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

package org.apache.ignite.internal.processors.cache.persistence.cdc;

import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;
import org.junit.Test;

/** */
public class RealtimeCdcBufferSelfTest {
    /** */
    @Test
    public void testDisableBuffer() {
        CdcBuffer buf = new CdcBuffer(10);

        // Fill the buffer.
        for (int i = 0; i < 10; i++) {
            boolean res = buf.offer(build());

            assert res;
            assert buf.size() == i + 1;
        }

        boolean res = buf.offer(build());

        assert !res;
    }

    /** */
    @Test
    public void testConsumeEmptyBuffer() {
        CdcBuffer buf = new CdcBuffer(10);

        for (int i = 0; i < 10; i++) {
            byte[] data = buf.poll();

            assert data == null;
            assert buf.size() == 0;
        }
    }

    /** */
    @Test
    public void testConsumeFullBuffer() {
        CdcBuffer buf = new CdcBuffer(10);

        for (int i = 0; i < 10; i++)
            buf.offer(build());

        for (int i = 0; i < 10; i++) {
            byte[] data = buf.poll();

            assert Arrays.equals(build(), data);
            assert buf.size() == 10 - (i + 1);
        }

        boolean res = buf.offer(build());

        assert res;
        assert buf.size() == 1;
    }

    /** */
    @Test
    public void testConcurrentFillBuffer() throws Exception {
        int size = 1_000_000;

        CdcBuffer buf = new CdcBuffer(size);

        Thread th1 = new Thread(() -> {
            for (int i = 0; i < size; i++) {
                buf.offer(build());

                LockSupport.parkNanos(1_000);
            }
        });

        Thread th2 = new Thread(() -> {
            // Goal is to invoke `poll` more frequently than `offer`, to check `poll` waits offered value.
            for (int i = 0; i < size * 1_000; i++)
                buf.poll();
        });

        th1.start();
        th2.start();

        th1.join();
        th2.join();
    }

    /** */
    private byte[] build() {
        byte[] data = new byte[1];
        data[0] = 1;

        return data;
    }
}
