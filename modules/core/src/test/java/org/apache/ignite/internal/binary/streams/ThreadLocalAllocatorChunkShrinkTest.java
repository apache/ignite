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

package org.apache.ignite.internal.binary.streams;

import java.lang.reflect.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test should demonstrate how ThreadLocalAllocator$Chunk shrinks if a small message is written after a large one.
 * <p>
 * Shrink logic is executed only if enough time has passed to check size again. Therefore, we modify the last check
 * timestamp according to binary memory allocator check frequency. The small message size is set to less than half of
 * the buffer size for the large message to make the chunk shrink.
 * </p>
 */
public class ThreadLocalAllocatorChunkShrinkTest {
    /** output stream */
    private BinaryHeapOutputStream outputStream;

    /** Clear Thread-Local chunk to equalize test startup condition. */
    @Before
    public void init() {
        clearThreadLocalForBinaryMemoryAllocatorChunk();
    }

    /**
     * First writes a large message to the stream then a small. If shrinking does not happen stream and chunk array are
     * the same which is why we use {@link BinaryStream#array()} for access. It should fit the large message first.
     * After writing the small message it should be reduced to half of its former size.
     */
    @Test
    public void testThreadLocalBufferShrinksAfterLargeMessage() {

        int initSize = 128;
        int largeMsgSize = 1024;

        outputStream = new BinaryHeapOutputStream(initSize);
        outputStream.writeByteArray(new byte[largeMsgSize]);
        forceShrinkCheck();
        // Closing the stream invokes BinaryMemoryAllocatorChunk#release(...) and thus the size check
        closeOutputStream();

        // new stream reuses thread-local chunk
        outputStream = new BinaryHeapOutputStream(initSize);
        // Stream array is assigned from chunk array only on creation of stream. Query the new chunk size.
        int largeBufSize = outputStream.array().length;

        assertTrue(largeBufSize >= largeMsgSize);

        outputStream.writeByte((byte)1);
        forceShrinkCheck();
        closeOutputStream();

        outputStream = new BinaryHeapOutputStream(initSize);

        int expectedBufSize = largeBufSize >> 1;
        int actualBufSize = outputStream.array().length;

        assertEquals(expectedBufSize, actualBufSize);
        closeOutputStream();
    }

    /**
     * Clear thread-local to not influence coming up tests. Close stream if not already happened (e.g. in case of
     * assertion error).
     */
    @After
    public void cleanup() {
        closeOutputStream();
        clearThreadLocalForBinaryMemoryAllocatorChunk();
    }

    /** Clears thread-local chunk. Since the field is private we use reflection to gain access. */
    private static void clearThreadLocalForBinaryMemoryAllocatorChunk() {

        try {
            Field holdersField = BinaryMemoryAllocator.THREAD_LOCAL.getClass().getDeclaredField("holders");
            holdersField.setAccessible(true);
            ThreadLocal<BinaryMemoryAllocatorChunk> holders = (ThreadLocal<BinaryMemoryAllocatorChunk>)holdersField
                    .get(BinaryMemoryAllocator.THREAD_LOCAL);
            holders.remove();
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Sets last check timestamp to be {@code BinaryMemoryAllocator#CHECK_FREQ} millis before now. The difference
     * between now and the timestamp will be greater than or equal to the check frequency. Again, reflection is needed
     * to access private fields.
     */
    private static void forceShrinkCheck() {

        try {
            BinaryMemoryAllocatorChunk chunk = BinaryMemoryAllocator.THREAD_LOCAL.chunk();
            Field lastCheckNanosField = chunk.getClass().getDeclaredField("lastCheckNanos");

            lastCheckNanosField.setAccessible(true);

            Field checkFreqField = BinaryMemoryAllocator.class.getDeclaredField("CHECK_FREQ");

            checkFreqField.setAccessible(true);
            // check frequency field is in millis
            long lastCheckNanos = System.nanoTime() - ((long)checkFreqField.get(null)) * 1_000_000;

            lastCheckNanosField.set(chunk, lastCheckNanos);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Closes the output stream if not already happened */
    private void closeOutputStream() {

        if (outputStream != null) {
            outputStream.close();
            outputStream = null;
        }
    }
}
