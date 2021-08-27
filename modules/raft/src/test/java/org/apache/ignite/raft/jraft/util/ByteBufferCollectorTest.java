/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 *
 */
public class ByteBufferCollectorTest {

    @Test
    public void testSmallSizeRecycle() {
        final ByteBufferCollector object = ByteBufferCollector.allocateByRecyclers(4096);
        object.recycle();
        assertEquals(4096, ByteBufferCollector.allocateByRecyclers().capacity());
    }

    @Test
    public void testLargeSizeRecycle() {
        final ByteBufferCollector object = ByteBufferCollector.allocateByRecyclers(4 * 1024 * 1024 + 1);
        object.recycle();
        assertEquals(Utils.RAFT_DATA_BUF_SIZE, ByteBufferCollector.allocateByRecyclers().capacity());
    }

    @Test
    public void testMultipleRecycle() {
        final ByteBufferCollector object = ByteBufferCollector.allocateByRecyclers();
        object.recycle();
        assertThrows(IllegalStateException.class, () -> object.recycle());
    }

    @Test
    public void testMultipleRecycleAtDifferentThread() throws InterruptedException {
        final ByteBufferCollector object = ByteBufferCollector.allocateByRecyclers();
        final Thread thread1 = new Thread(object::recycle);
        try {
            thread1.start();
        } finally {
            thread1.join();
        }
        assertSame(object, ByteBufferCollector.allocateByRecyclers());
    }

    @Test
    public void testRecycle() {
        final ByteBufferCollector object = ByteBufferCollector.allocateByRecyclers();
        object.recycle();
        final ByteBufferCollector object2 = ByteBufferCollector.allocateByRecyclers();
        assertSame(object, object2);
        object2.recycle();
    }
}
