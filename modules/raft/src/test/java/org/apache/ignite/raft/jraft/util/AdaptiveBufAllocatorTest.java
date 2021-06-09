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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class AdaptiveBufAllocatorTest {

    private AdaptiveBufAllocator.Handle handle;

    /*
     * The allocate size table:
     *
     * [16, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 272, 288, 304, 320, 336, 352, 368,
     * 384, 400, 416, 432, 448, 464, 480, 496, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
     * 524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912,
     * 1073741824]
     */

    @Before
    public void setup() {
        this.handle = new AdaptiveBufAllocator(64, 512, 524288).newHandle();
    }

    @Test
    public void incrementTest() {
        allocReadExpected(this.handle, 512);
        allocReadExpected(this.handle, 8192);
        allocReadExpected(this.handle, 131072);
        allocReadExpected(this.handle, 524288);
        allocRead(this.handle, 524288, 8388608);
    }

    @Test
    public void decreaseTest() {
        allocRead(this.handle, 512, 16);
        allocRead(this.handle, 512, 16);
        allocRead(this.handle, 496, 16);
        allocRead(this.handle, 496, 16);
        allocRead(this.handle, 480, 16);
        allocRead(this.handle, 480, 16);
        allocRead(this.handle, 464, 16);
        allocRead(this.handle, 464, 16);
    }

    private static void allocReadExpected(final AdaptiveBufAllocator.Handle handle, final int expectedSize) {
        allocRead(handle, expectedSize, expectedSize);
    }

    private static void allocRead(final AdaptiveBufAllocator.Handle handle, final int expectedSize,
        final int lastRead) {
        assertEquals(expectedSize, handle.allocate().capacity());
        handle.record(lastRead);
    }
}
