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

import java.nio.ByteBuffer;

/**
 * Utility class to provide static methods to create {@link BinaryInputStream} or {@link BinaryOutputStream} in different modes.
 */
public class BinaryStreams {
    /**
     * Create stream with pointer set at the given position.
     *
     * @param data Data.
     * @param pos Position.
     * @return Stream.
     */
    public static BinaryInputStream inputStream(byte[] data, int pos) {
        return BinaryHeapInputStream.create(data, pos);
    }

    /**
     * Create stream with pointer set at the given position.
     *
     * @param data Data.
     * @return Stream.
     */
    public static BinaryInputStream inputStream(byte[] data) {
        return new BinaryHeapInputStream(data);
    }

    /**
     * @param buf Buffer to wrap.
     * @return Stream.
     */
    public static BinaryInputStream inputStream(ByteBuffer buf) {
        return new BinaryByteBufferInputStream(buf);
    }

    /**
     * @param ptr Pointer.
     * @param cap Capacity.
     * @return Stream.
     */
    public static BinaryInputStream inputStream(long ptr, int cap) {
        return new BinaryOffheapInputStream(ptr, cap);
    }

    /**
     * @param ptr Pointer.
     * @param cap Capacity.
     * @param forceHeap If {@code true} method {@link BinaryInputStream#offheapPointer()} returns 0 and unmarshalling will
     *        create heap-based objects.
     * @return Stream.
     */
    public static BinaryInputStream inputStream(long ptr, int cap, boolean forceHeap) {
        return new BinaryOffheapInputStream(ptr, cap, forceHeap);
    }

    /**
     * @param cap Capacity.
     * @param disableAutoClose Whether to disable resource release in {@link BinaryOutputStream#close()} method
     *                         so that an explicit {@link BinaryOutputStream#release()} call is required.
     * @return Binary output stream data.
     */
    public static BinaryOutputStream createPooledOutputStream(int cap, boolean disableAutoClose) {
        return new BinaryHeapOutputStream(cap, BinaryMemoryAllocator.POOLED.chunk(), disableAutoClose);
    }

    /**
     * @param cap Capacity.
     * @return Binary output stream data.
     */
    public static BinaryOutputStream outputStream(int cap) {
        return new BinaryHeapOutputStream(cap);
    }

    /**
     * @param cap Capacity.
     * @param chunk Memory allocator chunk.
     * @return Binary output stream.
     */
    public static BinaryOutputStream outputStream(int cap, BinaryMemoryAllocatorChunk chunk) {
        return new BinaryHeapOutputStream(cap, chunk);
    }

    /**
     * @return Thread local binary memory allocator.
     */
    public static BinaryMemoryAllocatorChunk threadLocalChunk() {
        return BinaryMemoryAllocator.THREAD_LOCAL.chunk();
    }
}
