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
public class BinaryStreamsFactoryImpl implements BinaryStreamsFactory {
    /** {@inheritDoc} */
    @Override public BinaryInputStream inputStream(byte[] data, int pos) {
        return BinaryHeapInputStream.create(data, pos);
    }

    /** {@inheritDoc} */
    @Override public BinaryInputStream inputStream(byte[] data) {
        return new BinaryHeapInputStream(data);
    }

    /** {@inheritDoc} */
    @Override public BinaryInputStream inputStream(ByteBuffer buf) {
        return new BinaryByteBufferInputStream(buf);
    }

    /** {@inheritDoc} */
    @Override public BinaryInputStream inputStream(long ptr, int cap) {
        return new BinaryOffheapInputStream(ptr, cap);
    }

    /** {@inheritDoc} */
    @Override public BinaryInputStream inputStream(long ptr, int cap, boolean forceHeap) {
        return new BinaryOffheapInputStream(ptr, cap, forceHeap);
    }

    /** {@inheritDoc} */
    @Override public BinaryOutputStream createPooledOutputStream(int cap, boolean disableAutoClose) {
        return new BinaryHeapOutputStream(cap, BinaryMemoryAllocator.POOLED.chunk(), disableAutoClose);
    }

    /** {@inheritDoc} */
    @Override public BinaryOutputStream outputStream(int cap) {
        return new BinaryHeapOutputStream(cap);
    }

    /** {@inheritDoc} */
    @Override public BinaryOutputStream outputStream(int cap, BinaryMemoryAllocatorChunk chunk) {
        return new BinaryHeapOutputStream(cap, chunk);
    }

    /** {@inheritDoc} */
    @Override public BinaryMemoryAllocatorChunk threadLocalChunk() {
        return BinaryMemoryAllocator.THREAD_LOCAL.chunk();
    }
}
