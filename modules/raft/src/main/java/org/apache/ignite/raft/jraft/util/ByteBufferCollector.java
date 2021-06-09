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

import java.nio.ByteBuffer;

/**
 * A byte buffer collector that will expand automatically.
 */
public final class ByteBufferCollector implements Recyclable {

    private static final int MAX_CAPACITY_TO_RECYCLE = 4 * 1024 * 1024; // 4M

    private ByteBuffer buffer;

    public int capacity() {
        return this.buffer != null ? this.buffer.capacity() : 0;
    }

    public void expandIfNecessary() {
        if (!hasRemaining()) {
            getBuffer(Utils.RAFT_DATA_BUF_SIZE);
        }
    }

    public void expandAtMost(final int atMostBytes) {
        if (this.buffer == null) {
            this.buffer = Utils.allocate(atMostBytes);
        }
        else {
            this.buffer = Utils.expandByteBufferAtMost(this.buffer, atMostBytes);
        }
    }

    public boolean hasRemaining() {
        return this.buffer != null && this.buffer.hasRemaining();
    }

    private ByteBufferCollector(final int size, final Recyclers.Handle handle) {
        if (size > 0) {
            this.buffer = Utils.allocate(size);
        }
        this.handle = handle;
    }

    public static ByteBufferCollector allocate(final int size) {
        return new ByteBufferCollector(size, Recyclers.NOOP_HANDLE);
    }

    public static ByteBufferCollector allocate() {
        return allocate(Utils.RAFT_DATA_BUF_SIZE);
    }

    public static ByteBufferCollector allocateByRecyclers(final int size) {
        final ByteBufferCollector collector = recyclers.get();
        collector.reset(size);
        return collector;
    }

    public static ByteBufferCollector allocateByRecyclers() {
        return allocateByRecyclers(Utils.RAFT_DATA_BUF_SIZE);
    }

    public static int threadLocalCapacity() {
        return recyclers.threadLocalCapacity();
    }

    public static int threadLocalSize() {
        return recyclers.threadLocalSize();
    }

    private void reset(final int expectSize) {
        if (this.buffer == null) {
            this.buffer = Utils.allocate(expectSize);
        }
        else {
            if (this.buffer.capacity() < expectSize) {
                this.buffer = Utils.allocate(expectSize);
            }
        }
    }

    private ByteBuffer getBuffer(final int expectSize) {
        if (this.buffer == null) {
            this.buffer = Utils.allocate(expectSize);
        }
        else if (this.buffer.remaining() < expectSize) {
            this.buffer = Utils.expandByteBufferAtLeast(this.buffer, expectSize);
        }
        return this.buffer;
    }

    public void put(final ByteBuffer buf) {
        getBuffer(buf.remaining()).put(buf);
    }

    public void put(final byte[] bs) {
        getBuffer(bs.length).put(bs);
    }

    public void setBuffer(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public ByteBuffer getBuffer() {
        return this.buffer;
    }

    @Override
    public boolean recycle() {
        if (this.buffer != null) {
            if (this.buffer.capacity() > MAX_CAPACITY_TO_RECYCLE) {
                // If the size is too large, we should release it to avoid memory overhead
                this.buffer = null;
            }
            else {
                this.buffer.clear();
            }
        }
        return recyclers.recycle(this, handle);
    }

    private transient final Recyclers.Handle handle;

    // TODO asch fixme is it safe to have static recyclers ? IGNITE-14832
    private static final Recyclers<ByteBufferCollector> recyclers = new Recyclers<ByteBufferCollector>(
        Utils.MAX_COLLECTOR_SIZE_PER_THREAD) {

        @Override
        protected ByteBufferCollector newObject(final Handle handle) {
            return new ByteBufferCollector(0, handle);
        }
    };
}
