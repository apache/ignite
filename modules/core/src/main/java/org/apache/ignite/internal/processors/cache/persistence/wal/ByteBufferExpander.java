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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * ByteBuffer wrapper for dynamically expand buffer size.
 */
public class ByteBufferExpander implements AutoCloseable {
    /** Byte buffer */
    private ByteBuffer buf;

    /**
     * @param initSize Initial size.
     * @param order Byte order.
     */
    public ByteBufferExpander(int initSize, ByteOrder order) {
        ByteBuffer buffer = GridUnsafe.allocateBuffer(initSize);
        buffer.order(order);

        buf = buffer;
    }

    /**
     * Current byte buffer.
     *
     * @return Current byteBuffer.
     */
    public ByteBuffer buffer() {
        return buf;
    }

    /**
     * Expands current byte buffer to the requested size.
     *
     * @return ByteBuffer with requested size.
     */
    public ByteBuffer expand(int size) {
        assert buf.capacity() < size;

        int pos = buf.position();
        int lim = buf.limit();

        ByteBuffer newBuf = GridUnsafe.reallocateBuffer(buf, size);

        newBuf.order(buf.order());
        newBuf.position(pos);
        newBuf.limit(lim);

        buf = newBuf;

        return newBuf;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        GridUnsafe.freeBuffer(buf);
    }
}
