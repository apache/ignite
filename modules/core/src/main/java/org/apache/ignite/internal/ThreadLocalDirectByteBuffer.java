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

package org.apache.ignite.internal;

import java.nio.ByteBuffer;

/**
 * Thread local direct byte buffer.
 */
public class ThreadLocalDirectByteBuffer extends ThreadLocal<ByteBuffer> {
    /** */
    final int size;

    /** */
    public ThreadLocalDirectByteBuffer() {
        this(-1); // Avoiding useless initialization.
    }

    /** */
    public ThreadLocalDirectByteBuffer(int size) {
        this.size = size;
    }

    /** {@inheritDoc} */
    @Override protected ByteBuffer initialValue() {
        return size > 0 ? allocateDirectBuffer(size) : null;
    }

    /** */
    public ByteBuffer get(int capacity) {
        assert capacity > 0 : capacity;

        ByteBuffer buf = super.get();

//        if (buf == null || buf.capacity() < capacity) {
            buf = allocateDirectBuffer(capacity);

            set(buf);
//        }
//        else
//            buf.clear();

        return buf;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer get() {
        assert size > 0 : size;

        ByteBuffer buf = super.get();

        buf.clear();

        return buf;
    }

    /** */
    protected ByteBuffer allocateDirectBuffer(int capacity) {
        return ByteBuffer.allocateDirect(capacity);
    }
}
