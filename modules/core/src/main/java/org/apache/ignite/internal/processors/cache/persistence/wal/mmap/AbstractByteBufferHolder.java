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

package org.apache.ignite.internal.processors.cache.persistence.wal.mmap;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridUnsafe;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Implements common methods of {@link ByteBufferHolder}
 */
public abstract class AbstractByteBufferHolder implements ByteBufferHolder {
    /** */
    private final ByteBuffer buf;

    /** */
    protected boolean closed;

    /**
     * @param buf Holdel buffer.
     */
    protected AbstractByteBufferHolder(ByteBuffer buf) {
        this.buf = buf;
        this.buf.order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc */
    @Override public int capacity() {
        return buf.capacity();
    }

    /** {@inheritDoc */
    @Override public void msync(int off, int len) throws IgniteCheckedException {
        throw new NotImplementedException();
    }

    /** {@inheritDoc */
    @Override public void msync() throws IgniteCheckedException {
        throw new NotImplementedException();
    }

    /** {@inheritDoc */
    @Override public ByteBuffer buffer() {
        checkValidState();
        return buf;
    }

    /** {@inheritDoc */
    @Override public void close() {
        free();
    }

    /** {@inheritDoc */
    @Override public void free() {
        if (type() == Type.ONHEAP)
            return;

        if (closed)
            return;

        assert buf.isDirect() && type() != Type.OPTANE;
        closed = true;
        GridUnsafe.cleanDirectBuffer(buf);
    }

    /** */
    protected void checkValidState() {
        if (closed)
            throw new RuntimeException("Buffer has been already closed.");
    }
}
