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

package org.apache.ignite.internal.processors.cache.persistence.wal.mmap.optane;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.wal.mmap.AbstractByteBufferHolder;

/**
 * PMDK backed {@link ByteBuffer} holder.
 */
class OptaneByteBufferHolder extends AbstractByteBufferHolder {
    /** */
    private final Type type;

    /**
     * @param ctx Native extension context.
     */
    OptaneByteBufferHolder(Context ctx) {
        super(ctx.buffer());
        type = ctx.isPmem() ? Type.OPTANE : Type.MMAP;
    }

    /** {@inheritDoc */
    @Override public Type type() {
        checkValidState();
        return type;
    }

    /** {@inheritDoc */
    @Override public ByteBuffer buffer() {
        checkValidState();
        return super.buffer();
    }

    /** {@inheritDoc */
    @Override public void msync(int off, int len) {
        checkValidState();
        OptaneUtil.msync(buffer(), off, len, type == Type.OPTANE);
    }

    /** {@inheritDoc */
    @Override public void msync() {
        checkValidState();
        msync(0, buffer().capacity());
    }

    /** {@inheritDoc */
    @Override public void free() {
        if (closed)
            return;

        ByteBuffer buf = buffer();
        closed = true;
        OptaneUtil.munmap(buf);
    }
}
