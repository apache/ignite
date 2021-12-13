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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridUnsafe;

import static org.apache.ignite.internal.util.IgniteUtils.findField;
import static org.apache.ignite.internal.util.IgniteUtils.findNonPublicMethod;

/**
 * Mapped byte buffer holder.
 */
public class MappedByteBufferHolder extends AbstractByteBufferHolder {
    /** {@link MappedByteBuffer#force0(java.io.FileDescriptor, long, long)}. */
    private static final Method force0 = findNonPublicMethod(
        MappedByteBuffer.class, "force0",
        java.io.FileDescriptor.class, long.class, long.class
    );

    /** {@link MappedByteBuffer#mappingOffset()}. */
    private static final Method mappingOffset = findNonPublicMethod(MappedByteBuffer.class, "mappingOffset");

    /** {@link MappedByteBuffer#mappingAddress(long)}. */
    private static final Method mappingAddress = findNonPublicMethod(
        MappedByteBuffer.class, "mappingAddress", long.class
    );

    /** {@link MappedByteBuffer#fd} */
    private static final Field fd = findField(MappedByteBuffer.class, "fd");

    /** Page size. */
    private static final int PAGE_SIZE = GridUnsafe.pageSize();

    /**
     * @param buf Holded buffer.
     */
    MappedByteBufferHolder(MappedByteBuffer buf) {
        super(buf);
    }

    /** {@inheritDoc} */
    @Override public void msync(int off, int len) throws IgniteCheckedException {
        checkValidState();
        try {
            MappedByteBuffer buf = (MappedByteBuffer)buffer();

            long mappedOff = (Long)mappingOffset.invoke(buf);

            assert mappedOff == 0 : mappedOff;

            long addr = (Long)mappingAddress.invoke(buf, mappedOff);

            long delta = (addr + off) % PAGE_SIZE;

            long alignedAddr = (addr + off) - delta;

            force0.invoke(buf, fd.get(buf), alignedAddr, len + delta);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void msync() throws IgniteCheckedException {
        MappedByteBuffer buf = (MappedByteBuffer)buffer();
        buf.force();
    }

    /** {@inheritDoc} */
    @Override public Type type() {
        return Type.MMAP;
    }
}
