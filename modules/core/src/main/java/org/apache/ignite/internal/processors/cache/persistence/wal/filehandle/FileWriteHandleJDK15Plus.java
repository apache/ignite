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

package org.apache.ignite.internal.processors.cache.persistence.wal.filehandle;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.IgniteUtils.findField;
import static org.apache.ignite.internal.util.IgniteUtils.findNonPublicMethod;

/**
 * File handle for one log segment, for versions of java 15 and higher
 */
class FileWriteHandleJDK15Plus extends FileWriteHandleImpl {
    /**
     * @param cctx              Context.
     * @param fileIO            I/O file interface to use
     * @param rbuf
     * @param serializer        Serializer.
     * @param metrics           Data storage metrics.
     * @param writer            WAL writer.
     * @param pos               Initial position.
     * @param mode              WAL mode.
     * @param mmap              Mmap.
     * @param resume            Created on resume logging flag.
     * @param fsyncDelay        Fsync delay.
     * @param maxWalSegmentSize Max WAL segment size.
     * @throws IOException If failed.
     */
    FileWriteHandleJDK15Plus(GridCacheSharedContext cctx, SegmentIO fileIO, SegmentedRingByteBuffer rbuf, RecordSerializer serializer,
                             DataStorageMetricsImpl metrics, FileHandleManagerImpl.WALWriter writer, long pos, WALMode mode,
                             boolean mmap, boolean resume, long fsyncDelay, long maxWalSegmentSize) throws IOException {
        super(cctx, fileIO, rbuf, serializer, metrics, writer, pos, mode, mmap, resume, fsyncDelay, maxWalSegmentSize);
    }

    /** {can't link, package private MappedMemoryUtils#force0(java.io.FileDescriptor, long, long)}. */
    private static final Method force0 = findNonPublicMethod(
        U.classForName("java.nio.MappedMemoryUtils", null), "force0",
        java.io.FileDescriptor.class, long.class, long.class
    );

    /** {@link MappedByteBuffer#address}. */
    private static final Field address = findField(MappedByteBuffer.class, "address");

    /** {can't link, package private MappedMemoryUtils#mappingOffset(long)}. */
    private static final Method mappingOffset = findNonPublicMethod(
        U.classForName("java.nio.MappedMemoryUtils", null), "mappingOffset", long.class
    );

    /** {can't link, package private MappedMemoryUtils#mappingLength(long, long)}. */
    private static final Method mappingAddress = findNonPublicMethod(
        U.classForName("java.nio.MappedMemoryUtils", null), "mappingAddress", long.class, long.class
    );

    /**
     * @param buf Mapped byte buffer.
     * @param fieldObject Value of the field.
     * @param off Offset.
     * @param len Length.
     */
    @Override protected void fsync(MappedByteBuffer buf, Object fieldObject, int off, int len)
        throws IllegalAccessException, InvocationTargetException {
        long bufAddr = address.getLong(buf);
        long mappedOff = (Long)mappingOffset.invoke(null, bufAddr);
        assert mappedOff == 0 : mappedOff;
        long addr = (Long)mappingAddress.invoke(null, bufAddr, mappedOff);

        long delta = (addr + off) % PAGE_SIZE;

        long alignedAddr = (addr + off) - delta;

        force0.invoke(buf, fieldObject, alignedAddr, len + delta);
    }
}
