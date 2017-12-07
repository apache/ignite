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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.OpenOption;
import net.smacke.jaydio.DirectIoLib;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

public class AlignedBuffersDirectFileIOFactory implements FileIOFactory {

    @Nullable private final DirectIoLib directIoLib;

    private int pageSize;

    /** Backup factory for files in case native is not available or not applicable. */
    private final FileIOFactory backupFactory;

    private ThreadLocal<ByteBuffer> tblOnePageAligned;


    /** Managed aligned buffers. */
    private final ConcurrentHashMap8<Long, String> managedAlignedBuffers = new ConcurrentHashMap8<>();


    public AlignedBuffersDirectFileIOFactory(
        final File storePath,
        final int pageSize,
        final FileIOFactory backupFactory) {

        this.pageSize = pageSize;
        this.backupFactory = backupFactory;
        directIoLib = DirectIoLib.getLibForPath(storePath.getAbsolutePath());
        //todo validate data storage settings and invalidate factory
        tblOnePageAligned = new ThreadLocal<ByteBuffer>() {
            /** {@inheritDoc} */
            @Override protected ByteBuffer initialValue() {
                return createManagedBuffer(pageSize);
            }
        };
    }

    /**
     * @param capacity buffer size
     * @return
     */
    @NotNull public ByteBuffer createManagedBuffer(int capacity) {
        final ByteBuffer allocate = AlignedBuffer.allocate(fsBlockSize(), capacity).order(ByteOrder.nativeOrder());

        managedAlignedBuffers.put(GridUnsafe.bufferAddress(allocate), Thread.currentThread().getName());

        return allocate;
    }

    /** {@inheritDoc} */
    @Override public FileIO create(File file) throws IOException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public FileIO create(File file, OpenOption... modes) throws IOException {
        if (directIoLib == null)
            return backupFactory.create(file, modes);

        return new AlignedBuffersDirectFileIO(fsBlockSize(),  pageSize, file, modes, tblOnePageAligned, managedAlignedBuffers);
    }

    /**
     * @return fs block size, negative value if library init was failed
     */
    public int fsBlockSize() {
        return isDirectAvailable() ? directIoLib.blockSize() : -1;
    }

    public boolean isDirectAvailable() {
        return directIoLib != null;
    }

    public ConcurrentHashMap8<Long, String> managedAlignedBuffers() {
        return managedAlignedBuffers;
    }
}
