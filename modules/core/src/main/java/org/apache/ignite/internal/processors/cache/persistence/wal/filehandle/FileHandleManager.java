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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;

/**
 * Manager of {@link FileWriteHandle}.
 */
public interface FileHandleManager {
    /**
     * Initialize {@link FileWriteHandle}.
     *
     * @param fileIO FileIO.
     * @param position Init position.
     * @param resume {@code true} if it is calling during resume of WAL.
     * @param serializer Serializer for file handle.
     * @return Created file handle.
     * @throws IOException if creation was not success.
     */
    FileWriteHandle initHandle(SegmentIO fileIO,
        long position,
        boolean resume,
        RecordSerializer serializer) throws IOException;

    /**
     * Create next file handle.
     *
     * @param fileIO FileIO.
     * @param position Init position.
     * @param resume {@code true} if it is calling during resume of WAL.
     * @param serializer Serializer for file handle.
     * @return Created file handle.
     * @throws IOException if creation was not success.
     */
    FileWriteHandle nextHandle(
        SegmentIO fileIO,
        long position,
        boolean resume,
        RecordSerializer serializer) throws IOException;

    /**
     * Start manager.
     */
    void start();

    /**
     * Stop manager.
     *
     * @throws IgniteCheckedException if fail.
     */
    void stop() throws IgniteCheckedException;

    /**
     * Resume logging.
     */
    void resumeLogging();

    /**
     * @param ptr Pointer until need to flush.
     * @param explicitFsync {@code true} if fsync required.
     * @throws IgniteCheckedException if fail.
     * @throws StorageException if storage was fail.
     */
    void flush(WALPointer ptr, boolean explicitFsync) throws IgniteCheckedException, StorageException;
}
