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
     * Initialize {@link FileWriteHandle} for first time.
     *
     * @param fileIO FileIO.
     * @param position Init position.
     * @param serializer Serializer for file handle.
     * @return Created file handle.
     * @throws IOException if creation was not success.
     */
    FileWriteHandle initHandle(SegmentIO fileIO, long position, RecordSerializer serializer) throws IOException;

    /**
     * Create next file handle.
     *
     * @param fileIO FileIO.
     * @param serializer Serializer for file handle.
     * @return Created file handle.
     * @throws IOException if creation was not success.
     */
    FileWriteHandle nextHandle(SegmentIO fileIO, RecordSerializer serializer) throws IOException;

    /**
     * On deactivate.
     *
     * @throws IgniteCheckedException if fail.
     */
    void onDeactivate() throws IgniteCheckedException;

    /**
     * Resume logging.
     */
    void resumeLogging();

    /**
     * @param ptr Pointer until need to flush.
     * @param explicitFsync {@code true} if fsync required.
     * @throws IgniteCheckedException if fail.
     * @throws StorageException if storage was fail.
     * @return Last WAL position which was flushed to WAL segment file. May be greater than or equal to a {@code ptr}.
     * May be {@code null}, it means nothing has been flushed.
     */
    WALPointer flush(WALPointer ptr, boolean explicitFsync) throws IgniteCheckedException, StorageException;
}
