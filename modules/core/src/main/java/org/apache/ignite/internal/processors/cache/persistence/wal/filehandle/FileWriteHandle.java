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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.jetbrains.annotations.Nullable;

/**
 * File write handle.
 */
public interface FileWriteHandle {

    /**
     * @return Version of serializer.
     */
    int serializerVersion();

    /**
     * Do action after finish resume logging.
     */
    void finishResumeLogging();

    /**
     * Write header to segment.
     *
     * @throws IgniteCheckedException if fail.
     */
    void writeHeader() throws IgniteCheckedException;

    /**
     * @param rec Record to be added.
     * @return Pointer or null if roll over to next segment is required or already started by other thread.
     * @throws StorageException if storage was failed.
     * @throws IgniteCheckedException if fail.
     */
    @Nullable WALPointer addRecord(WALRecord rec) throws StorageException, IgniteCheckedException;

    /**
     * Flush all records.
     *
     * @throws IgniteCheckedException if fail.
     */
    void flushAll() throws IgniteCheckedException;

    /**
     * @param ptr Pointer.
     * @return {@code true} if fsync needed.
     */
    boolean needFsync(FileWALPointer ptr);

    /**
     * @return Pointer to the end of the last written record (probably not fsync-ed).
     */
    FileWALPointer position();

    /**
     * Do fsync.
     *
     * @param ptr Pointer to which fsync required.
     * @throws StorageException if storage fail.
     * @throws IgniteCheckedException if fail.
     */
    void fsync(FileWALPointer ptr) throws StorageException, IgniteCheckedException;

    /**
     * Close buffer.
     */
    void closeBuffer();

    /**
     * Close segment.
     *
     * @param rollOver Close for rollover.
     * @return {@code true} if close was success.
     * @throws IgniteCheckedException if fail.
     * @throws StorageException if storage was fail.
     */
    boolean close(boolean rollOver) throws IgniteCheckedException, StorageException;

    /**
     * Signals next segment available to wake up other worker threads waiting for WAL to write.
     */
    void signalNextAvailable();

    /**
     * Awaiting when next segment would be initialized.
     */
    void awaitNext();

    /**
     * @return Absolute WAL segment file index (incremental counter).
     */
    long getSegmentId();

    /**
     * @return SwitchSegmentRecord offset (0 if undef)
     */
    int getSwitchSegmentRecordOffset();
}
