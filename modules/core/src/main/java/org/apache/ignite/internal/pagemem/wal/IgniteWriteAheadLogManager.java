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

package org.apache.ignite.internal.pagemem.wal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.Iterator;

/**
 *
 */
public interface IgniteWriteAheadLogManager extends GridCacheSharedManager {
    /**
     * Resumes logging after start. When WAL manager is started, it will skip logging any updates until this
     * method is called to avoid logging changes induced by the state restore procedure.
     */
    public void resumeLogging() throws IgniteCheckedException;

    /**
     * Appends the given log entry to the write-ahead log.
     *
     * @param entry entry to log.
     * @param fsync If {@code true}, will sync the log after the entry write. Will effectively sync all
     *      previous records written to the log before.
     * @return WALPointer that may be passed to {@link #fsync(WALPointer)} method to make sure the record is
     *      written to the log.
     * @throws IgniteCheckedException If failed to construct log entry.
     * @throws StorageException If IO error occurred while writing log entry.
     */
    public WALPointer log(WALRecord entry, boolean fsync) throws IgniteCheckedException, StorageException;

    /**
     * Makes sure that all log entries written to the log up until the specified pointer are actually persisted to
     * the underlying storage.
     *
     * @param ptr Optional pointer to sync. If {@code null}, will sync up to the latest record.
     * @throws IgniteCheckedException If
     * @throws StorageException
     */
    public void fsync(WALPointer ptr) throws IgniteCheckedException, StorageException;

    /**
     * Invoke this method to iterate over the written log entries.
     *
     * @param start Optional WAL pointer from which to start iteration.
     * @return Records iterator.
     * @throws IgniteException If failed to start iteration.
     * @throws StorageException If IO error occurred while reading WAL entries.
     */
    public WALIterator replay(WALPointer start) throws IgniteCheckedException, StorageException;

    /**
     * Given WAL a hint to clear entries logged before the given pointer.
     *
     * @param ptr Pointer for which it is safe to clear the log.
     * @throws IgniteException If failed to
     * @throws StorageException
     */
    public void truncate(WALPointer ptr) throws IgniteException, StorageException;
}
