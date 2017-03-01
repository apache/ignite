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

package org.apache.ignite.internal.processors.query.h2.database;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.lang.IgniteFuture;

/**
 *
 */
public class NoOpWALManager implements IgniteWriteAheadLogManager {
    /** {@inheritDoc} */
    @Override public boolean isAlwaysWriteFullPages() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isFullSync() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void resumeLogging(WALPointer ptr) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public WALPointer log(WALRecord entry) throws IgniteCheckedException, StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void fsync(WALPointer ptr) throws IgniteCheckedException, StorageException {

    }

    /** {@inheritDoc} */
    @Override public WALIterator replay(WALPointer start) throws IgniteCheckedException, StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public int truncate(WALPointer ptr) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheSharedContext cctx) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {

    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean reconnect) throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {

    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {

    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {

    }

    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {

    }

    @Override public void onDeActivate(GridKernalContext kctx) throws IgniteCheckedException {

    }
}
