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
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;

/**
 * Noop stub for WAL manager.
 */
public class IgniteWriteAheadLogNoopManager extends GridCacheSharedManagerAdapter
    implements IgniteWriteAheadLogManager {
    /** {@inheritDoc} */
    @Override public boolean isAlwaysWriteFullPages() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void resumeLogging() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public WALPointer log(WALRecord entry) throws IgniteCheckedException, StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void fsync(WALPointer ptr) throws IgniteCheckedException, StorageException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public WALIterator replay(WALPointer start) throws IgniteCheckedException, StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public int truncate(WALPointer ptr) throws IgniteException, StorageException {
        return 0;
    }
}
