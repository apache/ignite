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

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.util.Set;
import java.util.function.Predicate;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Class for holding only very basic WAL filters for using in {@link FilteredWalIterator}. *
 */
public class WalFilters {
    /**
     * Filtering all checkpoint records.
     *
     * @return Predicate for filtering checkpoint records.
     */
    public static Predicate<IgniteBiTuple<WALPointer, WALRecord>> checkpoint() {
        return record -> record.get2() instanceof CheckpointRecord;
    }

    /**
     * Filtering all records whose pageId is contained in pageOwnerIds.
     *
     * @param pageOwnerIds Page id for filtering.
     * @return Predicate for filtering record from pageOwnerIds.
     */
    public static Predicate<IgniteBiTuple<WALPointer, WALRecord>> pageOwner(Set<Long> pageOwnerIds) {
        return record -> {
            if (record.get2() instanceof PageDeltaRecord)
                return pageOwnerIds.contains(((PageDeltaRecord)record.get2()).pageId());
            else if (record.get2() instanceof PageSnapshot)
                return pageOwnerIds.contains(((PageSnapshot)record.get2()).fullPageId().pageId());

            return false;
        };
    }
}
