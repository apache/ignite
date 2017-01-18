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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.tree.io.TrackingPageIO;

/**
 * Delta record for updates in tracking pages
 */
public class TrackingPageDeltaRecord extends PageDeltaRecord {
    /** Page id to mark. */
    private final long pageIdToMark;

    /** Next snapshot id. */
    private final long nextSnapshotId;

    /** Last successful snapshot id. */
    private final long lastSuccessfulSnapshotId;

    /**
     * @param cacheId Cache id.
     * @param pageId Page id.
     * @param nextSnapshotId
     * @param lastSuccessfulSnapshotId
     */
    public TrackingPageDeltaRecord(int cacheId, long pageId, long pageIdToMark, long nextSnapshotId, long lastSuccessfulSnapshotId) {
        super(cacheId, pageId);

        this.pageIdToMark = pageIdToMark;
        this.nextSnapshotId = nextSnapshotId;
        this.lastSuccessfulSnapshotId = lastSuccessfulSnapshotId;
    }

    /**
     * Page Id which should be marked as changed
     */
    public long pageIdToMark() {
        return pageIdToMark;
    }

    /**
     *
     */
    public long nextSnapshotId() {
        return nextSnapshotId;
    }

    /**
     *
     */
    public long lastSuccessfulSnapshotId() {
        return lastSuccessfulSnapshotId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        TrackingPageIO.VERSIONS.forPage(pageAddr).markChanged(pageMem.pageBuffer(pageAddr),
            pageIdToMark,
            nextSnapshotId,
            lastSuccessfulSnapshotId,
            pageMem.pageSize());
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.TRACKING_PAGE_DELTA;
    }
}
