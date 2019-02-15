/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
import org.apache.ignite.internal.util.typedef.internal.S;

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
     * @param grpId Cache group id.
     * @param pageId Page id.
     * @param nextSnapshotTag next snapshot tag
     * @param lastSuccessfulSnapshotId last successful snapshot id
     */
    public TrackingPageDeltaRecord(int grpId, long pageId, long pageIdToMark, long nextSnapshotTag, long lastSuccessfulSnapshotId) {
        super(grpId, pageId);

        this.pageIdToMark = pageIdToMark;
        this.nextSnapshotId = nextSnapshotTag;
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
            pageMem.realPageSize(groupId()));
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.TRACKING_PAGE_DELTA;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TrackingPageDeltaRecord.class, this, "super", super.toString());
    }
}
