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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Incremental snapshot metadata file.
 *
 * @see IgniteSnapshotManager#createIncrementalSnapshot(String)
 */
public class IncrementalSnapshotMetadata implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Unique snapshot request id. */
    private final UUID rqId;

    /** Snapshot name. */
    private final String snpName;

    /** Increment index. */
    private final int incIdx;

    /** Consistent id of a node to which this metadata relates. */
    private final String consId;

    /**
     * Directory related to the current consistent node id on which partition files are stored.
     * For some of the cases, consId doesn't equal the directory name.
     */
    private final String folderName;

    /** WAL pointer to {@link IncrementalSnapshotFinishRecord}. */
    private final WALPointer incSnpRec;

    /**
     * @param rqId Unique request id.
     * @param snpName Snapshot name.
     * @param incIdx Incremental snapshot index.
     * @param consId Consistent id of a node to which this metadata relates.
     * @param folderName Directory name which stores the data files.
     * @param incSnpRec Pointer to {@link IncrementalSnapshotFinishRecord}.
     */
    public IncrementalSnapshotMetadata(
        UUID rqId,
        String snpName,
        int incIdx,
        String consId,
        String folderName,
        WALPointer incSnpRec
    ) {
        this.rqId = rqId;
        this.snpName = snpName;
        this.incIdx = incIdx;
        this.consId = consId;
        this.folderName = folderName;
        this.incSnpRec = incSnpRec;
    }

    /** @return Snapshot request ID. */
    public UUID requestId() {
        return rqId;
    }

    /** @return Snapshot name. */
    public String snapshotName() {
        return snpName;
    }

    /** @return Consistent ID of a node to which this metadata relates. */
    public String consistentId() {
        return consId;
    }

    /** @return Pointer to {@link IncrementalSnapshotFinishRecord}. */
    public WALPointer incSnpPointer() {
        return incSnpRec;
    }

    /** @return Incremental snapshot index. */
    public int incrementalIndex() {
        return incIdx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IncrementalSnapshotMetadata.class, this);
    }
}
