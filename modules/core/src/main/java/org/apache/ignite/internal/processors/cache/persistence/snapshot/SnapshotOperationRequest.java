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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot operation start request for {@link DistributedProcess} initiate message.
 */
public class SnapshotOperationRequest extends AbstractSnapshotOperationRequest {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Operational node ID. */
    private final UUID opNodeId;

    /** Exception occurred during snapshot operation processing. */
    private volatile Throwable err;

    /**
     * Snapshot operation warnings. Warnings do not interrupt snapshot process but raise exception at the end to make
     * the operation status 'not OK' if no other error occurred.
     */
    private volatile List<String> warnings;

    /** Snapshot metadata. */
    @GridToStringExclude
    private transient SnapshotMetadata meta;

    /** Snapshot file tree. */
    @GridToStringExclude
    private transient SnapshotFileTree sft;

    /**
     * Warning flag of concurrent inconsistent-by-nature streamer updates.
     */
    @GridToStringExclude
    private transient volatile boolean streamerWrn;

    /** Flag indicating that the {@link DistributedProcessType#START_SNAPSHOT} phase has completed. */
    private transient volatile boolean startStageEnded;

    /** If {@code true} then incremental snapshot requested. */
    private final boolean incremental;

    /** Index of incremental snapshot. */
    private final int incIdx;

    /** If {@code true} snapshot only primary copies of partitions. */
    private final boolean onlyPrimary;

    /** If {@code true} then create dump. */
    private final boolean dump;

    /** If {@code true} then compress partition files. */
    private final boolean compress;

    /** If {@code true} then content of dump encrypted. */
    private final boolean encrypt;

    /**
     * @param reqId Request ID.
     * @param opNodeId Operational node ID.
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param grps List of cache group names.
     * @param nodes Baseline node IDs that must be alive to complete the operation.
     * @param incremental {@code True} if incremental snapshot requested.
     * @param incIdx Incremental snapshot index.
     * @param onlyPrimary If {@code true} snapshot only primary copies of partitions.
     * @param dump If {@code true} then create dump.
     * @param compress If {@code true} then compress partition files.
     * @param encrypt If {@code true} then content of dump encrypted.
     */
    public SnapshotOperationRequest(
        UUID reqId,
        UUID opNodeId,
        String snpName,
        String snpPath,
        @Nullable Collection<String> grps,
        Set<UUID> nodes,
        boolean incremental,
        int incIdx,
        boolean onlyPrimary,
        boolean dump,
        boolean compress,
        boolean encrypt
    ) {
        super(reqId, snpName, snpPath, grps, incIdx, nodes);

        this.opNodeId = opNodeId;
        this.incremental = incremental;
        this.incIdx = incIdx;
        this.onlyPrimary = onlyPrimary;
        this.dump = dump;
        this.compress = compress;
        this.encrypt = encrypt;
    }

    /**
     * @return Operational node ID.
     */
    public UUID operationalNodeId() {
        return opNodeId;
    }

    /**
     * @return Exception occurred during snapshot operation processing.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @param err Exception occurred during snapshot operation processing.
     */
    public void error(Throwable err) {
        this.err = err;
    }

    /** @return {@code True} if incremental snapshot requested. */
    public boolean incremental() {
        return incremental;
    }

    /** @return Incremental index. */
    public int incrementIndex() {
        return incIdx;
    }

    /** @return If {@code true} snapshot only primary copies of partitions. */
    public boolean onlyPrimary() {
        return onlyPrimary;
    }

    /** @return If {@code true} then create dump. */
    public boolean dump() {
        return dump;
    }

    /** @return If {@code true} then compress partition files. */
    public boolean compress() {
        return compress;
    }

    /** @return If {@code true} then content of dump encrypted. */
    public boolean encrypt() {
        return encrypt;
    }

    /**
     * @return Flag indicating that the {@link DistributedProcessType#START_SNAPSHOT} phase has completed.
     */
    protected boolean startStageEnded() {
        return startStageEnded;
    }

    /**
     * @param startStageEnded Flag indicating that the {@link DistributedProcessType#START_SNAPSHOT} phase has completed.
     */
    protected void startStageEnded(boolean startStageEnded) {
        this.startStageEnded = startStageEnded;
    }

    /**
     * @return Warnings of snapshot operation.
     */
    public List<String> warnings() {
        return warnings;
    }

    /**
     * @param warnings Warnings of snapshot operation.
     */
    public void warnings(List<String> warnings) {
        assert this.warnings == null;

        this.warnings = warnings;
    }

    /**
     * {@code True} If the streamer warning flag is set. {@code False} otherwise.
     */
    public boolean streamerWarning() {
        return streamerWrn;
    }

    /**
     * Sets the streamer warning flag.
     */
    public boolean streamerWarning(boolean val) {
        return streamerWrn = val;
    }

    /**
     * @return Snapshot metadata.
     */
    public SnapshotMetadata meta() {
        return meta;
    }

    /**
     * Stores snapshot metadata.
     */
    public void meta(SnapshotMetadata meta) {
        this.meta = meta;
    }

    /**
     * Stores snapshot file tree.
     */
    public void snapshotFileTree(SnapshotFileTree sft) {
        this.sft = sft;
    }

    /**
     * @return Snapshot file tree.
     */
    public SnapshotFileTree snapshotFileTree() {
        return sft;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotOperationRequest.class, this);
    }
}
