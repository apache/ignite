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
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
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

    /** If {@code true} then only cache config and metadata included in snapshot. */
    private final boolean configOnly;

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
     * @param configOnly If {@code true} then only cache config and metadata included in snapshot.
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
        boolean encrypt,
        boolean configOnly
    ) {
        super(reqId, snpName, snpPath, grps, nodes);

        this.opNodeId = opNodeId;
        this.incremental = incremental;
        this.incIdx = incIdx;
        this.onlyPrimary = onlyPrimary;
        this.dump = dump;
        this.compress = compress;
        this.encrypt = encrypt;
        this.configOnly = configOnly;
    }

    /**
     * @return Operational node ID.
     */
    public UUID operationalNodeId() {
        return opNodeId;
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

    /** @return If {@code true} then only cache config and metadata included in snapshot. */
    public boolean configOnly() {
        return configOnly;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotOperationRequest.class, this);
    }
}
