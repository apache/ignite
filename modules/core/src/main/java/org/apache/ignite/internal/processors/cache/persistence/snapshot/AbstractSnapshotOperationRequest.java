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
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot operation start request for {@link DistributedProcess} initiate message.
 */
abstract class AbstractSnapshotOperationRequest implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Request ID. */
    @GridToStringInclude
    protected final UUID reqId;

    /** Snapshot name. */
    @GridToStringInclude
    protected final String snpName;

    /** Snapshot directory path. */
    @GridToStringInclude
    protected final String snpPath;

    /** List of cache group names. */
    @GridToStringInclude
    protected final Collection<String> grps;

    /** Operational node ID. */
    @GridToStringInclude
    protected final UUID opNodeId;

    /** Index of incremental snapshot. */
    @GridToStringInclude
    protected final int incIdx;

    /** Start time. */
    @GridToStringInclude
    private final long startTime;

    /** IDs of the nodes that must be alive to complete the operation. */
    @GridToStringInclude
    protected final Set<UUID> nodes;

    /** Exception occurred during snapshot operation processing. */
    @GridToStringInclude
    private volatile Throwable err;

    /** Snapshot local metadata. */
    @GridToStringExclude
    private transient volatile SnapshotMetadata locMeta;

    /**
     * @param reqId Request ID.
     * @param opNodeId Operational node ID.
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param grps List of cache group names.
     * @param incIdx Incremental snapshot index.
     * @param nodes IDs of the nodes that must be alive to complete the operation.
     */
    protected AbstractSnapshotOperationRequest(
        UUID reqId,
        UUID opNodeId,
        String snpName,
        String snpPath,
        @Nullable Collection<String> grps,
        int incIdx,
        Collection<UUID> nodes
    ) {
        this.reqId = reqId;
        this.opNodeId = opNodeId;
        this.snpName = snpName;
        this.grps = grps;
        this.snpPath = snpPath;
        this.incIdx = incIdx;
        this.nodes = new HashSet<>(nodes);
        this.startTime = System.currentTimeMillis();
    }

    /** @return Snapshot local metadata. */
    public SnapshotMetadata meta() {
        return locMeta;
    }

    /** Stores snapshot local metadata. */
    void meta(SnapshotMetadata meta) {
        this.locMeta = meta;
    }

    /** Stores exception occurred during snapshot operation processing. */
    synchronized void error(Throwable err) {
        assert err != null;

        if (this.err == null)
            this.err = err;
    }

    /** @return Exception occurred during snapshot operation processing. */
    public Throwable error() {
        return err;
    }

    /** @return Request ID. */
    public UUID requestId() {
        return reqId;
    }

    /** @return Snapshot name. */
    public String snapshotName() {
        return snpName;
    }

    /** @return Snapshot directory path. */
    public String snapshotPath() {
        return snpPath;
    }

    /** @return List of cache group names. */
    public @Nullable Collection<String> groups() {
        return grps;
    }

    /** @return Operational node ID. */
    public UUID operationalNodeId() {
        return opNodeId;
    }

    /** @return Incremental index. */
    public int incrementIndex() {
        return incIdx;
    }

    /** @return Start time. */
    public long startTime() {
        return startTime;
    }

    /** @return IDs of the nodes that must be alive to complete the operation. */
    public Set<UUID> nodes() {
        return nodes;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || getClass() != other.getClass())
            return false;

        AbstractSnapshotOperationRequest o = (AbstractSnapshotOperationRequest)other;

        return reqId.equals(o.reqId) && snpName.equals(o.snpName) && Objects.equals(snpPath, o.snpPath)
            && Objects.equals(grps, o.grps) && opNodeId.equals(o.opNodeId) && incIdx == o.incIdx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractSnapshotOperationRequest.class, this);
    }
}
