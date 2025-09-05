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
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
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
    private final UUID reqId;

    /** Snapshot name. */
    @GridToStringInclude
    private final String snpName;

    /** Snapshot directory path. */
    @GridToStringInclude
    private final String snpPath;

    /** Collection of cache group names. */
    @GridToStringInclude
    private final Collection<String> grps;

    /** Start time. */
    @GridToStringInclude
    private final long startTime;

    /** IDs of the nodes that must be alive to complete the operation. */
    @GridToStringInclude
    private final Set<UUID> nodes;

    /**
     * @param reqId Request ID.
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param grps Collection of cache group names.
     * @param incIdx Incremental snapshot index.
     * @param nodes IDs of the nodes that must be alive to complete the operation.
     */
    protected AbstractSnapshotOperationRequest(
        UUID reqId,
        String snpName,
        String snpPath,
        @Nullable Collection<String> grps,
        int incIdx,
        Collection<UUID> nodes
    ) {
        this.reqId = reqId;
        this.snpName = snpName;
        this.grps = grps;
        this.snpPath = snpPath;
        this.nodes = new HashSet<>(nodes);
        this.startTime = System.currentTimeMillis();
    }

    /** @return Request ID. */
    @Nullable public UUID requestId() {
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

    /** @return Start time. */
    public long startTime() {
        return startTime;
    }

    /** @return IDs of the nodes that must be alive to complete the operation. */
    public Set<UUID> nodes() {
        return nodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractSnapshotOperationRequest.class, this);
    }
}
