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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot full check (validation) distributed process request.
 *
 * @see SnapshotCheckProcess
 */
public class SnapshotCheckProcessRequest extends AbstractSnapshotOperationRequest {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    final boolean includeCustomHandlers;

    /** If of the operation coordinator node. One of {@link AbstractSnapshotOperationRequest#nodes}. */
    @GridToStringInclude
    final UUID opCoordId;

    /** */
    @GridToStringExclude
    volatile Map<ClusterNode, List<SnapshotMetadata>> metas;

    /** Curent working future */
    @GridToStringExclude
    private transient volatile GridFutureAdapter<?> fut;

    /**
     * @param reqId    Request ID.
     * @param opNodeId Operational node ID.
     * @param snpName  Snapshot name.
     * @param nodes Baseline node IDs that must be alive to complete the operation..
     * @param opCoordId Operation coordinator node id. One of {@code nodes}.
     * @param snpPath  Snapshot directory path.
     * @param grps     List of cache group names.
     * @param incIdx   Incremental snapshot index.
     * @param includeCustomHandlers   Incremental snapshot index.
     */
    protected SnapshotCheckProcessRequest(
        UUID reqId,
        UUID opNodeId,
        Collection<UUID> nodes,
        UUID opCoordId,
        String snpName,
        String snpPath,
        @Nullable Collection<String> grps,
        int incIdx,
        boolean includeCustomHandlers,
        boolean validatePartitions
    ) {
        super(reqId, opNodeId, snpName, snpPath, grps, incIdx, nodes);

        assert nodes.contains(opCoordId);

        this.opCoordId = opCoordId;
        this.includeCustomHandlers = includeCustomHandlers;
    }

    /** */
    GridFutureAdapter<?> fut() {
        return fut;
    }

    /** */
    synchronized void fut(GridFutureAdapter<?> fut) {
        assert fut != null;

        this.fut = fut;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotCheckProcessRequest.class, this, super.toString());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || getClass() != other.getClass())
            return false;

        if (!super.equals(other))
            return false;

        SnapshotCheckProcessRequest o = (SnapshotCheckProcessRequest)other;

        return includeCustomHandlers == o.includeCustomHandlers;
    }
}
