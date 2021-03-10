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
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Request to prepare cache group restore from the snapshot.
 */
public class SnapshotRestorePrepareRequest implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Request ID. */
    private final UUID reqId;

    /** Snapshot name. */
    private final String snpName;

    /** Baseline node IDs that must be alive to complete the operation. */
    private final Set<UUID> nodes;

    /** List of cache group names to restore from the snapshot. */
    private final Collection<String> grps;

    /** Node ID from which to update the binary metadata. */
    private final UUID updateMetaNodeId;

    /**
     * @param reqId Request ID.
     * @param snpName Snapshot name.
     * @param nodes Baseline node IDs that must be alive to complete the operation.
     * @param grps List of cache group names to restore from the snapshot.
     * @param updateMetaNodeId Node ID from which to update the binary metadata.
     */
    public SnapshotRestorePrepareRequest(
        UUID reqId,
        String snpName,
        Set<UUID> nodes,
        Collection<String> grps,
        UUID updateMetaNodeId
    ) {
        this.reqId = reqId;
        this.snpName = snpName;
        this.nodes = nodes;
        this.grps = grps;
        this.updateMetaNodeId = updateMetaNodeId;
    }

    /**
     * @return Request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * @return Snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return List of cache group names to restore from the snapshot.
     */
    public Collection<String> groups() {
        return grps;
    }

    /**
     * @return Baseline node IDs that must be alive to complete the operation.
     */
    public Set<UUID> nodes() {
        return nodes;
    }

    /**
     * @return Node ID from which to update the binary metadata.
     */
    public UUID updateMetaNodeId() {
        return updateMetaNodeId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRestorePrepareRequest.class, this);
    }
}
