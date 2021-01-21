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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Request to perform snapshot restore.
 */
public class SnapshotRestorePerformRequest extends SnapshotRestorePrepareRequest {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Node ID from which to update the binary metadata. */
    private final UUID updateMetaNodeId;

    /**
     * @param reqId Request ID.
     * @param snpName Snapshot name.
     * @param grps List of cache group names to restore from the snapshot.
     * @param reqNodes List of baseline node IDs that must be alive to complete the operation.
     * @param updateMetaNodeId Node ID from which to update the binary metadata.
     */
    public SnapshotRestorePerformRequest(UUID reqId, String snpName, Collection<String> grps, Set<UUID> reqNodes, UUID updateMetaNodeId) {
        super(reqId, snpName, grps, reqNodes);

        this.updateMetaNodeId = updateMetaNodeId;
    }

    /**
     * @return Node ID from which to update the binary metadata.
     */
    public UUID updateMetaNodeId() {
        return updateMetaNodeId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRestorePerformRequest.class, this);
    }
}
