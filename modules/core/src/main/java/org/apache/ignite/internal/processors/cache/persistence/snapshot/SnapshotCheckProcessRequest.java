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
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
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

    /** If {@code true}, additionally calculates partition hashes. Otherwise, checks only snapshot integrity and partition counters. */
    @GridToStringInclude
    private final boolean fullCheck;

    /**
     * If {@code true}, all the registered {@link IgniteSnapshotManager#handlers()} of type {@link SnapshotHandlerType#RESTORE}
     * are invoked. Otherwise, only snapshot metadatas and partition hashes are validated.
     */
    @GridToStringInclude
    private final boolean allRestoreHandlers;

    /** Incremental snapshot index. If not positive, snapshot is not considered as incremental. */
    @GridToStringInclude
    private final int incIdx;

    /**
     * Creates snapshot check process request.
     *
     * @param reqId Request ID.
     * @param snpName Snapshot name.
     * @param nodes Baseline node IDs that must be alive to complete the operation..
     * @param snpPath Snapshot directory path.
     * @param grps List of cache group names.
     * @param fullCheck If {@code true}, additionally calculates partition hashes. Otherwise, checks only snapshot integrity
     *                  and partition counters.
     * @param incIdx Incremental snapshot index. If not positive, snapshot is not considered as incremental.
     * @param allRestoreHandlers If {@code true}, all the registered {@link IgniteSnapshotManager#handlers()} of type
     *                           {@link SnapshotHandlerType#RESTORE} are invoked. Otherwise, only snapshot metadatas and
     *                           partition hashes are validated.
     */
    SnapshotCheckProcessRequest(
        UUID reqId,
        Collection<UUID> nodes,
        String snpName,
        String snpPath,
        @Nullable Collection<String> grps,
        boolean fullCheck,
        int incIdx,
        boolean allRestoreHandlers
    ) {
        super(reqId, snpName, snpPath, grps, 0, nodes);

        assert !F.isEmpty(nodes);

        this.fullCheck = fullCheck;
        this.allRestoreHandlers = allRestoreHandlers;
        this.incIdx = incIdx;
    }

    /**
     * If {@code true}, all the registered {@link IgniteSnapshotManager#handlers()} of type {@link SnapshotHandlerType#RESTORE}
     * are invoked. Otherwise, only snapshot metadatas and partition hashes are checked.
     */
    public boolean allRestoreHandlers() {
        return allRestoreHandlers;
    }

    /** If {@code true}, additionally calculates partition hashes. Otherwise, checks only snapshot integrity and partition counters. */
    public boolean fullCheck() {
        return fullCheck;
    }

    /** @return Incremental snapshot index. If not positive, snapshot is not considered as incremental. */
    public int incrementalIndex() {
        return incIdx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotCheckProcessRequest.class, this, super.toString());
    }
}
