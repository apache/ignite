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

    /**
     * If {@code true}, all the registered {@link IgniteSnapshotManager#handlers()} of type {@link SnapshotHandlerType#RESTORE}
     * are invoked. Otherwise, only snapshot metadatas and partition hashes are validated.
     */
    @GridToStringInclude final boolean allRestoreHandlers;

    /**
     * Creates snapshot check process request.
     *
     * @param reqId Request ID.
     * @param snpName Snapshot name.
     * @param nodes Baseline node IDs that must be alive to complete the operation..
     * @param snpPath Snapshot directory path.
     * @param grps List of cache group names.
     * @param incIdx Incremental snapshot index.
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
        int incIdx,
        boolean allRestoreHandlers
    ) {
        super(reqId, null, snpName, snpPath, grps, incIdx, nodes);

        assert !F.isEmpty(nodes);

        this.allRestoreHandlers = allRestoreHandlers;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotCheckProcessRequest.class, this, super.toString());
    }
}
