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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Snapshot restore status task.
 */
@GridInternal
class SnapshotRestoreStatusTask extends SnapshotRestoreManagementTask<Map<UUID, SnapshotRestoreStatusDetails>> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected ComputeJob makeJob(String snpName) {
        return new ComputeJobAdapter() {
            @IgniteInstanceResource
            private transient IgniteEx ignite;

            @Override public SnapshotRestoreStatusDetails execute() throws IgniteException {
                return ignite.context().cache().context().snapshotMgr().localRestoreStatus(snpName);
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, SnapshotRestoreStatusDetails> reduce(List<ComputeJobResult> results) throws IgniteException {
        Map</*reqId*/UUID, Map</*nodeId*/UUID, SnapshotRestoreStatusDetails>> reqMap = new HashMap<>();
        T2<Long, UUID> oldestUUID = new T2<>(0L, null);

        for (ComputeJobResult r : results) {
            if (r.getException() != null)
                throw new IgniteException("Failed to execute job [nodeId=" + r.getNode().id() + ']', r.getException());

            SnapshotRestoreStatusDetails details = r.getData();

            if (details == null)
                continue;

            if (oldestUUID.get1() < details.startTime())
                oldestUUID.set(details.startTime(), details.requestId());

            reqMap.computeIfAbsent(details.requestId(), v -> new HashMap<>()).put(r.getNode().id(), details);
        }

        return reqMap.isEmpty() ? null : reqMap.get(oldestUUID.get2());
    }
}
