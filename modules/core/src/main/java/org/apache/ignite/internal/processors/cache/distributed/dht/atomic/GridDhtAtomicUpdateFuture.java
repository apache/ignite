/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * DHT atomic cache backup update future.
 */
class GridDhtAtomicUpdateFuture extends GridDhtAtomicAbstractUpdateFuture {
    /** */
    private int updateCntr;

    /**
     * @param cctx Cache context.
     * @param writeVer Write version.
     * @param updateReq Update request.
     */
    GridDhtAtomicUpdateFuture(
        GridCacheContext cctx,
        GridCacheVersion writeVer,
        GridNearAtomicAbstractUpdateRequest updateReq
    ) {
        super(cctx, writeVer, updateReq);

        mappings = U.newHashMap(updateReq.size());
    }

    /** {@inheritDoc} */
    @Override protected boolean sendAllToDht() {
        return updateCntr == updateReq.size();
    }

    /** {@inheritDoc} */
    @Override protected void addDhtKey(KeyCacheObject key, List<ClusterNode> dhtNodes) {
        assert updateCntr < updateReq.size();

        updateCntr++;
    }

    /** {@inheritDoc} */
    @Override protected void addNearKey(KeyCacheObject key, GridDhtCacheEntry.ReaderId[] readers) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected GridDhtAtomicAbstractUpdateRequest createRequest(
        UUID nodeId,
        long futId,
        GridCacheVersion writeVer,
        CacheWriteSynchronizationMode syncMode,
        @NotNull AffinityTopologyVersion topVer,
        long ttl,
        long conflictExpireTime,
        @Nullable GridCacheVersion conflictVer
    ) {
        return new GridDhtAtomicUpdateRequest(
            cctx.cacheId(),
            nodeId,
            futId,
            writeVer,
            syncMode,
            topVer,
            updateReq.subjectId(),
            updateReq.taskNameHash(),
            null,
            cctx.deploymentEnabled(),
            updateReq.keepBinary(),
            updateReq.skipStore(),
            false);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateFuture.class, this, "super", super.toString());
    }
}
