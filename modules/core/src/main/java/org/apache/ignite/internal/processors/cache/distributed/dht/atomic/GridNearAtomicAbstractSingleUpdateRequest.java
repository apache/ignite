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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.io.Externalizable;
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridNearAtomicAbstractSingleUpdateRequest extends GridNearAtomicAbstractUpdateRequest {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final CacheEntryPredicate[] NO_FILTER = new CacheEntryPredicate[0];

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    protected GridNearAtomicAbstractSingleUpdateRequest() {
        // No-op.
    }

    /**
     * Constructor.
     * @param cacheId Cache ID.
     * @param nodeId Node ID.
     * @param futId Future ID.
     * @param topVer Topology version.
     * @param syncMode Synchronization mode.
     * @param op Cache update operation.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param flags Flags.
     * @param addDepInfo Deployment info flag.
     */
    protected GridNearAtomicAbstractSingleUpdateRequest(
        int cacheId,
        UUID nodeId,
        long futId,
        @NotNull AffinityTopologyVersion topVer,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        @Nullable UUID subjId,
        int taskNameHash,
        byte flags,
        boolean addDepInfo
    ) {
        super(cacheId,
            nodeId,
            futId,
            topVer,
            syncMode,
            op,
            subjId,
            taskNameHash,
            flags,
            addDepInfo);
    }

    /**
     * @return Expiry policy.
     */
    @Override public ExpiryPolicy expiry() {
        return null;
    }

    /**
     * @return Optional arguments for entry processor.
     */
    @Override @Nullable public Object[] invokeArguments() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheEntryPredicate[] filter() {
        return NO_FILTER;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicAbstractSingleUpdateRequest.class, this,
            "nodeId", nodeId, "futId", futId, "topVer", topVer,
            "parent", super.toString());
    }
}
