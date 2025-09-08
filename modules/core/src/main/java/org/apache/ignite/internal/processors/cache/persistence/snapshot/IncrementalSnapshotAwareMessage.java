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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.jetbrains.annotations.Nullable;

/**
 * Message that holds a transaction message and incremental snapshot ID.
 */
public class IncrementalSnapshotAwareMessage extends GridCacheMessage {
    /** */
    public static final short TYPE_CODE = 400;

    /** Original transaction message. */
    @Order(3)
    private GridCacheMessage payload;

    /** Incremental snapshot ID. */
    @Order(4)
    private UUID id;

    /** ID of the latest incremental snapshot after which this transaction committed. */
    @Order(value = 5, method = "txIncrementalSnapshotId")
    private @Nullable UUID txSnpId;

    /** Incremental snapshot topology version. */
    @Order(value = 6, method = "snapshotTopologyVersion")
    private long topVer;

    /** */
    public IncrementalSnapshotAwareMessage() {
    }

    /** */
    public IncrementalSnapshotAwareMessage(
        GridCacheMessage payload,
        UUID id,
        @Nullable UUID txSnpId,
        long topVer
    ) {
        this.payload = payload;
        this.id = id;
        this.txSnpId = txSnpId;
        this.topVer = topVer;
    }

    /** @return Incremental snapshot ID. */
    public UUID id() {
        return id;
    }

    /**
     * @param id Incremental snapshot ID.
     */
    public void id(UUID id) {
        this.id = id;
    }

    /** ID of the latest incremental snapshot after which this transaction committed. */
    public UUID txIncrementalSnapshotId() {
        return txSnpId;
    }

    /**
     * @param txSnpId ID of the latest incremental snapshot after which this transaction committed.
     */
    public void txIncrementalSnapshotId(UUID txSnpId) {
        this.txSnpId = txSnpId;
    }

    /** */
    public GridCacheMessage payload() {
        return payload;
    }

    /**
     * @param payload Original transaction message.
     */
    public void payload(GridCacheMessage payload) {
        this.payload = payload;
    }

    /** @return Incremental snapshot topology version. */
    public long snapshotTopologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Incremental snapshot topology version.
     */
    public void snapshotTopologyVersion(long topVer) {
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        payload.prepareMarshal(ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        payload.finishUnmarshal(ctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean cacheGroupMessage() {
        return false;
    }
}
