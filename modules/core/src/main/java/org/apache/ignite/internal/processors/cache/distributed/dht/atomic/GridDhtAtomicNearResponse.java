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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateRequest.DHT_ATOMIC_HAS_RESULT_MASK;

/**
 * Message sent from DHT nodes to near node in FULL_SYNC mode.
 */
public class GridDhtAtomicNearResponse extends GridCacheIdMessage {
    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** */
    @Order(value = 4, method = "partition")
    private int partId;

    /** */
    @Order(value = 5, method = "futureId")
    private long futId;

    /** */
    @Order(6)
    private UUID primaryId;

    /** */
    @Order(7)
    @GridToStringExclude
    private byte flags;

    /** */
    @Order(value = 8, method = "errors")
    @GridToStringInclude
    private UpdateErrors errs;

    /**
     *
     */
    public GridDhtAtomicNearResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param partId Partition.
     * @param futId Future ID.
     * @param primaryId Primary node ID.
     * @param flags Flags.
     */
    public GridDhtAtomicNearResponse(int cacheId,
        int partId,
        long futId,
        UUID primaryId,
        byte flags
    ) {
        assert primaryId != null;

        this.cacheId = cacheId;
        this.partId = partId;
        this.futId = futId;
        this.primaryId = primaryId;
        this.flags = flags;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /**
     * @param partId Partition ID.
     */
    public void partition(int partId) {
        this.partId = partId;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /**
     * @param futId Future ID.
     */
    public void futureId(long futId) {
        this.futId = futId;
    }

    /**
     * @return Primary node ID.
     */
    public UUID primaryId() {
        return primaryId;
    }

    /**
     * @param primaryId Primary node ID.
     */
    public void primaryId(UUID primaryId) {
        this.primaryId = primaryId;
    }

    /**
     * @return Flags.
     */
    public byte flags() {
        return flags;
    }

    /**
     * @param flags Flags.
     */
    public void flags(byte flags) {
        this.flags = flags;
    }

    /**
     * @return Errors.
     */
    @Nullable public UpdateErrors errors() {
        return errs;
    }

    /**
     * @param errs Errors.
     */
    public void errors(UpdateErrors errs) {
        this.errs = errs;
    }

    /**
     * @param key Key.
     * @param e Error.
     */
    public void addFailedKey(KeyCacheObject key, Throwable e) {
        if (errs == null)
            errs = new UpdateErrors();

        errs.addFailedKey(key, e);
    }

    /**
     * @return Operation result.
     */
    public GridCacheReturn result() {
        assert hasResult() : this;

        return new GridCacheReturn(true, true);
    }

    /**
     * @return {@code True} if response contains operation result.
     */
    boolean hasResult() {
        return isFlag(DHT_ATOMIC_HAS_RESULT_MASK);
    }

    /**
     * Reads flag mask.
     *
     * @param mask Mask to read.
     * @return Flag value.
     */
    private boolean isFlag(int mask) {
        return (flags & mask) != 0;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -48;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (errs != null)
            errs.prepareMarshal(this, ctx.cacheContext(cacheId));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (errs != null)
            errs.finishUnmarshal(this, ctx.cacheContext(cacheId), ldr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder flags = new StringBuilder();

        if (hasResult())
            appendFlag(flags, "hasRes");

        return S.toString(GridDhtAtomicNearResponse.class, this,
            "flags", flags.toString());
    }
}
