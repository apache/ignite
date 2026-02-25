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

package org.apache.ignite.internal;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public final class TxEntriesInfo extends IgniteDiagnosticRequest.DiagnosticBaseInfo {
    /** */
    @Order(0)
    int cacheId;

    /** */
    @Order(1)
    Collection<KeyCacheObject> keys;

    /**
     * Empty constructor required by {@link GridIoMessageFactory}.
     */
    public TxEntriesInfo() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param keys    Keys.
     */
    TxEntriesInfo(int cacheId, Collection<KeyCacheObject> keys) {
        this.cacheId = cacheId;
        this.keys = new HashSet<>(keys);
    }

    /** */
    public int cacheId() {
        return cacheId;
    }

    /** */
    public void cacheId(int cacheId) {
        this.cacheId = cacheId;
    }

    /** */
    public Collection<KeyCacheObject> keys() {
        return keys;
    }

    /** */
    public void keys(Collection<KeyCacheObject> keys) {
        this.keys = keys;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -64;
    }

    /** {@inheritDoc} */
    @Override public void appendInfo(StringBuilder sb, GridKernalContext ctx) {
        sb.append(U.nl());

        GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(cacheId);

        if (cctx == null) {
            sb.append("Failed to find cache with id: ").append(cacheId);

            return;
        }

        try {
            for (KeyCacheObject key : keys)
                key.finishUnmarshal(cctx.cacheObjectContext(), null);
        }
        catch (IgniteCheckedException e) {
            ctx.cluster().diagnosticLog().error("Failed to unmarshal key: " + e, e);

            sb.append("Failed to unmarshal key: ").append(e).append(U.nl());
        }

        sb.append("Cache entries [cacheId=").append(cacheId)
            .append(", cacheName=").append(cctx.name()).append("]: ");

        for (KeyCacheObject key : keys) {
            GridCacheMapEntry e = (GridCacheMapEntry)cctx.cache().peekEx(key);

            sb.append(U.nl()).append("    Key [key=").append(key).append(", entry=").append(e).append("]");
        }
    }

    /** {@inheritDoc} */
    @Override public void merge(IgniteDiagnosticRequest.DiagnosticBaseInfo other) {
        TxEntriesInfo other0 = (TxEntriesInfo)other;

        assert other0 != null && cacheId == other0.cacheId : other;

        keys.addAll(other0.keys);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TxEntriesInfo that = (TxEntriesInfo)o;

        return cacheId == that.cacheId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(getClass(), cacheId);
    }
}
