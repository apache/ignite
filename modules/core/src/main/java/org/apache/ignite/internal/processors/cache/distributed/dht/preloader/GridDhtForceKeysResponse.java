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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Force keys response. Contains absent keys.
 */
public class GridDhtForceKeysResponse extends GridCacheIdMessage implements GridCacheDeployable {
    /** Error. */
    @Order(value = 4, method = "errorMessage")
    private volatile ErrorMessage err;

    /** Future ID. */
    @Order(value = 5, method = "futureId")
    private IgniteUuid futId;

    /** Cache entries. */
    @GridToStringInclude
    @Order(6)
    private List<GridCacheEntryInfo> infos;

    /** Mini-future ID. */
    @Order(7)
    private IgniteUuid miniId;

    /** Missed (not found) keys. */
    @GridToStringInclude
    @Order(8)
    private List<KeyCacheObject> missedKeys;

    /**
     * Empty constructor.
     */
    public GridDhtForceKeysResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Request id.
     * @param miniId Mini-future ID.
     * @param addDepInfo Deployment info flag.
     */
    public GridDhtForceKeysResponse(int cacheId, IgniteUuid futId, IgniteUuid miniId, boolean addDepInfo) {
        assert futId != null;
        assert miniId != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.addDepInfo = addDepInfo;
    }

    /**
     * Sets the error serialization message.
     *
     * @param err Error message.
     */
    public void errorMessage(ErrorMessage err) {
        this.err = err;
    }

    /**
     * @return The error serialization message.
     */
    public ErrorMessage errorMessage() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException error() {
        return err == null ? null : (IgniteCheckedException)err.toThrowable();
    }

    /** Sets the error. */
    public IgniteCheckedException error() {
        return err == null ? null : error();
    }

    /**
     * @return Keys.
     */
    public Collection<KeyCacheObject> missedKeys() {
        return missedKeys == null ? Collections.emptyList() : missedKeys;
    }

    /** */
    public void missedKeys(List<KeyCacheObject> missedKeys) {
        this.missedKeys = missedKeys;
    }

    /**
     * @return Forced entries.
     */
    public Collection<GridCacheEntryInfo> forcedInfos() {
        return infos == null ? Collections.emptyList() : infos;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /** */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /**
     * @return Mini-future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /** */
    public void miniId(IgniteUuid miniId) {
        this.miniId = miniId;
    }

    /**
     * @param key Key.
     */
    public void addMissed(KeyCacheObject key) {
        if (missedKeys == null)
            missedKeys = new ArrayList<>();

        missedKeys.add(key);
    }

    /**
     * @param info Entry info to add.
     */
    public void addInfo(GridCacheEntryInfo info) {
        assert info != null;

        if (infos == null)
            infos = new ArrayList<>();

        infos.add(info);
    }

    /** */
    public List<GridCacheEntryInfo> infos() {
        return infos;
    }

    /** */
    public void infos(List<GridCacheEntryInfo> infos) {
        this.infos = infos;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        if (missedKeys != null)
            prepareMarshalCacheObjects(missedKeys, cctx);

        if (infos != null) {
            for (GridCacheEntryInfo info : infos)
                info.marshal(cctx.cacheObjectContext());
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        if (missedKeys != null)
            finishUnmarshalCacheObjects(missedKeys, cctx, ldr);

        if (infos != null) {
            for (GridCacheEntryInfo info : infos)
                info.unmarshal(cctx.cacheObjectContext(), ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 43;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtForceKeysResponse.class, this, super.toString());
    }
}
