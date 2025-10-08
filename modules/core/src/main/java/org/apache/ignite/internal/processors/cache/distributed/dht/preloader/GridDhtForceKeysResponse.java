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
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
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
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class GridDhtForceKeysResponse extends GridCacheIdMessage implements GridCacheDeployable {
    /** Future ID. */
    @Order(value = 4, method = "futureId")
    private IgniteUuid futId;

    /** Mini-future ID. */
    @Order(5)
    private IgniteUuid miniId;

    /** Error. */
    @Order(value = 6, method = "error")
    private volatile IgniteCheckedException err;

    /** Missed (not found) keys. */
    @GridToStringInclude
    @Order(7)
    private List<KeyCacheObject> missedKeys;

    /** Forced entries. */
    @GridToStringInclude
    @Order(8)
    private List<GridCacheEntryInfo> forcedInfos;

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
     * @param err New error.
     */
    public void error(IgniteCheckedException err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException error() {
        return err;
    }

    /**
     * @return Keys.
     */
    public Collection<KeyCacheObject> missedKeys() {
        return missedKeys;
    }

    /**
     * @param missedKeys New missed (not found) keys.
     */
    public void missedKeys(List<KeyCacheObject> missedKeys) {
        this.missedKeys = missedKeys;
    }

    /**
     * @return Forced entries.
     */
    public Collection<GridCacheEntryInfo> forcedInfos() {
        return forcedInfos;
    }

    /**
     * @param forcedInfos New forced entries.
     */
    public void forcedInfos(List<GridCacheEntryInfo> forcedInfos) {
        this.forcedInfos = forcedInfos;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param futId New future ID.
     */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /**
     * @return Mini-future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param miniId New mini-future ID.
     */
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

        if (forcedInfos == null)
            forcedInfos = new ArrayList<>();

        forcedInfos.add(info);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        if (missedKeys != null)
            prepareMarshalCacheObjects(missedKeys, cctx);

        if (forcedInfos != null) {
            for (GridCacheEntryInfo info : forcedInfos)
                info.marshal(cctx.cacheObjectContext());
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        if (missedKeys != null)
            finishUnmarshalCacheObjects(missedKeys, cctx, ldr);

        if (forcedInfos != null) {
            for (GridCacheEntryInfo info : forcedInfos)
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
