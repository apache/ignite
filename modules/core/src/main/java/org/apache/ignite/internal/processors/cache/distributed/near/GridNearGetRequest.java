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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Get request. Responsible for obtaining entry from primary node. 'Near' means 'Initiating node' here, not 'Near Cache'.
 */
public class GridNearGetRequest extends GridCacheIdMessage implements GridCacheDeployable,
    GridCacheVersionable {
    /** */
    private static final int READ_THROUGH_FLAG_MASK = 0x01;

    /** */
    private static final int SKIP_VALS_FLAG_MASK = 0x02;

    /** */
    private static final int ADD_READER_FLAG_MASK = 0x04;

    /** */
    public static final int RECOVERY_FLAG_MASK = 0x08;

    /** Future ID. */
    @Order(value = 4, method = "futureId")
    private IgniteUuid futId;

    /** Sub ID. */
    @Order(5)
    private IgniteUuid miniId;

    /** Version. */
    @Order(value = 6, method = "version")
    private GridCacheVersion ver;

    /** */
    @GridToStringInclude
    private LinkedHashMap<KeyCacheObject, Boolean> keyMap;

    /** */
    @Order(7)
    private List<KeyCacheObject> keys;

    /** */
    @Order(8)
    private List<Boolean> readersFlags;

    /** */
    @Order(9)
    private byte flags;

    /** Topology version. */
    @Order(value = 10, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** Task name hash. */
    @Order(11)
    private int taskNameHash;

    /** TTL for read operation. */
    @Order(12)
    private long createTtl;

    /** TTL for read operation. */
    @Order(13)
    private long accessTtl;

    /** Transaction label. */
    @Order(value = 14, method = "txLabel")
    private @Nullable String txLbl;

    /**
     * Empty constructor.
     */
    public GridNearGetRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param miniId Sub ID.
     * @param ver Version.
     * @param keys Keys.
     * @param readThrough Read through flag.
     * @param skipVals Skip values flag. When false, only boolean values will be returned indicating whether
     *      cache entry has a value.
     * @param topVer Topology version.
     * @param taskNameHash Task name hash.
     * @param createTtl New TTL to set after entry is created, -1 to leave unchanged.
     * @param accessTtl New TTL to set after entry is accessed, -1 to leave unchanged.
     * @param addDepInfo Deployment info.
     * @param txLbl Transaction label.
     */
    public GridNearGetRequest(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion ver,
        Map<KeyCacheObject, Boolean> keys,
        boolean readThrough,
        @NotNull AffinityTopologyVersion topVer,
        int taskNameHash,
        long createTtl,
        long accessTtl,
        boolean addReader,
        boolean skipVals,
        boolean addDepInfo,
        boolean recovery,
        @Nullable String txLbl
    ) {
        assert futId != null;
        assert miniId != null;
        assert keys != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.ver = ver;

        this.keys = new ArrayList<>(keys.size());

        if (addReader)
            readersFlags = new ArrayList<>(keys.size());

        for (Map.Entry<KeyCacheObject, Boolean> entry : keys.entrySet()) {
            this.keys.add(entry.getKey());

            if (addReader)
                readersFlags.add(entry.getValue());
        }

        this.topVer = topVer;
        this.taskNameHash = taskNameHash;
        this.createTtl = createTtl;
        this.accessTtl = accessTtl;
        this.addDepInfo = addDepInfo;
        this.txLbl = txLbl;

        if (readThrough)
            flags |= READ_THROUGH_FLAG_MASK;

        if (skipVals)
            flags |= SKIP_VALS_FLAG_MASK;

        if (addReader)
            flags |= ADD_READER_FLAG_MASK;

        if (recovery)
            flags |= RECOVERY_FLAG_MASK;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param futId Future ID.
     */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /**
     * @return Sub ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param miniId Sub ID.
     */
    public void miniId(IgniteUuid miniId) {
        this.miniId = miniId;
    }

    /**
     * Gets task name hash.
     *
     * @return Task name hash.
     */
    public int taskNameHash() {
        return taskNameHash;
    }

    /**
     * @param taskNameHash Task name hash.
     */
    public void taskNameHash(int taskNameHash) {
        this.taskNameHash = taskNameHash;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @param ver Version.
     */
    public void version(GridCacheVersion ver) {
        this.ver = ver;
    }

    /**
     * @return Keys.
     */
    public LinkedHashMap<KeyCacheObject, Boolean> keyMap() {
        return keyMap;
    }

    /**
     * @return Keys.
     */
    public List<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @param keys Keys.
     */
    public void keys(List<KeyCacheObject> keys) {
        this.keys = keys;
    }

    /**
     * @return Readers flags.
     */
    public List<Boolean> readersFlags() {
        return readersFlags;
    }

    /**
     * @param readersFlags Readers flags.
     */
    public void readersFlags(List<Boolean> readersFlags) {
        this.readersFlags = readersFlags;
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
     * @return Read through flag.
     */
    public boolean readThrough() {
        return (flags & READ_THROUGH_FLAG_MASK) != 0;
    }

    /**
     * @return Skip values flag. If true, boolean values indicating whether cache entry has a value will be
     *      returned as future result.
     */
    public boolean skipValues() {
        return (flags & SKIP_VALS_FLAG_MASK) != 0;
    }

    /**
     * @return Recovery flag.
     */
    public boolean recovery() {
        return (flags & RECOVERY_FLAG_MASK) != 0;
    }

    /** */
    public boolean addReaders() {
        return (flags & ADD_READER_FLAG_MASK) != 0;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return New TTL to set after entry is created, -1 to leave unchanged.
     */
    public long createTtl() {
        return createTtl;
    }

    /**
     * @param createTtl New TTL to set after entry is created, -1 to leave unchanged.
     */
    public void createTtl(long createTtl) {
        this.createTtl = createTtl;
    }

    /**
     * @return New TTL to set after entry is accessed, -1 to leave unchanged.
     */
    public long accessTtl() {
        return accessTtl;
    }

    /**
     * @param accessTtl New TTL to set after entry is accessed, -1 to leave unchanged.
     */
    public void accessTtl(long accessTtl) {
        this.accessTtl = accessTtl;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return keys != null && !keys.isEmpty() ? keys.get(0).partition() : -1;
    }

    /**
     * Get transaction label (may be null).
     *
     * @return Possible transaction label;
     */
    @Nullable public String txLabel() {
        return txLbl;
    }

    /**
     * @param txLbl Possible transaction label.
     */
    public void txLabel(String txLbl) {
        this.txLbl = txLbl;
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        assert ctx != null;
        assert !F.isEmpty(keys);
        assert readersFlags == null || keys.size() == readersFlags.size();

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        prepareMarshalCacheObjects(keys, cctx);
    }

    /**
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        finishUnmarshalCacheObjects(keys, cctx, ldr);

        assert !F.isEmpty(keys);
        assert readersFlags == null || keys.size() == readersFlags.size();

        if (keyMap == null) {
            keyMap = U.newLinkedHashMap(keys.size());

            Iterator<KeyCacheObject> keysIt = keys.iterator();

            for (int i = 0; i < keys.size(); i++) {
                Boolean addRdr = readersFlags != null ? readersFlags.get(i) : Boolean.FALSE;

                keyMap.put(keysIt.next(), addRdr);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 49;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearGetRequest.class, this);
    }
}
