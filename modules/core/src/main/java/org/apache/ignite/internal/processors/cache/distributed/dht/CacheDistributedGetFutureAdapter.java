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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_NEAR_GET_MAX_REMAPS;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 *
 */
public abstract class CacheDistributedGetFutureAdapter<K, V> extends GridCompoundIdentityFuture<Map<K, V>>
    implements GridCacheFuture<Map<K, V>>, CacheGetFuture {
    /** Default max remap count value. */
    public static final int DFLT_MAX_REMAP_CNT = 3;

    /** Maximum number of attempts to remap key to the same primary node. */
    protected static final int MAX_REMAP_CNT = getInteger(IGNITE_NEAR_GET_MAX_REMAPS, DFLT_MAX_REMAP_CNT);

    /** Context. */
    protected final GridCacheContext<K, V> cctx;

    /** Keys. */
    protected Collection<KeyCacheObject> keys;

    /** Read through flag. */
    protected boolean readThrough;

    /** Force primary flag. */
    protected boolean forcePrimary;

    /** Future ID. */
    protected IgniteUuid futId;

    /** Trackable flag. */
    protected boolean trackable;

    /** Remap count. */
    protected AtomicInteger remapCnt = new AtomicInteger();

    /** Subject ID. */
    protected UUID subjId;

    /** Task name. */
    protected String taskName;

    /** Whether to deserialize portable objects. */
    protected boolean deserializePortable;

    /** Skip values flag. */
    protected boolean skipVals;

    /** Expiry policy. */
    protected IgniteCacheExpiryPolicy expiryPlc;

    /** Flag indicating that get should be done on a locked topology version. */
    protected final boolean canRemap;

    /** */
    protected final boolean needVer;

    /** */
    protected final boolean keepCacheObjects;

    /**
     * @param cctx Context.
     * @param keys Keys.
     * @param readThrough Read through flag.
     * @param forcePrimary If {@code true} then will force network trip to primary node even
     *          if called on backup node.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param deserializePortable Deserialize portable flag.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     * @param canRemap Flag indicating whether future can be remapped on a newer topology version.
     * @param needVer If {@code true} returns values as tuples containing value and version.
     * @param keepCacheObjects Keep cache objects flag.
     */
    protected CacheDistributedGetFutureAdapter(
        GridCacheContext<K, V> cctx,
        Collection<KeyCacheObject> keys,
        boolean readThrough,
        boolean forcePrimary,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals,
        boolean canRemap,
        boolean needVer,
        boolean keepCacheObjects
    ) {
        super(cctx.kernalContext(), CU.<K, V>mapsReducer(keys.size()));

        assert !F.isEmpty(keys);

        this.cctx = cctx;
        this.keys = keys;
        this.readThrough = readThrough;
        this.forcePrimary = forcePrimary;
        this.subjId = subjId;
        this.taskName = taskName;
        this.deserializePortable = deserializePortable;
        this.expiryPlc = expiryPlc;
        this.skipVals = skipVals;
        this.canRemap = canRemap;
        this.needVer = needVer;
        this.keepCacheObjects = keepCacheObjects;

        futId = IgniteUuid.randomUuid();
    }

    /**
     * @param map Result map.
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     */
    @SuppressWarnings("unchecked")
    protected final void versionedResult(Map map, KeyCacheObject key, Object val, GridCacheVersion ver) {
        assert needVer;
        assert skipVals || val != null;
        assert ver != null;

        map.put(key, new T2<>(skipVals ? true : val, ver));
    }

    /**
     * Affinity node to send get request to.
     *
     * @param key Key to get.
     * @param topVer Topology version.
     * @return Affinity node to get key from.
     */
    protected final ClusterNode affinityNode(KeyCacheObject key, AffinityTopologyVersion topVer) {
        if (!canRemap) {
            List<ClusterNode> affNodes = cctx.affinity().nodes(key, topVer);

            for (ClusterNode node : affNodes) {
                if (cctx.discovery().alive(node))
                    return node;
            }

            return null;
        }
        else
            return cctx.affinity().primary(key, topVer);
    }
}
