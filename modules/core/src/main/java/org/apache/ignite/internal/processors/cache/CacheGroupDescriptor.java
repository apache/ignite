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

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheGroupDescriptor {
    /** */
    private final int grpId;

    /** */
    private final String grpName;

    /** */
    private final AffinityTopologyVersion startTopVer;

    /** */
    private final UUID rcvdFrom;

    /** */
    private final IgniteUuid deploymentId;

    /** */
    @GridToStringExclude
    private final CacheConfiguration<?, ?> cacheCfg;

    /** */
    @GridToStringInclude
    private Map<String, Integer> caches;

    /** */
    private AffinityTopologyVersion rcvdFromVer;

    /** Persistence enabled flag. */
    private final boolean persistenceEnabled;

    /** Wal mode */
    private volatile CacheGroupWalMode walMode;

    /**
     * @param cacheCfg Cache configuration.
     * @param grpName Group name.
     * @param grpId Group ID.
     * @param rcvdFrom Node ID cache group received from.
     * @param startTopVer Start version for dynamically started group.
     * @param deploymentId Deployment ID.
     * @param caches Cache group caches.
     * @param persistenceEnabled Persistence enabled flag.
     */
    CacheGroupDescriptor(
        CacheConfiguration cacheCfg,
        @Nullable String grpName,
        int grpId,
        UUID rcvdFrom,
        @Nullable AffinityTopologyVersion startTopVer,
        IgniteUuid deploymentId,
        Map<String, Integer> caches,
        boolean persistenceEnabled,
        CacheGroupWalMode walMode) {
        assert cacheCfg != null;
        assert grpId != 0;

        this.grpName = grpName;
        this.grpId = grpId;
        this.rcvdFrom = rcvdFrom;
        this.startTopVer = startTopVer;
        this.deploymentId = deploymentId;
        this.cacheCfg = new CacheConfiguration<>(cacheCfg);
        this.caches = caches;
        this.persistenceEnabled = persistenceEnabled;
        this.walMode = walMode;
    }

    /**
     * @return Node ID group was received from.
     */
    public UUID receivedFrom() {
        return rcvdFrom;
    }

    /**
     * @return Deployment ID.
     */
    public IgniteUuid deploymentId() {
        return deploymentId;
    }

    /**
     *
     */
    public CacheGroupWalMode walMode() {
        return walMode;
    }

    /**
     *
     */
    public void walMode(CacheGroupWalMode walMode) {
        assert (this.walMode == CacheGroupWalMode.ENABLING && walMode == CacheGroupWalMode.ENABLE) ||
            (this.walMode == CacheGroupWalMode.ENABLE && walMode == CacheGroupWalMode.DISABLING) ||
            (this.walMode == CacheGroupWalMode.DISABLING && walMode == CacheGroupWalMode.DISABLE) ||
            (this.walMode == CacheGroupWalMode.DISABLE && walMode == CacheGroupWalMode.ENABLING) ||
            this.walMode == walMode: //todo: this line is incorrect and should be failoved properly
            "Unexpected modification [current=" + this.walMode + " ,new=" + walMode + "]";

        this.walMode = walMode;
    }

    /**
     * @param cacheName Cache name
     * @param cacheId Cache ID.
     */
    void onCacheAdded(String cacheName, int cacheId) {
        assert cacheName != null;
        assert cacheId != 0 : cacheName;

        Map<String, Integer> caches = new HashMap<>(this.caches);

        caches.put(cacheName, cacheId);

        this.caches = caches;
    }

    /**
     * @param cacheName Cache name
     * @param cacheId Cache ID.
     */
    void onCacheStopped(String cacheName, int cacheId) {
        assert cacheName != null;
        assert cacheId != 0;

        Map<String, Integer> caches = new HashMap<>(this.caches);

        Integer rmvd = caches.remove(cacheName);

        assert rmvd != null && rmvd == cacheId : cacheName;

        this.caches = caches;
    }

    /**
     * @return {@code True} if group contains cache.
     */
    boolean hasCaches() {
        return caches != null && !caches.isEmpty();
    }

    /**
     * @return {@code True} if group can contain multiple caches.
     */
    public boolean sharedGroup() {
        return grpName != null;
    }

    /**
     * @return Group name if it is specified, otherwise cache name.
     */
    public String cacheOrGroupName() {
        return grpName != null ? grpName : cacheCfg.getName();
    }

    /**
     * @return Group name or {@code null} if group name was not specified for cache.
     */
    @Nullable public String groupName() {
        return grpName;
    }

    /**
     * @return Group ID.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @return Configuration.
     */
    public CacheConfiguration<?, ?> config() {
        return cacheCfg;
    }

    /**
     * @return Group caches.
     */
    public Map<String, Integer> caches() {
        return caches;
    }

    /**
     * @return Topology version when node provided cache configuration was started.
     */
    @Nullable AffinityTopologyVersion receivedFromStartVersion() {
        return rcvdFromVer;
    }

    /**
     * @param rcvdFromVer Topology version when node provided cache configuration was started.
     */
    void receivedFromStartVersion(AffinityTopologyVersion rcvdFromVer) {
        this.rcvdFromVer = rcvdFromVer;
    }

    /**
     * Method to merge this CacheGroup descriptor with another one.
     *
     * @param otherDesc CacheGroup descriptor that must be merged with this one.
     */
    void mergeWith(CacheGroupDescriptor otherDesc) {
        assert otherDesc != null && otherDesc.config() != null : otherDesc;

        CacheConfiguration otherCfg = otherDesc.config();

        cacheCfg.setRebalanceDelay(otherCfg.getRebalanceDelay());
        cacheCfg.setRebalanceBatchesPrefetchCount(otherCfg.getRebalanceBatchesPrefetchCount());
        cacheCfg.setRebalanceBatchSize(otherCfg.getRebalanceBatchSize());
        cacheCfg.setRebalanceOrder(otherCfg.getRebalanceOrder());
        cacheCfg.setRebalanceThrottle(otherCfg.getRebalanceThrottle());
        cacheCfg.setRebalanceTimeout(otherCfg.getRebalanceTimeout());
    }

    /**
     * @return Start version for dynamically started group.
     */
    @Nullable public AffinityTopologyVersion startTopologyVersion() {
        return startTopVer;
    }

    /**
     * @return Persistence enabled flag.
     */
    public boolean persistenceEnabled() {
        return persistenceEnabled;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheGroupDescriptor.class, this, "cacheName", cacheCfg.getName());
    }
}
