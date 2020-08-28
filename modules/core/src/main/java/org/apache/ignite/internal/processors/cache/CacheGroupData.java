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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheGroupData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

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
    private final CacheConfiguration<?, ?> cacheCfg;

    /** */
    @GridToStringInclude
    private final Map<String, Integer> caches;

    /** */
    private long flags;

    /** Persistence enabled flag. */
    private final boolean persistenceEnabled;

    /** WAL state. */
    private final boolean walEnabled;

    /** WAL change requests. */
    private final List<WalStateProposeMessage> walChangeReqs;

    /** Cache configuration enrichment. */
    private final CacheConfigurationEnrichment cacheCfgEnrichment;

    /**
     * @param cacheCfg Cache configuration.
     * @param grpName Group name.
     * @param grpId Group ID.
     * @param rcvdFrom Node ID cache group received from.
     * @param startTopVer Start version for dynamically started group.
     * @param deploymentId Deployment ID.
     * @param caches Cache group caches.
     * @param persistenceEnabled Persistence enabled flag.
     * @param walEnabled WAL state.
     * @param walChangeReqs WAL change requests.
     * @param cacheCfgEnrichment Cache configuration enrichment.
     */
    CacheGroupData(
        CacheConfiguration cacheCfg,
        @Nullable String grpName,
        int grpId,
        UUID rcvdFrom,
        @Nullable AffinityTopologyVersion startTopVer,
        IgniteUuid deploymentId,
        Map<String, Integer> caches,
        long flags,
        boolean persistenceEnabled,
        boolean walEnabled,
        List<WalStateProposeMessage> walChangeReqs,
        CacheConfigurationEnrichment cacheCfgEnrichment
    ) {
        assert cacheCfg != null;
        assert grpId != 0 : cacheCfg.getName();
        assert deploymentId != null : cacheCfg.getName();

        this.cacheCfg = cacheCfg;
        this.grpName = grpName;
        this.grpId = grpId;
        this.rcvdFrom = rcvdFrom;
        this.startTopVer = startTopVer;
        this.deploymentId = deploymentId;
        this.caches = caches;
        this.flags = flags;
        this.persistenceEnabled = persistenceEnabled;
        this.walEnabled = walEnabled;
        this.walChangeReqs = walChangeReqs;
        this.cacheCfgEnrichment = cacheCfgEnrichment;
    }

    /**
     * @return Start version for dynamically started group.
     */
    @Nullable public AffinityTopologyVersion startTopologyVersion() {
        return startTopVer;
    }

    /**
     * @return Node ID group was received from.
     */
    public UUID receivedFrom() {
        return rcvdFrom;
    }

    /**
     * @return Group name.
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
     * @return Deployment ID.
     */
    public IgniteUuid deploymentId() {
        return deploymentId;
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
    Map<String, Integer> caches() {
        return caches;
    }

    /**
     * @return Persistence enabled flag.
     */
    public boolean persistenceEnabled() {
        return persistenceEnabled;
    }

    /**
     * @return {@code True} if WAL is enabled.
     */
    public boolean walEnabled() {
        return walEnabled;
    }

    /**
     * @return WAL mode change requests.
     */
    public List<WalStateProposeMessage> walChangeRequests() {
        return walChangeReqs;
    }

    /**
     * @return Cache configuration enrichment.
     */
    public CacheConfigurationEnrichment cacheConfigurationEnrichment() {
        return cacheCfgEnrichment;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheGroupData.class, this);
    }
}
