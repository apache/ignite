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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Specific cache information for start.
 */
public class StartCacheInfo {
    /** Cache configuration for start. */
    private final CacheConfiguration startedConf;

    /** Cache descriptor for start. */
    private final DynamicCacheDescriptor desc;

    /** Near cache configuration for start. */
    private final @Nullable NearCacheConfiguration reqNearCfg;

    /** Exchange topology version in which starting happened. */
    private final AffinityTopologyVersion exchTopVer;

    /** Disable started cache after start or not. */
    private final boolean disabledAfterStart;

    /** Cache is client or not. */
    private final boolean clientCache;

    /**
     * @param desc Cache configuration for start.
     * @param reqNearCfg Near cache configuration for start.
     * @param exchTopVer Exchange topology version in which starting happened.
     * @param disabledAfterStart Disable started cache after start or not.
     */
    public StartCacheInfo(DynamicCacheDescriptor desc,
        NearCacheConfiguration reqNearCfg,
        AffinityTopologyVersion exchTopVer, boolean disabledAfterStart) {
        this(desc.cacheConfiguration(), desc, reqNearCfg, exchTopVer, disabledAfterStart);
    }

    /**
     * @param conf Cache configuration for start.
     * @param desc Cache descriptor for start.
     * @param reqNearCfg Near cache configuration for start.
     * @param exchTopVer Exchange topology version in which starting happened.
     * @param disabledAfterStart Disable started cache after start or not.
     */
    public StartCacheInfo(CacheConfiguration conf, DynamicCacheDescriptor desc,
        NearCacheConfiguration reqNearCfg,
        AffinityTopologyVersion exchTopVer, boolean disabledAfterStart) {
        this(conf, desc, reqNearCfg, exchTopVer, disabledAfterStart, false);
    }

    /**
     * @param conf Cache configuration for start.
     * @param desc Cache descriptor for start.
     * @param reqNearCfg Near cache configuration for start.
     * @param exchTopVer Exchange topology version in which starting happened.
     * @param disabledAfterStart Disable started cache after start or not.
     * @param clientCache {@code true} in case starting cache on client node.
     */
    public StartCacheInfo(CacheConfiguration conf, DynamicCacheDescriptor desc,
        NearCacheConfiguration reqNearCfg,
        AffinityTopologyVersion exchTopVer, boolean disabledAfterStart, boolean clientCache) {
        startedConf = conf;
        this.desc = desc;
        this.reqNearCfg = reqNearCfg;
        this.exchTopVer = exchTopVer;
        this.disabledAfterStart = disabledAfterStart;
        this.clientCache = clientCache;
    }

    /**
     * @return Cache configuration for start.
     */
    public CacheConfiguration getStartedConfiguration() {
        return startedConf;
    }

    /**
     * @return Cache descriptor for start.
     */
    public DynamicCacheDescriptor getCacheDescriptor() {
        return desc;
    }

    /**
     * @return Near cache configuration for start.
     */
    @Nullable public NearCacheConfiguration getReqNearCfg() {
        return reqNearCfg;
    }

    /**
     * @return Exchange topology version in which starting happened.
     */
    public AffinityTopologyVersion getExchangeTopVer() {
        return exchTopVer;
    }

    /**
     * @return Disable started cache after start or not.
     */
    public boolean isDisabledAfterStart() {
        return disabledAfterStart;
    }

    /**
     * @return Start cache on client or not.
     */
    public boolean isClientCache() {
        return clientCache;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StartCacheInfo.class, this);
    }
}
