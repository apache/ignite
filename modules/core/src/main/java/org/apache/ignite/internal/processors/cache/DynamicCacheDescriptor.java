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

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.util.*;

/**
 * Cache start descriptor.
 */
public class DynamicCacheDescriptor {
    /** Cache start ID. */
    private IgniteUuid deploymentId;

    /** Cache configuration. */
    @GridToStringExclude
    private CacheConfiguration cacheCfg;

    /** Cancelled flag. */
    private boolean cancelled;

    /** Locally configured flag. */
    private boolean locCfg;

    /** Statically configured flag. */
    private boolean staticCfg;

    /** Started flag. */
    private boolean started;

    /** */
    private volatile Map<UUID, CacheConfiguration> rmtCfgs;

    /**
     * @param cacheCfg Cache configuration.
     */
    public DynamicCacheDescriptor(CacheConfiguration cacheCfg, IgniteUuid deploymentId) {
        this.cacheCfg = cacheCfg;
        this.deploymentId = deploymentId;
    }

    /**
     * @return Start ID.
     */
    public IgniteUuid deploymentId() {
        return deploymentId;
    }

    /**
     * @param deploymentId Deployment ID.
     */
    public void deploymentId(IgniteUuid deploymentId) {
        this.deploymentId = deploymentId;
    }

    /**
     * @return Locally configured flag.
     */
    public boolean locallyConfigured() {
        return locCfg;
    }

    /**
     * @param locCfg Locally configured flag.
     */
    public void locallyConfigured(boolean locCfg) {
        this.locCfg = locCfg;
    }

    /**
     * @return {@code True} if statically configured.
     */
    public boolean staticallyConfigured() {
        return staticCfg;
    }

    /**
     * @param staticCfg {@code True} if statically configured.
     */
    public void staticallyConfigured(boolean staticCfg) {
        this.staticCfg = staticCfg;
    }

    /**
     * @return {@code True} if started flag was flipped by this call.
     */
    public boolean onStart() {
        if (!started) {
            started = true;

            return true;
        }

        return false;
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration cacheConfiguration() {
        return cacheCfg;
    }

    /**
     * Sets cancelled flag.
     */
    public void onCancelled() {
        cancelled = true;
    }

    /**
     * @return Cancelled flag.
     */
    public boolean cancelled() {
        return cancelled;
    }

    /**
     * @param nodeId Remote node ID.
     * @return Configuration.
     */
    public CacheConfiguration remoteConfiguration(UUID nodeId) {
        Map<UUID, CacheConfiguration> cfgs = rmtCfgs;

        return cfgs == null ? null : cfgs.get(nodeId);
    }

    /**
     * @param nodeId Remote node ID.
     * @param cfg Remote node configuration.
     */
    public void addRemoteConfiguration(UUID nodeId, CacheConfiguration cfg) {
        Map<UUID, CacheConfiguration> cfgs = rmtCfgs;

        if (cfgs == null)
            rmtCfgs = cfgs = new HashMap<>();

        cfgs.put(nodeId, cfg);
    }

    /**
     *
     */
    public void clearRemoteConfigurations() {
        rmtCfgs = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheDescriptor.class, this, "cacheName", cacheCfg.getName());
    }
}
