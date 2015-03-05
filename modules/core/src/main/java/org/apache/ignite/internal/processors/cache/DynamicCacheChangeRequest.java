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

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.io.*;

/**
 * Cache start/stop request.
 */
public class DynamicCacheChangeRequest implements Serializable {
    /** Start ID. */
    private IgniteUuid deploymentId;

    /** Stop cache name. */
    @GridToStringExclude
    private final String stopName;

    /** Cache start configuration. */
    private final CacheConfiguration startCfg;

    /** Cache start node filter. */
    private final IgnitePredicate<ClusterNode> startNodeFltr;

    /**
     * Constructor creates cache start request.
     *
     * @param startCfg Start cache configuration.
     * @param startNodeFltr Start node filter.
     */
    public DynamicCacheChangeRequest(
        CacheConfiguration startCfg,
        IgnitePredicate<ClusterNode> startNodeFltr
    ) {
        this.startCfg = startCfg;
        this.startNodeFltr = startNodeFltr;

        deploymentId = IgniteUuid.randomUuid();
        stopName = null;
    }

    /**
     * Constructor creates cache stop request.
     *
     * @param stopName Cache stop name.
     */
    public DynamicCacheChangeRequest(String stopName) {
        this.stopName = stopName;

        startCfg = null;
        startNodeFltr = null;
    }

    /**
     * @return Deployment ID.
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
     * @return {@code True} if this is a start request.
     */
    public boolean isStart() {
        return startCfg != null;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return stopName != null ? stopName : startCfg.getName();
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration startCacheConfiguration() {
        return startCfg;
    }

    /**
     * @return Node filter.
     */
    public IgnitePredicate<ClusterNode> startNodeFilter() {
        return startNodeFltr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheChangeRequest.class, this, "cacheName", cacheName());
    }
}
