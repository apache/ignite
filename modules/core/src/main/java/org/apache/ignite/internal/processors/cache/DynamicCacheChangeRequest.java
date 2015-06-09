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

import java.io.*;
import java.util.*;

/**
 * Cache start/stop request.
 */
public class DynamicCacheChangeRequest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Start ID. */
    private IgniteUuid deploymentId;

    /** Stop cache name. */
    @GridToStringExclude
    private String cacheName;

    /** Cache start configuration. */
    private CacheConfiguration startCfg;

    /** Cache type. */
    private CacheType cacheType;

    /** Near node ID in case if near cache is being started. */
    private UUID initiatingNodeId;

    /** Near cache configuration. */
    private NearCacheConfiguration nearCacheCfg;

    /** Start only client cache, do not start data nodes. */
    private boolean clientStartOnly;

    /** Stop flag. */
    private boolean stop;

    /** Fail if exists flag. */
    private boolean failIfExists;

    /** Template configuration flag. */
    private boolean template;

    /**
     * Constructor creates cache stop request.
     *
     * @param cacheName Cache stop name.
     * @param initiatingNodeId Initiating node ID.
     * @param stop Stop flag.
     */
    public DynamicCacheChangeRequest(String cacheName, UUID initiatingNodeId, boolean stop) {
        this.cacheName = cacheName;
        this.initiatingNodeId = initiatingNodeId;

        this.stop = stop;
    }

    /**
     * Constructor means for start requests.
     *
     * @param cacheName Cache name.
     * @param initiatingNodeId Initiating node ID.
     */
    public DynamicCacheChangeRequest(String cacheName, UUID initiatingNodeId) {
        this(cacheName, initiatingNodeId, false);
    }

    /**
     * @param template {@code True} if this is request for adding template configuration.
     */
    public void template(boolean template) {
        this.template = template;
    }

    /**
     * @return {@code True} if this is template configuration.
     */
    public boolean template() {
        return template;
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
    public boolean start() {
        return !template && startCfg != null;
    }

    /**
     * @return {@code True} if this is a stop request.
     */
    public boolean stop() {
        return stop;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Near node ID.
     */
    public UUID initiatingNodeId() {
        return initiatingNodeId;
    }

    /**
     * @return Near cache configuration.
     */
    public NearCacheConfiguration nearCacheConfiguration() {
        return nearCacheCfg;
    }

    /**
     * @param nearCacheCfg Near cache configuration.
     */
    public void nearCacheConfiguration(NearCacheConfiguration nearCacheCfg) {
        this.nearCacheCfg = nearCacheCfg;
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration startCacheConfiguration() {
        return startCfg;
    }

    /**
     * @param startCfg Cache configuration.
     */
    public void startCacheConfiguration(CacheConfiguration startCfg) {
        this.startCfg = startCfg;
    }

    /**
     * @param cacheType Cache type.
     */
    public void cacheType(CacheType cacheType) {
        this.cacheType = cacheType;
    }

    /**
     * @return Cache type.
     */
    public CacheType cacheType() {
        return cacheType;
    }

    /**
     * @return Client start only.
     */
    public boolean clientStartOnly() {
        return clientStartOnly;
    }

    /**
     * @param clientStartOnly Client start only.
     */
    public void clientStartOnly(boolean clientStartOnly) {
        this.clientStartOnly = clientStartOnly;
    }

    /**
     * @return Fail if exists flag.
     */
    public boolean failIfExists() {
        return failIfExists;
    }

    /**
     * @param failIfExists Fail if exists flag.
     */
    public void failIfExists(boolean failIfExists) {
        this.failIfExists = failIfExists;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheChangeRequest.class, this, "cacheName", cacheName());
    }
}
