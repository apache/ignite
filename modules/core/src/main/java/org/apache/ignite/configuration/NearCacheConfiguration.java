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

package org.apache.ignite.configuration;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.*;

import javax.cache.configuration.*;

import static org.apache.ignite.configuration.CacheConfiguration.*;

/**
 * Client cache configuration.
 */
public class NearCacheConfiguration<K, V> extends MutableConfiguration<K, V> {
    /** Cache name. */
    private String name;

    /** Near enabled flag. */
    private boolean nearEnabled;

    /** Near cache eviction policy. */
    private CacheEvictionPolicy nearEvictPlc;

    /** Flag indicating whether eviction is synchronized with near nodes. */
    private boolean evictNearSync = DFLT_EVICT_NEAR_SYNCHRONIZED;

    /** Default near cache start size. */
    private int nearStartSize = DFLT_NEAR_START_SIZE;

    /**
     * Empty constructor.
     */
    public NearCacheConfiguration() {
        // No-op.
    }

    /**
     * @param cfg Configuration to copy.
     */
    public NearCacheConfiguration(CompleteConfiguration<K, V> cfg) {
        super(cfg);

        // Preserve alphabetic order.
        if (cfg instanceof CacheConfiguration) {
            CacheConfiguration ccfg = (CacheConfiguration)cfg;

            evictNearSync = ccfg.isEvictNearSynchronized();
            name = ccfg.getName();
            nearEnabled = ccfg.isNearEnabled();
            nearEvictPlc = ccfg.getNearEvictionPolicy();
            nearStartSize = ccfg.getNearStartSize();
        }
        else if (cfg instanceof NearCacheConfiguration) {
            NearCacheConfiguration ccfg = (NearCacheConfiguration)cfg;

            evictNearSync = ccfg.isEvictNearSynchronized();
            name = ccfg.getName();
            nearEnabled = ccfg.isNearEnabled();
            nearEvictPlc = ccfg.getNearEvictionPolicy();
            nearStartSize = ccfg.getNearStartSize();
        }
    }

    /**
     * Gets cache name. The cache can be accessed via {@link Ignite#jcache(String)} method.
     *
     * @return Cache name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets cache name.
     *
     * @param name Cache name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets near enabled flag.
     *
     * @return Near enabled flag.
     */
    public boolean isNearEnabled() {
        return nearEnabled;
    }

    /**
     * Sets near enabled flag.
     *
     * @param nearEnabled Near enabled flag.
     */
    public void setNearEnabled(boolean nearEnabled) {
        this.nearEnabled = nearEnabled;
    }

    /**
     * @return Near eviction policy.
     */
    public CacheEvictionPolicy getNearEvictionPolicy() {
        return nearEvictPlc;
    }

    /**
     * @param nearEvictPlc Near eviction policy.
     */
    public void setNearEvictionPolicy(CacheEvictionPolicy nearEvictPlc) {
        this.nearEvictPlc = nearEvictPlc;
    }

    /**
     * Gets flag indicating whether eviction on primary node is synchronized with
     * near nodes where entry is kept. Default value is {@code true}.
     * <p>
     * Note that in most cases this property should be set to {@code true} to keep
     * cache consistency. But there may be the cases when user may use some
     * special near eviction policy to have desired control over near cache
     * entry set.
     *
     * @return {@code true} If eviction is synchronized with near nodes in
     *      partitioned cache, {@code false} if not.
     */
    public boolean isEvictNearSynchronized() {
        return evictNearSync;
    }

    /**
     * Sets flag indicating whether eviction is synchronized with near nodes.
     *
     * @param evictNearSync {@code true} if synchronized, {@code false} if not.
     */
    public void setEvictNearSynchronized(boolean evictNearSync) {
        this.evictNearSync = evictNearSync;
    }

    /**
     * Gets initial cache size for near cache which will be used to pre-create internal
     * hash table after start. Default value is defined by {@link CacheConfiguration#DFLT_NEAR_START_SIZE}.
     *
     * @return Initial near cache size.
     */
    public int getNearStartSize() {
        return nearStartSize;
    }

    /**
     * Start size for near cache. This property is only used for {@link CacheMode#PARTITIONED} caching mode.
     *
     * @param nearStartSize Start size for near cache.
     */
    public void setNearStartSize(int nearStartSize) {
        this.nearStartSize = nearStartSize;
    }
}
