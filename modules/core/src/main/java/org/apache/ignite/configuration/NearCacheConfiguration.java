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
    private CacheEvictionPolicy<K, V> nearEvictPlc;

    /** Default near cache start size. */
    private int nearStartSize = DFLT_NEAR_START_SIZE;

    /**
     * Empty constructor.
     */
    public NearCacheConfiguration() {
        // No-op.
    }

    /**
     * @param ccfg Configuration to copy.
     */
    public NearCacheConfiguration(NearCacheConfiguration<K, V> ccfg) {
        super(ccfg);

        name = ccfg.getName();
        nearEnabled = ccfg.isNearEnabled();
        nearEvictPlc = ccfg.getNearEvictionPolicy();
        nearStartSize = ccfg.getNearStartSize();
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
    public CacheEvictionPolicy<K, V> getNearEvictionPolicy() {
        return nearEvictPlc;
    }

    /**
     * @param nearEvictPlc Near eviction policy.
     */
    public void setNearEvictionPolicy(CacheEvictionPolicy<K, V> nearEvictPlc) {
        this.nearEvictPlc = nearEvictPlc;
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
