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

import org.apache.ignite.cache.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class IgniteCollectionConfiguration {
    /** Default backups number. */
    public static final int DFLT_BACKUPS = 0;

    /** Default cache mode. */
    public static final CacheMode DFLT_CACHE_MODE = PARTITIONED;

    /** Default atomicity mode. */
    public static final CacheAtomicityMode DFLT_ATOMICITY_MODE = ATOMIC;

    /** Default memory mode. */
    public static final CacheMemoryMode DFLT_MEMORY_MODE = ONHEAP_TIERED;

    /** Default distribution mode. */
    public static final CacheDistributionMode DFLT_DISTRIBUTION_MODE = PARTITIONED_ONLY;

    /** Cache mode. */
    private CacheMode cacheMode = DFLT_CACHE_MODE;

    /** Cache distribution mode. */
    private CacheDistributionMode distro = DFLT_DISTRIBUTION_MODE;

    /** Number of backups. */
    private int backups = DFLT_BACKUPS;

    /** Atomicity mode. */
    private CacheAtomicityMode atomicityMode = DFLT_ATOMICITY_MODE;

    /** Memory mode. */
    private CacheMemoryMode memMode = DFLT_MEMORY_MODE;

    /** */
    private boolean collocated;

    /**
     * @return {@code True} if all items within the same collection will be collocated on the same node.
     */
    public boolean isCollocated() {
        return collocated;
    }

    /**
     * @param collocated If {@code true} then all items within the same collection will be collocated on the same node.
     *      Otherwise elements of the same set maybe be cached on different nodes. This parameter works only
     *      collections stored in {@link CacheMode#PARTITIONED} cache.
     */
    public void setCollocated(boolean collocated) {
        this.collocated = collocated;
    }

    /**
     * @return Number of cache backups.
     */
    public int getBackups() {
        return backups;
    }

    /**
     * @param backups Number of cache backups.
     */
    public void setBackups(int backups) {
        this.backups = backups;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * @param cacheMode Cache mode.
     */
    public void setCacheMode(CacheMode cacheMode) {
        this.cacheMode = cacheMode;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode getAtomicityMode() {
        return atomicityMode;
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     */
    public void setAtomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;
    }

    /**
     * @return Cache memory mode.
     */
    public CacheMemoryMode getMemoryMode() {
        return memMode;
    }

    /**
     * @param memMode Cache memory mode.
     */
    public void setMemoryMode(CacheMemoryMode memMode) {
        this.memMode = memMode;
    }

    /**
     * @return Cache distribution mode.
     */
    public CacheDistributionMode getDistributionMode() {
        return distro;
    }

    /**
     * @param distro Cache distribution mode.
     */
    public void setDistributionMode(CacheDistributionMode distro) {
        this.distro = distro;
    }
}
