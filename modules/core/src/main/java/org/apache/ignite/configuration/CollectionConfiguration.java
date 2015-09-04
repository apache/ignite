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

import java.io.Serializable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Configuration for Ignite collections.
 */
public class CollectionConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache atomicity mode. */
    private CacheAtomicityMode atomicityMode = ATOMIC;

    /** Cache mode. */
    private CacheMode cacheMode = PARTITIONED;

    /** Cache memory mode. */
    private CacheMemoryMode memoryMode = ONHEAP_TIERED;

    /** Node filter specifying nodes on which this cache should be deployed. */
    private IgnitePredicate<ClusterNode> nodeFilter;

    /** Number of backups. */
    private int backups = 0;

    /** Off-heap memory size. */
    private long offHeapMaxMem = -1;

    /** Collocated flag. */
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
     * @return Cache memory mode.
     */
    public CacheMemoryMode getMemoryMode() {
        return memoryMode;
    }

    /**
     * @param memoryMode Memory mode.
     */
    public void setMemoryMode(CacheMemoryMode memoryMode) {
        this.memoryMode = memoryMode;
    }

    /**
     * @return Predicate specifying on which nodes the cache should be started.
     */
    public IgnitePredicate<ClusterNode> getNodeFilter() {
        return nodeFilter;
    }

    /**
     * @param nodeFilter Predicate specifying on which nodes the cache should be started.
     */
    public void setNodeFilter(IgnitePredicate<ClusterNode> nodeFilter) {
        this.nodeFilter = nodeFilter;
    }

    /**
     * @return Number of backups.
     */
    public int getBackups() {
        return backups;
    }

    /**
     * @param backups Cache number of backups.
     */
    public void setBackups(int backups) {
        this.backups = backups;
    }

    /**
     * @return Off-heap memory size.
     */
    public long getOffHeapMaxMemory() {
        return offHeapMaxMem;
    }

    /**
     * @param offHeapMaxMemory Off-heap memory size.
     */
    public void setOffHeapMaxMemory(long offHeapMaxMemory) {
        this.offHeapMaxMem = offHeapMaxMemory;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CollectionConfiguration.class, this);
    }
}