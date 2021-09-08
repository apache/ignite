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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
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

    /** Node filter specifying nodes on which this cache should be deployed. */
    private IgnitePredicate<ClusterNode> nodeFilter;

    /** Number of backups. */
    private int backups = 0;

    /** Off-heap memory size. */
    private long offHeapMaxMem = -1;

    /** Collocated flag. */
    private boolean collocated;

    /** Group name. */
    private String grpName;

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
     *
     * @return {@code this} for chaining.
     */
    public CollectionConfiguration setCollocated(boolean collocated) {
        this.collocated = collocated;

        return this;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode getAtomicityMode() {
        return atomicityMode;
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @return {@code this} for chaining.
     */
    public CollectionConfiguration setAtomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;

        return this;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * @param cacheMode Cache mode.
     * @return {@code this} for chaining.
     */
    public CollectionConfiguration setCacheMode(CacheMode cacheMode) {
        this.cacheMode = cacheMode;

        return this;
    }

    /**
     * @return Predicate specifying on which nodes the cache should be started.
     */
    public IgnitePredicate<ClusterNode> getNodeFilter() {
        return nodeFilter;
    }

    /**
     * @param nodeFilter Predicate specifying on which nodes the cache should be started.
     * @return {@code this} for chaining.
     */
    public CollectionConfiguration setNodeFilter(IgnitePredicate<ClusterNode> nodeFilter) {
        this.nodeFilter = nodeFilter;

        return this;
    }

    /**
     * @return Number of backups.
     */
    public int getBackups() {
        return backups;
    }

    /**
     * @param backups Cache number of backups.
     * @return {@code this} for chaining.
     */
    public CollectionConfiguration setBackups(int backups) {
        this.backups = backups;

        return this;
    }

    /**
     * @return Off-heap memory size.
     */
    public long getOffHeapMaxMemory() {
        return offHeapMaxMem;
    }

    /**
     * @param offHeapMaxMemory Off-heap memory size.
     * @return {@code this} for chaining.
     */
    public CollectionConfiguration setOffHeapMaxMemory(long offHeapMaxMemory) {
        this.offHeapMaxMem = offHeapMaxMemory;

        return this;
    }

    /**
     * @return Group name.
     */
    public String getGroupName() {
        return grpName;
    }

    /**
     * @param grpName Group name.
     * @return {@code this} for chaining.
     */
    public CollectionConfiguration setGroupName(String grpName) {
        this.grpName = grpName;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CollectionConfiguration.class, this);
    }
}
