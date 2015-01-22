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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for eviction configuration properties.
 */
public class VisorCacheEvictionConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Eviction policy. */
    private String plc;

    /** Cache eviction policy max size. */
    private Integer plcMaxSize;

    /** Eviction filter to specify which entries should not be evicted. */
    private String filter;

    /** Synchronous eviction concurrency level. */
    private int syncConcurrencyLvl;

    /** Synchronous eviction timeout. */
    private long syncTimeout;

    /** Synchronized key buffer size. */
    private int syncKeyBufSize;

    /** Synchronous evicts flag. */
    private boolean evictSynchronized;

    /** Synchronous near evicts flag. */
    private boolean nearSynchronized;

    /** Eviction max overflow ratio. */
    private float maxOverflowRatio;

    /**
     * @param ccfg Cache configuration.
     * @return Data transfer object for eviction configuration properties.
     */
    public static VisorCacheEvictionConfiguration from(CacheConfiguration ccfg) {
        VisorCacheEvictionConfiguration cfg = new VisorCacheEvictionConfiguration();

        final GridCacheEvictionPolicy plc = ccfg.getEvictionPolicy();

        cfg.policy(compactClass(plc));
        cfg.policyMaxSize(evictionPolicyMaxSize(plc));
        cfg.filter(compactClass(ccfg.getEvictionFilter()));
        cfg.synchronizedConcurrencyLevel(ccfg.getEvictSynchronizedConcurrencyLevel());
        cfg.synchronizedTimeout(ccfg.getEvictSynchronizedTimeout());
        cfg.synchronizedKeyBufferSize(ccfg.getEvictSynchronizedKeyBufferSize());
        cfg.evictSynchronized(ccfg.isEvictSynchronized());
        cfg.nearSynchronized(ccfg.isEvictNearSynchronized());
        cfg.maxOverflowRatio(ccfg.getEvictMaxOverflowRatio());

        return cfg;
    }

    /**
     * @return Eviction policy.
     */
    @Nullable public String policy() {
        return plc;
    }

    /**
     * @param plc New eviction policy.
     */
    public void policy(String plc) {
        this.plc = plc;
    }

    /**
     * @return Cache eviction policy max size.
     */
    @Nullable public Integer policyMaxSize() {
        return plcMaxSize;
    }

    /**
     * @param plcMaxSize New cache eviction policy max size.
     */
    public void policyMaxSize(Integer plcMaxSize) {
        this.plcMaxSize = plcMaxSize;
    }

    /**
     * @return Eviction filter to specify which entries should not be evicted.
     */
    @Nullable public String filter() {
        return filter;
    }

    /**
     * @param filter New eviction filter to specify which entries should not be evicted.
     */
    public void filter(String filter) {
        this.filter = filter;
    }

    /**
     * @return synchronized eviction concurrency level.
     */
    public int synchronizedConcurrencyLevel() {
        return syncConcurrencyLvl;
    }

    /**
     * @param syncConcurrencyLvl New synchronized eviction concurrency level.
     */
    public void synchronizedConcurrencyLevel(int syncConcurrencyLvl) {
        this.syncConcurrencyLvl = syncConcurrencyLvl;
    }

    /**
     * @return synchronized eviction timeout.
     */
    public long synchronizedTimeout() {
        return syncTimeout;
    }

    /**
     * @param syncTimeout New synchronized eviction timeout.
     */
    public void synchronizedTimeout(long syncTimeout) {
        this.syncTimeout = syncTimeout;
    }

    /**
     * @return Synchronized key buffer size.
     */
    public int synchronizedKeyBufferSize() {
        return syncKeyBufSize;
    }

    /**
     * @param syncKeyBufSize New synchronized key buffer size.
     */
    public void synchronizedKeyBufferSize(int syncKeyBufSize) {
        this.syncKeyBufSize = syncKeyBufSize;
    }

    /**
     * @return Synchronous evicts flag.
     */
    public boolean evictSynchronized() {
        return evictSynchronized;
    }

    /**
     * @param evictSynchronized New synchronous evicts flag.
     */
    public void evictSynchronized(boolean evictSynchronized) {
        this.evictSynchronized = evictSynchronized;
    }

    /**
     * @return Synchronous near evicts flag.
     */
    public boolean nearSynchronized() {
        return nearSynchronized;
    }

    /**
     * @param nearSynchronized New synchronous near evicts flag.
     */
    public void nearSynchronized(boolean nearSynchronized) {
        this.nearSynchronized = nearSynchronized;
    }

    /**
     * @return Eviction max overflow ratio.
     */
    public float maxOverflowRatio() {
        return maxOverflowRatio;
    }

    /**
     * @param maxOverflowRatio New eviction max overflow ratio.
     */
    public void maxOverflowRatio(float maxOverflowRatio) {
        this.maxOverflowRatio = maxOverflowRatio;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheEvictionConfiguration.class, this);
    }
}
