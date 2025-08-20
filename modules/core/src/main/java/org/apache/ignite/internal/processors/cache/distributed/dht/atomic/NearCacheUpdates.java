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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class NearCacheUpdates implements Message {
    /** Indexes of keys for which values were generated on primary node (used if originating node has near cache). */
    @Order(value = 0, method = "nearValuesIndexes")
    private List<Integer> nearValsIdxs;

    /** Indexes of keys for which update was skipped (used if originating node has near cache). */
    @Order(value = 1, method = "skippedIndexes")
    private List<Integer> nearSkipIdxs;

    /** Values generated on primary node which should be put to originating node's near cache. */
    @GridToStringInclude
    @Order(value = 2, method = "nearValues")
    private List<CacheObject> nearVals;

    /** Version generated on primary node to be used for originating node's near cache update. */
    @Order(value = 3, method = "nearVersion")
    private GridCacheVersion nearVer;

    /** Near TTLs. */
    @Order(4)
    private GridLongList nearTtls;

    /** Near expire times. */
    @Order(5)
    private GridLongList nearExpireTimes;

    /**
     * @return Values.
     */
    public List<CacheObject> nearValues() {
        return nearVals;
    }

    /**
     * @param nearVals values.
     */
    public void nearValues(List<CacheObject> nearVals) {
        this.nearVals = nearVals;
    }

    /**
     * @return Near TTLs.
     */
    public GridLongList nearTtls() {
        return nearTtls;
    }

    /**
     * @param nearTtls Near TTLs.
     */
    public void nearTtls(GridLongList nearTtls) {
        this.nearTtls = nearTtls;
    }

    /**
     * @return Near expire times.
     */
    public GridLongList nearExpireTimes() {
        return nearExpireTimes;
    }

    /**
     * @param nearExpireTimes Near expire times.
     */
    public void nearExpireTimes(GridLongList nearExpireTimes) {
        this.nearExpireTimes = nearExpireTimes;
    }

    /**
     * Adds value to be put in near cache on originating node.
     *
     * @param keyIdx Key index.
     * @param val Value.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    void addNearValue(int keyIdx,
        @Nullable CacheObject val,
        long ttl,
        long expireTime) {
        if (nearValsIdxs == null) {
            nearValsIdxs = new ArrayList<>();
            nearVals = new ArrayList<>();
        }

        addNearTtl(keyIdx, ttl, expireTime);

        nearValsIdxs.add(keyIdx);
        nearVals.add(val);
    }

    /**
     * @param keyIdx Key index.
     * @param ttl TTL for near cache update.
     * @param expireTime Expire time for near cache update.
     */
    void addNearTtl(int keyIdx, long ttl, long expireTime) {
        if (ttl >= 0) {
            if (nearTtls == null) {
                nearTtls = new GridLongList(16);

                for (int i = 0; i < keyIdx; i++)
                    nearTtls.add(-1L);
            }
        }

        if (nearTtls != null)
            nearTtls.add(ttl);

        if (expireTime >= 0) {
            if (nearExpireTimes == null) {
                nearExpireTimes = new GridLongList(16);

                for (int i = 0; i < keyIdx; i++)
                    nearExpireTimes.add(-1);
            }
        }

        if (nearExpireTimes != null)
            nearExpireTimes.add(expireTime);
    }

    /**
     * @param idx Index.
     * @return Expire time for near cache update.
     */
    long nearExpireTime(int idx) {
        if (nearExpireTimes != null) {
            assert idx >= 0 && idx < nearExpireTimes.size();

            return nearExpireTimes.get(idx);
        }

        return -1L;
    }

    /**
     * @param idx Index.
     * @return TTL for near cache update.
     */
    long nearTtl(int idx) {
        if (nearTtls != null) {
            assert idx >= 0 && idx < nearTtls.size();

            return nearTtls.get(idx);
        }

        return -1L;
    }

    /**
     * @param nearVer Version generated on primary node to be used for originating node's near cache update.
     */
    public void nearVersion(GridCacheVersion nearVer) {
        this.nearVer = nearVer;
    }

    /**
     * @return Version generated on primary node to be used for originating node's near cache update.
     */
    public GridCacheVersion nearVersion() {
        return nearVer;
    }

    /**
     * @param keyIdx Index of key for which update was skipped
     */
    void addSkippedIndex(int keyIdx) {
        if (nearSkipIdxs == null)
            nearSkipIdxs = new ArrayList<>();

        nearSkipIdxs.add(keyIdx);

        addNearTtl(keyIdx, -1L, -1L);
    }

    /**
     * @return Indexes of keys for which update was skipped
     */
    public @Nullable List<Integer> skippedIndexes() {
        return nearSkipIdxs;
    }

    /**
     * @param nearSkipIdxs Indexes of keys for which update was skipped
     */
    public void skippedIndexes(List<Integer> nearSkipIdxs) {
        this.nearSkipIdxs = nearSkipIdxs;
    }

    /**
     * @return Indexes of keys for which values were generated on primary node.
     */
    public @Nullable List<Integer> nearValuesIndexes() {
        return nearValsIdxs;
    }

    /**
     * @param nearValsIdxs Indexes of keys for which values were generated on primary node.
     */
    public void nearValuesIndexes(List<Integer> nearValsIdxs) {
        this.nearValsIdxs = nearValsIdxs;
    }

    /**
     * @param idx Index.
     * @return Value generated on primary node which should be put to originating node's near cache.
     */
    @Nullable CacheObject nearValue(int idx) {
        return nearVals.get(idx);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -51;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NearCacheUpdates.class, this);
    }
}
