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

import javax.cache.processor.EntryProcessor;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Cache entry atomic update result.
 */
public class GridCacheUpdateAtomicResult {
    /** Success flag.*/
    private final boolean success;

    /** Old value. */
    @GridToStringInclude
    private final CacheObject oldVal;

    /** New value. */
    @GridToStringInclude
    private final CacheObject newVal;

    /** New TTL. */
    private final long newTtl;

    /** Explicit DR expire time (if any). */
    private final long conflictExpireTime;

    /** Version for deferred delete. */
    @GridToStringInclude
    private final GridCacheVersion rmvVer;

    /** DR resolution result. */
    @GridToStringInclude
    private final GridCacheVersionConflictContext<?, ?> conflictRes;

    /** Whether update should be propagated to DHT node. */
    private final boolean sndToDht;

    /** Value computed by entry processor. */
    private IgniteBiTuple<Object, Exception> res;

    /**
     * Constructor.
     *
     * @param success Success flag.
     * @param oldVal Old value.
     * @param newVal New value.
     * @param res Value computed by the {@link EntryProcessor}.
     * @param newTtl New TTL.
     * @param conflictExpireTime Explicit DR expire time (if any).
     * @param rmvVer Version for deferred delete.
     * @param conflictRes DR resolution result.
     * @param sndToDht Whether update should be propagated to DHT node.
     */
    public GridCacheUpdateAtomicResult(boolean success,
        @Nullable CacheObject oldVal,
        @Nullable CacheObject newVal,
        @Nullable IgniteBiTuple<Object, Exception> res,
        long newTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion rmvVer,
        @Nullable GridCacheVersionConflictContext<?, ?> conflictRes,
        boolean sndToDht) {
        this.success = success;
        this.oldVal = oldVal;
        this.newVal = newVal;
        this.res = res;
        this.newTtl = newTtl;
        this.conflictExpireTime = conflictExpireTime;
        this.rmvVer = rmvVer;
        this.conflictRes = conflictRes;
        this.sndToDht = sndToDht;
    }

    /**
     * @return Value computed by the {@link EntryProcessor}.
     */
    @Nullable public IgniteBiTuple<Object, Exception> computedResult() {
        return res;
    }

    /**
     * @return Success flag.
     */
    public boolean success() {
        return success;
    }

    /**
     * @return Old value.
     */
    @Nullable public CacheObject oldValue() {
        return oldVal;
    }

    /**
     * @return New value.
     */
    @Nullable public CacheObject newValue() {
        return newVal;
    }

    /**
     * @return {@link GridCacheUtils#TTL_NOT_CHANGED} if TTL did not change, otherwise new TTL.
     */
    public long newTtl() {
        return newTtl;
    }

    /**
     * @return Explicit conflict expire time (if any). Set only if it is necessary to propagate concrete expire time
     * value to DHT node. Otherwise set to {@link GridCacheUtils#EXPIRE_TIME_CALCULATE}.
     */
    public long conflictExpireTime() {
        return conflictExpireTime;
    }

    /**
     * @return Version for deferred delete.
     */
    @Nullable public GridCacheVersion removeVersion() {
        return rmvVer;
    }

    /**
     * @return DR conflict resolution context.
     */
    @Nullable public GridCacheVersionConflictContext<?, ?> conflictResolveResult() {
        return conflictRes;
    }

    /**
     * @return Whether update should be propagated to DHT node.
     */
    public boolean sendToDht() {
        return sndToDht;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheUpdateAtomicResult.class, this);
    }
}