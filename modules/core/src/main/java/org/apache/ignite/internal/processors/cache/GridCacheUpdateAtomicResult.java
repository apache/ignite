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
    /** Update operation outcome. */
    private final UpdateOutcome outcome;

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

    /** */
    private final long updateCntr;

    /** Flag indicating whether value is transformed. */
    private boolean transformed;

    /** Value computed by entry processor. */
    private IgniteBiTuple<Object, Exception> res;

    /**
     * Constructor.
     *
     * @param outcome Update outcome.
     * @param oldVal Old value.
     * @param newVal New value.
     * @param res Value computed by the {@link EntryProcessor}.
     * @param newTtl New TTL.
     * @param conflictExpireTime Explicit DR expire time (if any).
     * @param rmvVer Version for deferred delete.
     * @param conflictRes DR resolution result.
     * @param updateCntr Partition update counter.
     * @param transformed {@code True} if result was transformed.
     */
    GridCacheUpdateAtomicResult(UpdateOutcome outcome,
        @Nullable CacheObject oldVal,
        @Nullable CacheObject newVal,
        @Nullable IgniteBiTuple<Object, Exception> res,
        long newTtl,
        long conflictExpireTime,
        @Nullable GridCacheVersion rmvVer,
        @Nullable GridCacheVersionConflictContext<?, ?> conflictRes,
        long updateCntr,
        boolean transformed) {
        assert outcome != null;

        this.outcome = outcome;
        this.oldVal = oldVal;
        this.newVal = newVal;
        this.res = res;
        this.newTtl = newTtl;
        this.conflictExpireTime = conflictExpireTime;
        this.rmvVer = rmvVer;
        this.conflictRes = conflictRes;
        this.updateCntr = updateCntr;
        this.transformed = transformed;
    }

    /**
     * @return Update operation outcome.
     */
    UpdateOutcome outcome() {
        return outcome;
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
        return outcome.success();
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
     * @return Partition update index.
     */
    public long updateCounter() {
        return updateCntr;
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
        return outcome.sendToDht();
    }

    /**
     *
     */
    public enum UpdateOutcome {
        /** */
        CONFLICT_USE_OLD(false, false, false),

        /** */
        VERSION_CHECK_FAILED(false, false, false),

        /** */
        FILTER_FAILED(false, false, true),

        /** */
        INVOKE_NO_OP(false, false, true),

        /** */
        INTERCEPTOR_CANCEL(false, false, true),

        /** */
        REMOVE_NO_VAL(false, true, true),

        /** */
        SUCCESS(true, true, true);

        /** */
        private final boolean success;

        /** */
        private final boolean sndToDht;

        /** */
        private final boolean updateReadMetrics;

        /**
         * @param success Success flag.
         * @param sndToDht Whether update should be propagated to DHT node.
         * @param updateReadMetrics Metrics update flag.
         */
        UpdateOutcome(boolean success, boolean sndToDht, boolean updateReadMetrics) {
            this.success = success;
            this.sndToDht = sndToDht;
            this.updateReadMetrics = updateReadMetrics;
        }

        /**
         * @return Success flag.
         */
        public boolean success() {
            return success;
        }

        /**
         * @return Whether update should be propagated to DHT node.
         */
        public boolean sendToDht() {
            return sndToDht;
        }

        /**
         * @return Metrics update flag.
         */
        public boolean updateReadMetrics() {
            return updateReadMetrics;
        }
    }

    /**
     * @return {@code True} if transformed.
     */
    public boolean transformed() {
        return transformed;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheUpdateAtomicResult.class, this);
    }
}
