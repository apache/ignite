/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridCacheCompoundFuture<T, R> extends GridCompoundFuture<T, R> implements GridCacheFuture<R> {
    /** Future start time. */
    private final long startTime = U.currentTimeMillis();

    /** Future end time. */
    private volatile long endTime;

    /**
     * @param rdc Reducer.
     */
    protected GridCacheCompoundFuture(@Nullable IgniteReducer<T, R> rdc) {
        super(rdc);
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public long duration() {
        long endTime = this.endTime;

        return (endTime == 0 ? U.currentTimeMillis() : endTime) - startTime;
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable R res, @Nullable Throwable err, boolean cancel) {
        if(super.onDone(res, err, cancel)){
            endTime = U.currentTimeMillis();
            return true;
        }

        return false;
    }
}
