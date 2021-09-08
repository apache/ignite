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

import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridCacheCompoundIdentityFuture<T> extends GridCompoundIdentityFuture<T> implements GridCacheFuture<T> {
    /** Future start time. */
    private final long startTime = U.currentTimeMillis();

    /** Future end time. */
    private volatile long endTime;

    /**
     * @param rdc Reducer.
     */
    protected GridCacheCompoundIdentityFuture(@Nullable IgniteReducer<T, T> rdc) {
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
    @Override protected boolean onDone(@Nullable T res, @Nullable Throwable err, boolean cancel) {
        if (super.onDone(res, err, cancel)) {
            endTime = U.currentTimeMillis();
            return true;
        }

        return false;
    }
}
