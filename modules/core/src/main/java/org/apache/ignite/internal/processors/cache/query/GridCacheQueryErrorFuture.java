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

package org.apache.ignite.internal.processors.cache.query;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Error future.
 */
public class GridCacheQueryErrorFuture<T> extends GridFinishedFuture<Collection<T>> implements CacheQueryFuture<T> {
    /**
     * @param ctx Context.
     * @param err Error.
     */
    public GridCacheQueryErrorFuture(GridKernalContext ctx, Throwable err) {
        super(err);
    }

    /** {@inheritDoc} */
    @Override public int available() throws IgniteCheckedException {
        return 0;
    }

    /** {@inheritDoc} */
    @Nullable @Override public T next() throws IgniteCheckedException {
        get();

        return null;
    }
}