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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Implementation of public API future for cache.
 */
public class IgniteCacheFutureImpl<V> extends IgniteFutureImpl<V> {
    /**
     * Constructor.
     *
     * @param fut Internal future.
     */
    public IgniteCacheFutureImpl(IgniteInternalFuture<V> fut) {
        super(fut);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteFuture<T> chain(IgniteClosure<? super IgniteFuture<V>, T> doneCb) {
        return new IgniteCacheFutureImpl<>(chainInternal(doneCb));
    }

    /** {@inheritDoc} */
    @Override protected RuntimeException convertException(IgniteCheckedException e) {
        if (e instanceof IgniteFutureCancelledCheckedException ||
            e instanceof IgniteInterruptedCheckedException ||
            e instanceof IgniteFutureTimeoutCheckedException)
            return U.convertException(e);

        return CU.convertToCacheException(e);
    }
}