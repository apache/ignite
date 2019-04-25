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

package org.apache.ignite;

import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Exception thrown from ignite cache API if cache is restarting.
 */
public class IgniteCacheRestartingException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final transient IgniteFuture<?> restartFut;

    /**
     * @param cacheName Error message.
     */
    public IgniteCacheRestartingException(String cacheName) {
        this(null, cacheName, null);
    }

    /**
     * @param restartFut Restart future.
     * @param cacheName Error message.
     */
    public IgniteCacheRestartingException(IgniteFuture<?> restartFut, String cacheName) {
        this(restartFut, cacheName, null);
    }

    /**
     * @param restartFut Restart future.
     * @param cacheName Cache name what is restarting.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteCacheRestartingException(
        IgniteFuture<?> restartFut,
        String cacheName,
        @Nullable Throwable cause
    ) {
        super("Cache is restarting:" + cacheName + ", you could wait restart completion with restartFuture", cause);

        this.restartFut = restartFut;
    }

    /**
     * @return Future that will be completed when cache is restarted.
     */
    public IgniteFuture<?> restartFuture() {
        return restartFut;
    }
}
