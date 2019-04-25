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

import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * This exception is used to indicate that requested operation is unavailable in current cache state.
 */
public class CacheInvalidStateException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Create empty exception.
     */
    public CacheInvalidStateException() {
    }

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public CacheInvalidStateException(String msg) {
        super(msg);
    }

    /**
     * Creates new grid exception with given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public CacheInvalidStateException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public CacheInvalidStateException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
