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

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Thrown when an operation is performed on removed entry.
 */
public class GridCacheFilterFailedException extends Exception {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for which filter failed. */
    private final CacheObject val;

    /**
     * Empty constructor.
     */
    public GridCacheFilterFailedException() {
        val = null;
    }

    /**
     * @param val Value for which filter failed.
     */
    public GridCacheFilterFailedException(CacheObject val) {
        this.val = val;
    }

    /**
     * @return Value for failed filter.
     */
    public CacheObject value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheFilterFailedException.class, this);
    }
}