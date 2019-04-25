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

import java.io.Serializable;

/**
 * Key for system utility cache.
 */
public abstract class GridCacheUtilityKey<K extends GridCacheUtilityKey> implements GridCacheInternal, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public final boolean equals(Object obj) {
        return obj == this || obj != null && obj.getClass() == getClass() && equalsx((K)obj);
    }

    /**
     * Child-specific equals method.
     *
     * @param key Key.
     * @return {@code True} if equals.
     */
    protected abstract boolean equalsx(K key);

    /** {@inheritDoc} */
    @Override public abstract int hashCode();
}
