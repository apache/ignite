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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionAware;

/**
 *
 */
public interface CacheSearchRow extends MvccVersionAware {
    /**
     * @return Cache key.
     */
    public KeyCacheObject key();

    /**
     * @return Link for this row.
     */
    public long link();

    /**
     * @return Key hash code.
     */
    public int hash();

    /**
     * @return Cache ID or {@code 0} if cache ID is not defined.
     */
    public int cacheId();
}
