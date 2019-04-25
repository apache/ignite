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

package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.internal.processors.cache.IgniteInternalCache;

/**
 * Provides callback for marking object as removed.
 */
public interface GridCacheRemovable {
    /**
     * Set status of data structure as removed.
     *
     * @return Current status.
     */
    public boolean onRemoved();

    /**
     *
     */
    public void needCheckNotRemoved();

    /**
     * Would suspend calls for this object.
     */
    public void suspend();

    /**
     * Would return this object work to normal.
     * @param cache To update with.
     */
    public void restart(IgniteInternalCache cache);
}