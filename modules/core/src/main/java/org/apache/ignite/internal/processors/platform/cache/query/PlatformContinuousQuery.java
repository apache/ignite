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

package org.apache.ignite.internal.processors.platform.cache.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.platform.PlatformTarget;

import javax.cache.event.CacheEntryUpdatedListener;

/**
 * Platform continuous query.
 */
public interface PlatformContinuousQuery extends CacheEntryUpdatedListener, PlatformContinuousQueryFilter {
    /**
     * Start continuous query execution.
     *
     * @param cache Cache.
     * @param loc Local flag.
     * @param bufSize Buffer size.
     * @param timeInterval Time interval.
     * @param autoUnsubscribe Auto-unsubscribe flag.
     * @param initialQry Initial query.
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    public void start(IgniteCacheProxy cache, boolean loc, int bufSize, long timeInterval, boolean autoUnsubscribe,
        Query initialQry) throws IgniteCheckedException;

    /**
     * Close continuous query.
     */
    public void close();

    /**
     * Gets initial query cursor (if any).
     *
     * @return Initial query cursor.
     */
    public PlatformTarget getInitialQueryCursor();
}