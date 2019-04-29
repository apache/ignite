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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;

/**
 * Meta store.
 */
public interface IndexStorage {
    /**
     * Allocate page for cache index. Index name will be masked if needed.
     *
     * @param cacheId Cache ID.
     * @param idxName Index name.
     * @param segment Segment.
     * @return Root page.
     * @throws IgniteCheckedException If failed.
     */
    public RootPage allocateCacheIndex(Integer cacheId, String idxName, int segment) throws IgniteCheckedException;

    /**
     * Get or allocate initial page for an index.
     *
     * @param idxName Index name.
     * @return {@link RootPage} that keeps pageId, allocated flag that shows whether the page
     *      was newly allocated, and rootId that is counter which increments each time new page allocated.
     * @throws IgniteCheckedException If failed.
     */
    public RootPage allocateIndex(String idxName) throws IgniteCheckedException;

    /**
     * Deallocate index page and remove from tree.
     *
     * @param cacheId Cache ID.
     * @param idxName Index name.
     * @param segment Segment.
     * @return Root ID or -1 if no page was removed.
     * @throws IgniteCheckedException  If failed.
     */
    public RootPage dropCacheIndex(Integer cacheId, String idxName, int segment) throws IgniteCheckedException;

    /**
     * Deallocate index page and remove from tree.
     *
     * @param idxName Index name.
     * @return Root ID or -1 if no page was removed.
     * @throws IgniteCheckedException  If failed.
     */
    public RootPage dropIndex(String idxName) throws IgniteCheckedException;

    /**
     * Destroy this meta store.
     *
     * @throws IgniteCheckedException  If failed.
     */
    public void destroy() throws IgniteCheckedException;

    /**
     * @return Index names of all indexes which this storage keeps.
     *
     * @throws IgniteCheckedException  If failed.
     */
    public Collection<String> getIndexNames() throws IgniteCheckedException;

    /**
     *
     * @param idxName Index name to check.
     * @param cacheId Cache id to check.
     * @return True if the given idxName could be assosiated with the given cacheId (existing is not checked).
     */
    boolean nameIsAssosiatedWithCache(String idxName, int cacheId);
}
