/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence;

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
}
