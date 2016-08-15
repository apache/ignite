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

package org.apache.ignite.internal.processors.cache.database;

import org.apache.ignite.IgniteCheckedException;

/**
 * Meta store.
 */
public interface MetaStore {
    /**
     * Get or allocate initial page for an index.
     *
     * @param idxName Index name.
     * @return {@link RootPage} that keeps pageId, allocated flag that shows whether the page
     *      was newly allocated, and rootId that is counter which increments each time new page allocated.
     * @throws IgniteCheckedException
     */
    public RootPage getOrAllocateForTree(String idxName) throws IgniteCheckedException;

    /**
     * Deallocate index page and remove from tree.
     *
     * @param idxName Index name.
     * @return Root ID or -1 if no page was removed.
     * @throws IgniteCheckedException
     */
    public RootPage dropRootPage(String idxName) throws IgniteCheckedException;

    /**
     * Destroy this meta store.
     *
     * @throws IgniteCheckedException
     */
    public void destroy() throws IgniteCheckedException;
}
