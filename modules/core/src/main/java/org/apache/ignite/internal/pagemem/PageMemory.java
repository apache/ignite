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

package org.apache.ignite.internal.pagemem;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 */
public interface PageMemory extends LifecycleAware, PageIdAllocator {
    /**
     * Gets the page associated with the given page ID. Each page obtained with this method must be released by
     * calling {@link #releasePage(Page)}. This method will allocate page with given ID if it doesn't exist.
     *
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return Page.
     */
    public Page page(int cacheId, long pageId) throws IgniteCheckedException;

    /**
     * @see #page(int, long)
     * Will not read page from file if it is not present in memory.
     * TODO this method should be moved to PageMemoryEx altogether will all WAL records.
     *
     * @param cacheId Cache id.
     * @param pageId Page id.
     * @param restore Get page for memory restore
     */
    public Page page(int cacheId, long pageId, boolean restore) throws IgniteCheckedException;

    /**
     * @param page Page to release.
     */
    public void releasePage(Page page) throws IgniteCheckedException;

    /**
     * @return Page size in bytes.
     */
    public int pageSize();

    /**
     * @return Page size with system overhead, in bytes.
     */
    public int systemPageSize();
}
