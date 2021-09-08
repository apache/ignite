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

package org.apache.ignite.internal.processors.cache.persistence.tree.reuse;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.jetbrains.annotations.Nullable;

/**
 * Reuse list.
 */
public interface ReuseList {
    /**
     * @param bag Reuse bag.
     * @throws IgniteCheckedException If failed.
     */
    public void addForRecycle(ReuseBag bag) throws IgniteCheckedException;

    /**
     * @return Page ID or {@code 0} if none available.
     * @throws IgniteCheckedException If failed.
     */
    public long takeRecycledPage() throws IgniteCheckedException;

    /**
     * @return Number of recycled pages it contains.
     * @throws IgniteCheckedException If failed.
     */
    public long recycledPagesCount() throws IgniteCheckedException;

    /**
     * Converts recycled page id back to a usable id. Might modify page content as well if flag is changing.
     *
     * @param pageId Id of the recycled page.
     * @param flag Flag value for the page. One of {@link PageIdAllocator#FLAG_DATA}, {@link PageIdAllocator#FLAG_IDX}
     *      or {@link PageIdAllocator#FLAG_AUX}.
     * @param initIO Page IO to reinit reused page.
     * @return Updated page id.
     * @throws IgniteCheckedException If failed.
     *
     * @see FullPageId
     */
    long initRecycledPage(long pageId, byte flag, @Nullable PageIO initIO) throws IgniteCheckedException;
}
