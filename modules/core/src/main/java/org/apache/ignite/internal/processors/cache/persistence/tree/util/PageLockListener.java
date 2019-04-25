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

package org.apache.ignite.internal.processors.cache.persistence.tree.util;

/**
 * Page lock listener.
 */
public interface PageLockListener {
    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     */
    public void onBeforeWriteLock(int cacheId, long pageId, long page);

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     */
    public void onWriteLock(int cacheId, long pageId, long page, long pageAddr);

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     */
    public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr);

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     */
    public void onBeforeReadLock(int cacheId, long pageId, long page);

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     */
    public void onReadLock(int cacheId, long pageId, long page, long pageAddr);

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     */
    public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr);
}
