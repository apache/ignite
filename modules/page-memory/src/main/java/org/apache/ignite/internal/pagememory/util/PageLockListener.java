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

package org.apache.ignite.internal.pagememory.util;

/**
 * Page lock listener.
 */
//TODO IGNITE-16350 Consider froper Before/After naming convention for all methods in this class.
public interface PageLockListener extends AutoCloseable {
    /**
     * Callback that's called before write lock acquiring.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     */
    public void onBeforeWriteLock(int groupId, long pageId, long page);

    /**
     * Callback that's called after lock acquiring.
     *
     * @param groupId  Group ID.
     * @param pageId   Page ID.
     * @param page     Page pointer.
     * @param pageAddr Page address.
     */
    public void onWriteLock(int groupId, long pageId, long page, long pageAddr);

    /**
     * Callback that's called before write lock releasing.
     *
     * @param groupId  Group ID.
     * @param pageId   Page ID.
     * @param page     Page pointer.
     * @param pageAddr Page address.
     */
    public void onWriteUnlock(int groupId, long pageId, long page, long pageAddr);

    /**
     * Callback that's called before read lock acquiring.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     */
    public void onBeforeReadLock(int groupId, long pageId, long page);

    /**
     * Callback that's called after read lock acquiring.
     *
     * @param groupId  Group ID.
     * @param pageId   Page ID.
     * @param page     Page pointer.
     * @param pageAddr Page address.
     */
    public void onReadLock(int groupId, long pageId, long page, long pageAddr);

    /**
     * Callback that's called before read lock releasing.
     *
     * @param groupId  Group ID.
     * @param pageId   Page ID.
     * @param page     Page pointer.
     * @param pageAddr Page address.
     */
    public void onReadUnlock(int groupId, long pageId, long page, long pageAddr);

    /** {@inheritDoc} */
    @Override
    public void close();
}
