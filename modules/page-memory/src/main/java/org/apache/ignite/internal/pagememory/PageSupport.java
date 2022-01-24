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

package org.apache.ignite.internal.pagememory;

import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Class responsible for acquiring/releasing and locking/unlocking pages.
 */
//TODO IGNITE-16350 Document a naming convention for "page" and "pageAddr" parameters.
public interface PageSupport {
    /**
     * Returns an absolute pointer to a page, associated with the given page ID. Each pointer obtained with this method must be released by
     * calling {@link #releasePage(int, long, long)}. This method will allocate a page with the given ID if it doesn't exist.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @return Page pointer.
     * @throws IgniteInternalCheckedException If failed.
     */
    long acquirePage(int groupId, long pageId) throws IgniteInternalCheckedException;

    /**
     * Returns an absolute pointer to a page, associated with the given page ID. Each page obtained with this method must be released by
     * calling {@link #releasePage(int, long, long)}. This method will allocate a page with the given ID if it doesn't exist.
     *
     * @param groupId    Group ID.
     * @param pageId     Page ID.
     * @param statHolder Statistics holder to track IO operations.
     * @return Page pointer.
     * @throws IgniteInternalCheckedException If failed.
     */
    long acquirePage(int groupId, long pageId, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException;

    /**
     * Releases pages acquired by any of the {@code acquirePage} methods.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID to release.
     * @param page    Page pointer returned by the corresponding {@code acquirePage} call.
     */
    void releasePage(int groupId, long pageId, long page);

    /**
     * Acquires a read lock associated with the given page.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @return Pointer for reading the page or {@code 0} if page has been reused.
     */
    long readLock(int groupId, long pageId, long page);

    /**
     * Acquires a read lock, associated with a given page, without checking the page tag.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @return Pointer for reading the page.
     */
    long readLockForce(int groupId, long pageId, long page);

    /**
     * Releases a read lock, associated with a given page.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     */
    void readUnlock(int groupId, long pageId, long page);

    /**
     * Acquired a write lock on the page.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @return Address of a buffer with contents of the given page or {@code 0L} if attempt to take the write lock failed.
     */
    long writeLock(int groupId, long pageId, long page);

    /**
     * Tries to acquire a write lock on the page.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @return Address of a buffer with contents of the given page or {@code 0L} if attempt to take the write lock failed.
     */
    long tryWriteLock(int groupId, long pageId, long page);

    /**
     * Releases locked page.
     *
     * @param groupId   Group ID.
     * @param pageId    Page ID.
     * @param page      Page pointer.
     * @param dirtyFlag Determines whether the page was modified since the last checkpoint.
     */
    void writeUnlock(int groupId, long pageId, long page, boolean dirtyFlag);

    /**
     * Checks whether the page is dirty.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     * @param page    Page pointer.
     * @return {@code True} if the page is dirty.
     */
    boolean isDirty(int groupId, long pageId, long page);
}
