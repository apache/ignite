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

import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 * Allocates page ID's.
 */
public interface PageIdAllocator {
    /**
     * Flag for Data page.
     * Also used by partition meta and tracking pages.
     * This type doesn't use Page ID rotation mechanizm.
     */
    public static final byte FLAG_DATA = 1;

    /**
     * Flag for index page.
     * Also used by internal structure in inmemory caches.
     * This type uses Page ID rotation mechanizm.
     */
    public static final byte FLAG_IDX = 2;

    /**
     * Flag for internal structure page.
     * This type uses Page ID rotation mechanizm.
     */
    public static final byte FLAG_AUX = 4;

    /** Max partition ID that can be used by affinity. */
    public static final int MAX_PARTITION_ID = 65500;

    /** Special partition reserved for index space. */
    public static final int INDEX_PARTITION = 0xFFFF;

    /** Old special partition reserved for metastore space. */
    public static final int OLD_METASTORE_PARTITION = 0x0;

    /** Special partition reserved for metastore space. */
    public static final int METASTORE_PARTITION = 0x1;

    /** Cache group meta page id. */
    public static final long META_PAGE_ID = pageId(INDEX_PARTITION, FLAG_IDX, 0);

    /**
     * Allocates a page from the space for the given partition ID and the given flags.
     *
     * @param grpId Cache Group ID.
     * @param partId Partition ID.
     * @return Allocated page ID.
     */
    public long allocatePage(int grpId, int partId, byte flags) throws IgniteCheckedException;

    /**
     * The given page is free now.
     *
     * @param cacheId Cache Group ID.
     * @param pageId Page ID.
     */
    public boolean freePage(int cacheId, long pageId) throws IgniteCheckedException;
}
