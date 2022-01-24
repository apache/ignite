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

import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Class responsible for allocating/freeing page IDs.
 */
public interface PageIdAllocator {
    /**
     * Flag for a Data page. Also used by partition meta and tracking pages. This type doesn't use the Page ID rotation mechanism.
     */
    public static final byte FLAG_DATA = 1;

    /**
     * Flag for an internal structure page. This type uses the Page ID rotation mechanism.
     */
    public static final byte FLAG_AUX = 2;

    /**
     * Max partition ID that can be used by affinity.
     */
    // TODO IGNITE-16280 Use constant from the table configuration.
    public static final int MAX_PARTITION_ID = 65500;

    /**
     * Special partition reserved for the index space.
     */
    public static final int INDEX_PARTITION = 0xFFFF;

    /**
     * Allocates a page from the space for the given partition ID and the given flags.
     *
     * @param groupId     Group ID.
     * @param partitionId Partition ID.
     * @return Allocated page ID.
     */
    public long allocatePage(int groupId, int partitionId, byte flags) throws IgniteInternalCheckedException;

    /**
     * Frees the given page.
     *
     * @param groupId Group ID.
     * @param pageId  Page ID.
     */
    public boolean freePage(int groupId, long pageId);
}
