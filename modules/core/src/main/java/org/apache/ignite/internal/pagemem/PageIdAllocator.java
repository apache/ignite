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

package org.apache.ignite.internal.pagemem;

import org.apache.ignite.IgniteCheckedException;

/**
 * Allocates page ID's.
 */
public interface PageIdAllocator {
    /** */
    public static final byte FLAG_DATA = 1;

    /** */
    public static final byte FLAG_IDX = 2;

    /** Max partition ID that can be used by affinity. */
    public static final int MAX_PARTITION_ID = 65500;

    /** Special partition reserved for index space. */
    public static final int INDEX_PARTITION = 0xFFFF;

    /** Old special partition reserved for metastore space. */
    public static final int OLD_METASTORE_PARTITION = 0x0;

    /** Special partition reserved for metastore space. */
    public static final int METASTORE_PARTITION = 0x1;

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
