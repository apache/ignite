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

package org.apache.ignite.internal.processors.cache.persistence.evict;

import java.util.LinkedList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;

/**
 * On-heap FIFO page eviction tracker. Only for test purposes.
 */
public class FairFifoPageEvictionTracker extends PageAbstractEvictionTracker {
    /** Page usage deque. */
    private final LinkedList<Integer> pageUsageList = new LinkedList<>();

    /**
     * @param pageMem Page memory.
     * @param plcCfg Data region configuration.
     * @param sharedCtx Shared context.
     */
    public FairFifoPageEvictionTracker(
        PageMemoryNoStoreImpl pageMem,
        DataRegionConfiguration plcCfg,
        GridCacheSharedContext sharedCtx) {
        super(pageMem, plcCfg, sharedCtx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public synchronized void touchPage(long pageId) throws IgniteCheckedException {
        pageUsageList.addLast(PageIdUtils.pageIndex(pageId));
    }

    /** {@inheritDoc} */
    @Override public synchronized void evictDataPage() throws IgniteCheckedException {
        evictDataPage(pageUsageList.pollFirst());
    }

    /** {@inheritDoc} */
    @Override public synchronized void forgetPage(long pageId) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected synchronized boolean checkTouch(long pageId) {
        return true;
    }
}
