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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.stat.IoStatisticsHolder;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * FreeList implementation for cache.
 */
public class CacheFreeListImpl extends AbstractFreeList<CacheDataRow> {
    /**
     * @param cacheId Cache id.
     * @param name Name.
     * @param regionMetrics Region metrics.
     * @param dataRegion Data region.
     * @param reuseList Reuse list.
     * @param wal Wal.
     * @param metaPageId Meta page id.
     * @param initNew Initialize new.
     */
    public CacheFreeListImpl(int cacheId, String name, DataRegionMetricsImpl regionMetrics, DataRegion dataRegion,
        ReuseList reuseList,
        IgniteWriteAheadLogManager wal, long metaPageId, boolean initNew) throws IgniteCheckedException {
        super(cacheId, name, regionMetrics, dataRegion, reuseList, wal, metaPageId, initNew);
    }

    /** {@inheritDoc} */
    @Override public IOVersions<? extends AbstractDataPageIO<CacheDataRow>> ioVersions() {
        return DataPageIO.VERSIONS;
    }

    /** {@inheritDoc} */
    @Override public void insertDataRow(CacheDataRow row, IoStatisticsHolder statHolder) throws IgniteCheckedException {
        super.insertDataRow(row, statHolder);

        assert row.key().partition() == PageIdUtils.partId(row.link()) :
            "Constructed a link with invalid partition ID [partId=" + row.key().partition() +
            ", link=" + U.hexLong(row.link()) + ']';
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "FreeList [name=" + name + ']';
    }
}
