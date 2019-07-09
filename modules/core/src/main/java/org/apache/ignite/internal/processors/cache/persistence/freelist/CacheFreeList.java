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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * FreeList implementation for cache.
 */
public class CacheFreeList extends AbstractFreeList<CacheDataRow> {
    /**
     * @param cacheId Cache id.
     * @param name Name.
     * @param regionMetrics Region metrics.
     * @param dataRegion Data region.
     * @param wal Wal.
     * @param metaPageId Meta page id.
     * @param initNew Initialize new.
     */
    public CacheFreeList(
        int cacheId,
        String name,
        DataRegionMetricsImpl regionMetrics,
        DataRegion dataRegion,
        IgniteWriteAheadLogManager wal,
        long metaPageId,
        boolean initNew,
        PageLockListener lockLsnr
    ) throws IgniteCheckedException {
        super(
            cacheId,
            name,
            regionMetrics,
            dataRegion,
            null,
            wal,
            metaPageId,
            initNew,
            lockLsnr
        );
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
