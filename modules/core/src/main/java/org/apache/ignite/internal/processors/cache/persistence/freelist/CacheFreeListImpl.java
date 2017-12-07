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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;

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
    @Override public String toString() {
        return "FreeList [name=" + name + ']';
    }
}
