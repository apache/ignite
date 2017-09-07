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

package org.apache.ignite.internal.processors.cache.tree;

import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUpdateVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class MvccDataRow extends DataRow {
    /** */
    private long mvccTopVer;

    /** */
    private long mvccCntr;

    /**
     * @param grp
     * @param hash
     * @param link
     * @param part
     * @param rowData
     * @param mvccTopVer
     * @param mvccCntr
     */
    public MvccDataRow(CacheGroupContext grp, int hash, long link, int part, RowData rowData, long mvccTopVer, long mvccCntr) {
        super(grp, hash, link, part, rowData);

        assert mvccTopVer > 0 : mvccTopVer;
        assert mvccCntr != MvccUpdateVersion.COUNTER_NA;

        this.mvccTopVer = mvccTopVer;
        this.mvccCntr = mvccCntr;
    }

    /**
     * @param key
     * @param val
     * @param ver
     * @param part
     * @param cacheId
     */
    public MvccDataRow(KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        int part,
        int cacheId,
        long mvccTopVer,
        long mvccCntr) {
        super(key, val, ver, part, 0L, cacheId);

        this.mvccCntr = mvccCntr;
        this.mvccTopVer = mvccTopVer;
    }

    /** {@inheritDoc} */
    @Override public long mvccUpdateTopologyVersion() {
        return mvccTopVer;
    }

    /** {@inheritDoc} */
    @Override public long mvccUpdateCounter() {
        return mvccCntr;
    }
}
