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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class DataRow extends CacheDataRowAdapter {
    /** */
    protected int part;

    /** */
    protected int hash;

    /**
     * @param grp Cache group (used to initialize row).
     * @param hash Hash code.
     * @param link Link.
     * @param part Partition.
     * @param rowData Required row data.
     */
    DataRow(CacheGroupContext grp, int hash, long link, int part, RowData rowData) {
        super(link);

        this.hash = hash;

        this.part = part;

        try {
            // We can not init data row lazily because underlying buffer can be concurrently cleared.
            initFromLink(grp, rowData);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        if (key != null)
            key.partition(part);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param part Partition.
     * @param expireTime Expire time.
     * @param cacheId Cache ID.
     */
    public DataRow(KeyCacheObject key, CacheObject val, GridCacheVersion ver, int part, long expireTime, int cacheId) {
        super(0);

        this.hash = key.hashCode();
        this.key = key;
        this.val = val;
        this.ver = ver;
        this.part = part;
        this.expireTime = expireTime;
        this.cacheId = cacheId;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        return hash;
    }

    /** {@inheritDoc} */
    @Override public void link(long link) {
        this.link = link;
    }

    /**
     * @param cacheId Cache ID.
     */
    public void cacheId(int cacheId) {
        this.cacheId = cacheId;
    }
}
