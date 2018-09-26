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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Row with locking support needed for unique key conflicts resolution.
 */
public abstract class GridH2Row extends GridH2SearchRowAdapter implements CacheDataRow {
    /** Row. */
    protected final CacheDataRow row;

    /**
     * @param row Row.
     */
    GridH2Row(CacheDataRow row) {
        this.row = row;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        return row.key();
    }

    /** {@inheritDoc} */
    @Override public void key(KeyCacheObject key) {
        row.key(key);
    }

    /** {@inheritDoc} */
    @Override public CacheObject value() {
        return row.value();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return row.version();
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return row.partition();
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return row.expireTime();
    }

    /** {@inheritDoc} */
    @Override public long link() {
        return row.link();
    }

    /** {@inheritDoc} */
    @Override public void link(long link) {
        row.link(link);
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        return row.hash();
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return row.cacheId();
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return row.mvccCoordinatorVersion();
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return row.mvccCounter();
    }

    /** {@inheritDoc} */
    @Override public int mvccOperationCounter() {
        return row.mvccOperationCounter();
    }

    /** {@inheritDoc} */
    public byte mvccTxState() {
        return row.mvccTxState();
    }

    /** {@inheritDoc} */
    @Override public long newMvccCoordinatorVersion() {
        return row.newMvccCoordinatorVersion();
    }

    /** {@inheritDoc} */
    @Override public long newMvccCounter() {
        return row.newMvccCounter();
    }

    /** {@inheritDoc} */
    @Override public int newMvccOperationCounter() {
        return row.newMvccOperationCounter();
    }

    /** {@inheritDoc} */
    @Override public byte newMvccTxState() {
        return row.newMvccTxState();
    }

    /** {@inheritDoc} */
    @Override public boolean isKeyAbsentBefore() {
        return row.isKeyAbsentBefore();
    }

    /** {@inheritDoc} */
    @Override public boolean indexSearchRow() {
        return false;
    }
}