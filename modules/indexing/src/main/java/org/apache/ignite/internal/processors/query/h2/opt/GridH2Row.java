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
    /** Link. */
    private long link;

    /** Key. */
    private KeyCacheObject key;

    /** Value. */
    private CacheObject val;

    /** Version. */
    private GridCacheVersion ver;

    /** Partition. */
    private int partId;

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public void key(KeyCacheObject key) {
        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public CacheObject value() {
        return val;
    }

    /**
     * @param val Value.
     */
    public void value(CacheObject val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @param ver Version.
     */
    public void version(GridCacheVersion ver) {
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /**
     * @param partId Partition.
     */
    public void partition(int partId) {
        this.partId = partId;
    }

    /** {@inheritDoc} */
    @Override public long link() {
        return link;
    }

    /** {@inheritDoc} */
    @Override public void link(long link) {
        this.link = link;
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return 0;
    }
}