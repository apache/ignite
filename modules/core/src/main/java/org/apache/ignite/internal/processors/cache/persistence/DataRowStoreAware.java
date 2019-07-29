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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class DataRowStoreAware extends DataRow {
    /** */
    private final boolean storeCacheId;

    /** */
    private final DataRow delegate;

    /**
     *
     * @param delegate
     * @param storeCacheId
     */
    public DataRowStoreAware(DataRow delegate, boolean storeCacheId) {
        this.delegate = delegate;
        this.storeCacheId = storeCacheId;
    }

    /** */
    public DataRow delegate() {
        return delegate;
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return storeCacheId ? delegate.cacheId() : CU.UNDEFINED_CACHE_ID;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        return delegate.key();
    }

    /** {@inheritDoc} */
    @Override public CacheObject value() {
        return delegate.value();
    }

    /** {@inheritDoc} */
    @Override public boolean isReady() {
        return delegate.isReady();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return delegate.version();
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return delegate.expireTime();
    }

    /** {@inheritDoc} */
    @Override public int size() throws IgniteCheckedException {
        int size = delegate.size();

        return storeCacheId || delegate.cacheId == 0 ? size : size - 4;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return delegate.partition();
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        return delegate.hash();
    }

    /** {@inheritDoc} */
    @Override public long link() {
        return delegate.link();
    }

    /** {@inheritDoc} */
    @Override public void key(KeyCacheObject key) {
        delegate.key(key);
    }

    /** {@inheritDoc} */
    @Override public void link(long link) {
        delegate.link(link);
    }

    /** {@inheritDoc} */
    @Override public void cacheId(int cacheId) {
        delegate.cacheId(cacheId);
    }

    /** {@inheritDoc} */
    @Override public IOVersions<? extends AbstractDataPageIO> ioVersions() {
        return delegate.ioVersions();
    }
}
