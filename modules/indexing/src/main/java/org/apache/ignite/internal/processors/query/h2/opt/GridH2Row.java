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
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.value.Value;

/**
 * Row with locking support needed for unique key conflicts resolution.
 */
public abstract class GridH2Row implements SearchRow, CacheDataRow, Row {
    /** */
    public long link; // TODO remove

    /** */
    public KeyCacheObject key; // TODO remove

    /** */
    public CacheObject val; // TODO remove

    /** */
    public GridCacheVersion ver; // TODO remove

    /** */
    public int partId; // TODO remove

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

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
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
    @Override public Row getCopy() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setVersion(int version) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getByteCount(Data dummy) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setDeleted(boolean deleted) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setSessionId(int sessionId) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getSessionId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void commit() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isDeleted() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setKeyAndVersion(SearchRow old) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getVersion() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void setKey(long key) {
        // No-op, may be set in H2 INFORMATION_SCHEMA.
    }

    /** {@inheritDoc} */
    @Override public long getKey() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getMemory() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Value[] getValueList() {
        throw new UnsupportedOperationException();
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