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

import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_CRD_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_NA;

/**
 *
 */
public class SearchRow implements CacheSearchRow {
    /** */
    private final KeyCacheObject key;

    /** */
    private final int hash;

    /** */
    private final int cacheId;

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     */
    public SearchRow(int cacheId, KeyCacheObject key) {
        this.key = key;
        this.hash = key.hashCode();
        this.cacheId = cacheId;
    }

    /**
     * Instantiates a new fake search row as a logic cache based bound.
     *
     * @param cacheId Cache ID.
     */
    public SearchRow(int cacheId) {
        this.key = null;
        this.hash = 0;
        this.cacheId = cacheId;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public long link() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int hash() {
        return hash;
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return cacheId;
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return MVCC_CRD_COUNTER_NA;
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return MVCC_COUNTER_NA;
    }

    /** {@inheritDoc} */
    @Override public int mvccOperationCounter() {
        return MVCC_OP_COUNTER_NA;
    }

    /** {@inheritDoc} */
    @Override public byte mvccTxState() {
        return TxState.NA;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SearchRow.class, this);
    }
}
