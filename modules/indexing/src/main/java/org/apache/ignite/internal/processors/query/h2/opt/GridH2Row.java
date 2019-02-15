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
    @Override public boolean indexSearchRow() {
        return false;
    }
}