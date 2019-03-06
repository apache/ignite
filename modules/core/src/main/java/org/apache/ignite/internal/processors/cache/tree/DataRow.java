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
     * @param skipVer Whether version read should be skipped.
     */
    protected DataRow(CacheGroupContext grp, int hash, long link, int part, RowData rowData, boolean skipVer) {
        super(link);

        this.hash = hash;
        this.part = part;

        try {
            // We can not init data row lazily outside of entry lock because underlying buffer can be concurrently cleared.
            if (rowData != RowData.LINK_ONLY)
                initFromLink(grp, rowData, skipVer);
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

        verReady = true;
    }

    /**
     * @param link Link.
     */
    protected DataRow(long link) {
        super(link);
    }

    /**
     *
     */
    public DataRow() {
        super(0);
    }

    /** {@inheritDoc} */
    @Override public void key(KeyCacheObject key) {
        super.key(key);

        hash = key.hashCode();
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
