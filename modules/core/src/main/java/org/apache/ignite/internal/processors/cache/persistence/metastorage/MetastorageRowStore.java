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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeList;

/**
 *
 */
public class MetastorageRowStore {
    /** */
    private final FreeList freeList;

    /** */
    protected final IgniteCacheDatabaseSharedManager db;

    /** */
    public MetastorageRowStore(FreeList freeList, IgniteCacheDatabaseSharedManager db) {
        this.freeList = freeList;
        this.db = db;
    }

    /**
     * @param link Row link.
     * @return Data row.
     */
    public MetastorageDataRow dataRow(String key, long link) throws IgniteCheckedException {
        return ((MetaStorage.FreeListImpl)freeList).readRow(key, link);
    }

    /**
     * @param link Row link.
     * @throws IgniteCheckedException If failed.
     */
    public void removeRow(long link) throws IgniteCheckedException {
        assert link != 0;
        db.checkpointReadLock();

        try {
            freeList.removeDataRowByLink(link);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /**
     * @param row Row.
     * @throws IgniteCheckedException If failed.
     */
    public void addRow(MetastorageDataRow row) throws IgniteCheckedException {
        db.checkpointReadLock();

        try {
            freeList.insertDataRow(row);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /**
     * @param link Row link.
     * @param row New row data.
     * @return {@code True} if was able to update row.
     * @throws IgniteCheckedException If failed.
     */
    public boolean updateRow(long link, MetastorageDataRow row) throws IgniteCheckedException {
        return freeList.updateDataRow(link, row);
    }

    /**
     * @return Free list.
     */
    public FreeList freeList() {
        return freeList;
    }

}
