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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.partstorage.PartitionMetaStorage;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;

/**
 *
 */
public class MetastorageRowStore {
    /** */
    private final PartitionMetaStorage<MetastorageDataRow> partStorage;

    /** */
    protected final IgniteCacheDatabaseSharedManager db;

    /** */
    public MetastorageRowStore(PartitionMetaStorage<MetastorageDataRow> partStorage, IgniteCacheDatabaseSharedManager db) {
        this.partStorage = partStorage;
        this.db = db;
    }

    /**
     * @param link Row link.
     * @return Data row.
     */
    public MetastorageDataRow dataRow(String key, long link) throws IgniteCheckedException {
        return new MetastorageDataRow(link, key, partStorage.readRow(link));
    }

    /**
     * @param link Row link.
     * @throws IgniteCheckedException If failed.
     */
    public void removeRow(long link) throws IgniteCheckedException {
        assert link != 0;
        db.checkpointReadLock();

        try {
            partStorage.removeDataRowByLink(link, IoStatisticsHolderNoOp.INSTANCE);
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
            partStorage.insertDataRow(row, IoStatisticsHolderNoOp.INSTANCE);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }
}
