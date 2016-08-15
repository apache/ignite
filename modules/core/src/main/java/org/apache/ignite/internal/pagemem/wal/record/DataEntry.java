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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class DataEntry {
    /** */
    protected int cacheId;

    /** */
    protected KeyCacheObject key;

    /** */
    protected CacheObject val;

    /** */
    protected GridCacheOperation op;

    /** */
    protected GridCacheVersion nearXidVer;

    /** */
    protected GridCacheVersion writeVer;

    /** */
    protected int partId;

    /** */
    protected long partCnt;

    /**
     * @param txEntry Transactional entry.
     * @param tx Transaction.
     * @return Built data entry.
     */
    public static DataEntry fromTxEntry(IgniteTxEntry txEntry, IgniteInternalTx tx) {
        DataEntry de = new DataEntry();

        assert txEntry.key().partition() >= 0 : txEntry.key();

        de.cacheId = txEntry.cacheId();
        de.key = txEntry.key();
        de.val = txEntry.value();
        de.op = txEntry.op();
        de.nearXidVer = tx.nearXidVersion();
        de.writeVer = tx.writeVersion();
        de.partId = txEntry.key().partition();
        de.partCnt = txEntry.updateCounter();

        return de;
    }

    private DataEntry() {
        // No-op, used from factory methods.
    }

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     * @param val Value.
     * @param op Operation.
     * @param nearXidVer Near transaction version.
     * @param writeVer Write version.
     * @param partId Partition ID.
     * @param partCnt Partition counter.
     */
    public DataEntry(
        int cacheId,
        KeyCacheObject key,
        CacheObject val,
        GridCacheOperation op,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer,
        int partId,
        long partCnt
    ) {
        this.cacheId = cacheId;
        this.key = key;
        this.val = val;
        this.op = op;
        this.nearXidVer = nearXidVer;
        this.writeVer = writeVer;
        this.partId = partId;
        this.partCnt = partCnt;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Key cache object.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Value cache object.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @return Cache operation.
     */
    public GridCacheOperation op() {
        return op;
    }

    /**
     * @return Near transaction version if the write was transactional.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Write version.
     */
    public GridCacheVersion writeVersion() {
        return writeVer;
    }

    /**
     * @return Partition ID.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * @return Partition counter.
     */
    public long partitionCounter() {
        return partCnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataEntry.class, this);
    }
}
