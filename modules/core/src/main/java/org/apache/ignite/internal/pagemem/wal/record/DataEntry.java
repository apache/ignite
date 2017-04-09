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
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

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
    @GridToStringInclude
    protected GridCacheOperation op;

    /** */
    protected GridCacheVersion nearXidVer;

    /** */
    @GridToStringInclude
    protected GridCacheVersion writeVer;

    /** */
    protected long expireTime;

    /** */
    @GridToStringInclude
    protected int partId;

    /** */
    @GridToStringInclude
    protected long partCnt;

    /** Class loader for the key. If peer-classloading disabled then {@code null}, otherwise classloader UUID. */
    protected IgniteUuid keyClsLdrId;

    /** Class loader for the val. If peer-classloading disabled then {@code null}, otherwise classloader UUID. */
    protected IgniteUuid valClsLdrId;

    /** P2P flag. */
    private transient boolean p2pEnabled;

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
     * @param expireTime Expire time.
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
        long expireTime,
        int partId,
        long partCnt,
        boolean p2pEnabled,
        IgniteUuid keyClsLdrId,
        IgniteUuid valClsLdrId
    ) {
        this.cacheId = cacheId;
        this.key = key;
        this.val = val;
        this.op = op;
        this.nearXidVer = nearXidVer;
        this.writeVer = writeVer;
        this.expireTime = expireTime;
        this.partId = partId;
        this.partCnt = partCnt;
        this.p2pEnabled = p2pEnabled;
        this.keyClsLdrId = keyClsLdrId;
        this.valClsLdrId = valClsLdrId;

        // Only CREATE, UPDATE and DELETE operations should be stored in WAL.
        assert op == GridCacheOperation.CREATE || op == GridCacheOperation.UPDATE || op == GridCacheOperation.DELETE : op;
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

    /**
     * @return Expire time.
     */
    public long expireTime() {
        return expireTime;
    }

    /**
     * @return Key class loader ID.
     */
    public IgniteUuid keyClassLoaderId() {
        return keyClsLdrId;
    }

    /**
     * @return Value class loader ID.
     */
    public IgniteUuid valueClassLoaderId() {
        return valClsLdrId;
    }

    /**
     * @return P2P enabled flag.
     */
    public boolean p2pEnabled() {
        return p2pEnabled;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataEntry.class, this);
    }
}
