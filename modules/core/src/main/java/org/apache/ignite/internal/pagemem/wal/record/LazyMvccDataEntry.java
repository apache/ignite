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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;

/**
 * Represents Data Entry ({@link #key}, {@link #val value}) pair update {@link #op operation}. <br>
 * This Data entry was not converted to key, value pair during record deserialization.
 */
public class LazyMvccDataEntry extends MvccDataEntry implements MarshalledDataEntry {
    /** */
    private GridCacheSharedContext cctx;

    /** Data Entry key type code. See {@link CacheObject} for built-in value type codes */
    private byte keyType;

    /** Key value bytes. */
    private byte[] keyBytes;

    /** Data Entry Value type code. See {@link CacheObject} for built-in value type codes */
    private byte valType;

    /** Value value bytes. */
    private byte[] valBytes;

    /**
     * @param cctx Shared context.
     * @param cacheId Cache ID.
     * @param keyType Object type code for Key.
     * @param keyBytes Data Entry Key value bytes.
     * @param valType Object type code for Value.
     * @param valBytes Data Entry Value value bytes.
     * @param op Operation.
     * @param nearXidVer Near transaction version.
     * @param writeVer Write version.
     * @param expireTime Expire time.
     * @param partId Partition ID.
     * @param partCnt Partition counter.
     * @param mvccVer Mvcc version.
     */
    public LazyMvccDataEntry(
        GridCacheSharedContext cctx,
        int cacheId,
        byte keyType,
        byte[] keyBytes,
        byte valType,
        byte[] valBytes,
        GridCacheOperation op,
        GridCacheVersion nearXidVer,
        GridCacheVersion writeVer,
        long expireTime,
        int partId,
        long partCnt,
        MvccVersion mvccVer
    ) {
        super(cacheId, null, null, op, nearXidVer, writeVer, expireTime, partId, partCnt, mvccVer);

        this.cctx = cctx;
        this.keyType = keyType;
        this.keyBytes = keyBytes;
        this.valType = valType;
        this.valBytes = valBytes;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        try {
            if (key == null) {
                GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                if (cacheCtx == null)
                    throw new IgniteException("Failed to find cache context for the given cache ID: " + cacheId);

                IgniteCacheObjectProcessor co = cctx.kernalContext().cacheObjects();

                key = co.toKeyCacheObject(cacheCtx.cacheObjectContext(), keyType, keyBytes);

                if (key.partition() == -1)
                    key.partition(partId);
            }

            return key;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheObject value() {
        if (val == null && valBytes != null) {
            GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

            if (cacheCtx == null)
                throw new IgniteException("Failed to find cache context for the given cache ID: " + cacheId);

            IgniteCacheObjectProcessor co = cctx.kernalContext().cacheObjects();

            val = co.toCacheObject(cacheCtx.cacheObjectContext(), valType, valBytes);
        }

        return val;
    }

    /** {@inheritDoc} */
    @Override public byte getKeyType() {
        return keyType;
    }

    /** {@inheritDoc} */
    @Override public byte[] getKeyBytes() {
        return keyBytes;
    }

    /** {@inheritDoc} */
    @Override public byte getValType() {
        return valType;
    }

    /** {@inheritDoc} */
    @Override public byte[] getValBytes() {
        return valBytes;
    }
}
