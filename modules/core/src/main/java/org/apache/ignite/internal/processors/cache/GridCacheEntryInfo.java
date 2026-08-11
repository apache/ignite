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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.CacheIdAware;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Entry information that gets passed over wire.
 */
public class GridCacheEntryInfo implements CacheIdAware, Message {
    /** */
    private static final int SIZE_OVERHEAD = 3 * 8 /* reference */ + 4 /* int */ + 2 * 8 /* long */ + 32 /* version */;

    /** Cache key. */
    @Order(0)
    @GridToStringInclude
    @Nullable KeyCacheObject key;

    /** Cache ID. */
    @Order(1)
    int cacheId;

    /** Cache value. */
    @Order(2)
    @Nullable CacheObject val;

    /** Time to live. */
    @Order(3)
    long ttl;

    /** Base time to calculate {@link #expireTime()}. */
    private long initTime;

    /** Expiration time delta to transfer. -1 if no expiration is used. */
    @Order(4)
    long expireTimeDelta = -1L;

    /** Entry version. */
    @Order(5)
    GridCacheVersion ver;

    /** New flag. */
    private boolean isNew;

    /** Deleted flag. */
    private boolean deleted;

    /**
     * Empty constructor for serialization purposes. Initializes {@link #initTime} to properly calculate {@link #expireTime()}
     * if {@link #expireTimeDelta} and the expiration is effective. If no expiration is set, initialization of {@link #initTime}
     * is not required, but it is a tradeoff for absence of the message serealization awareness.
     */
    public GridCacheEntryInfo() {
        initTime = U.currentTimeMillis();
    }

    /** */
    public GridCacheEntryInfo(
        int cacheId,
        KeyCacheObject key,
        @Nullable CacheObject val,
        GridCacheVersion ver,
        long initTime,
        long expireTime,
        long ttl
    ) {
        assert expireTime >= 0;
        assert initTime > 0;

        this.initTime = initTime;

        expireTimeDelta = expireTime - initTime;

        // Timeouted mark.
        if (expireTimeDelta < 0)
            expireTimeDelta = 0;

        this.cacheId = cacheId;
        this.key = key;
        this.val = val;
        this.ver = ver;
        this.ttl = ttl;
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return cacheId;
    }

    /**
     * @param key Entry key.
     */
    public void key(@Nullable KeyCacheObject key) {
        this.key = key;
    }

    /**
     * @return Entry key.
     */
    @Nullable public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Entry value.
     */
    public @Nullable CacheObject value() {
        return val;
    }

    /**
     * @return Expire time >= 0. 0 means no expiration is set.
     */
    public long expireTime() {
        if (expireTimeDelta == -1L)
            return 0L;

        long res = initTime + expireTimeDelta;

        // Overflow protection.
        return res < 0 ? 0 : res;
    }

    /**
     * @return Time to live.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return New flag.
     */
    public boolean isNew() {
        return isNew;
    }

    /**
     * @param isNew New flag.
     */
    public void setNew(boolean isNew) {
        this.isNew = isNew;
    }

    /**
     * @return {@code True} if deleted.
     */
    public boolean isDeleted() {
        return deleted;
    }

    /**
     * @param deleted Deleted flag.
     */
    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    /**
     * @param ctx Cache object context.
     * @return Marshalled size.
     * @throws IgniteCheckedException If failed.
     */
    public int marshalledSize(CacheObjectContext ctx) throws IgniteCheckedException {
        int size = 0;

        if (val != null)
            size += val.valueBytes(ctx).length;

        size += key.valueBytes(ctx).length;

        return SIZE_OVERHEAD + size;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEntryInfo.class, this);
    }
}
