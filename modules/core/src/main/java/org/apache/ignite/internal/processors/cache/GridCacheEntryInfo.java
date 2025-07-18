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
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Entry information that gets passed over wire.
 */
public class GridCacheEntryInfo implements Message {
    /** */
    private static final int SIZE_OVERHEAD = 3 * 8 /* reference */ + 4 /* int */ + 2 * 8 /* long */ + 32 /* version */;

    /** Cache key. */
    @Order(0)
    @GridToStringInclude
    private KeyCacheObject key;

    /** Cache ID. */
    @Order(1)
    private int cacheId;

    /** Cache value. */
    @Order(value = 2, method = "value")
    private CacheObject val;

    /** Time to live. */
    @Order(3)
    private long ttl;

    /** Expiration time. */
    @Order(4)
    private long expireTime;

    /** Entry version. */
    @Order(value = 5, method = "version")
    private GridCacheVersion ver;

    /** New flag. */
    private boolean isNew;

    /** Deleted flag. */
    private boolean deleted;

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @param cacheId Cache ID.
     */
    public void cacheId(int cacheId) {
        this.cacheId = cacheId;
    }

    /**
     * @param key Entry key.
     */
    public void key(KeyCacheObject key) {
        this.key = key;
    }

    /**
     * @return Entry key.
     */
    public KeyCacheObject key() {
        return key;
    }

    /**
     * @return Entry value.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @param value Entry value.
     */
    public void value(CacheObject value) {
        this.val = value;
    }

    /**
     * @return Expire time.
     */
    public long expireTime() {
        return expireTime;
    }

    /**
     * @param expireTime Expiration time.
     */
    public void expireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    /**
     * @return Time to live.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @param ttl Time to live.
     */
    public void ttl(long ttl) {
        this.ttl = ttl;
    }

    /**
     * @return Version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /**
     * @param version Version.
     */
    public void version(GridCacheVersion version) {
        this.ver = version;
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

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 91;
    }

    /**
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    public void unmarshalValue(GridCacheContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (val != null)
            val.finishUnmarshal(ctx.cacheObjectContext(), ldr);
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

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException In case of error.
     */
    public void marshal(GridCacheContext ctx) throws IgniteCheckedException {
        marshal(ctx.cacheObjectContext());
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException In case of error.
     */
    public void marshal(CacheObjectContext ctx) throws IgniteCheckedException {
        assert key != null;

        key.prepareMarshal(ctx);

        if (val != null)
            val.prepareMarshal(ctx);

        if (expireTime == 0)
            expireTime = -1;
        else {
            expireTime = expireTime - U.currentTimeMillis();

            if (expireTime < 0)
                expireTime = 0;
        }
    }

    /**
     * Unmarshalls entry.
     *
     * @param ctx Cache context.
     * @param clsLdr Class loader.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public void unmarshal(GridCacheContext ctx, ClassLoader clsLdr) throws IgniteCheckedException {
        unmarshal(ctx.cacheObjectContext(), clsLdr);
    }

    /**
     * Unmarshalls entry.
     *
     * @param ctx Cache context.
     * @param clsLdr Class loader.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public void unmarshal(CacheObjectContext ctx, ClassLoader clsLdr) throws IgniteCheckedException {
        key.finishUnmarshal(ctx, clsLdr);

        if (val != null)
            val.finishUnmarshal(ctx, clsLdr);

        long remaining = expireTime;

        expireTime = remaining < 0 ? 0 : U.currentTimeMillis() + remaining;

        // Account for overflow.
        if (expireTime < 0)
            expireTime = 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEntryInfo.class, this);
    }
}
