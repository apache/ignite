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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Entry information that gets passed over wire.
 */
public class GridCacheEntryInfo implements Message {
    /** */
    private static final int SIZE_OVERHEAD = 3 * 8 /* reference */ + 4 /* int */ + 2 * 8 /* long */ + 32 /* version */;

    /** */
    private static final long serialVersionUID = 0L;

    /** Cache key. */
    @GridToStringInclude
    private KeyCacheObject key;

    /** Cache ID. */
    private int cacheId;

    /** Cache value. */
    private CacheObject val;

    /** Time to live. */
    private long ttl;

    /** Expiration time. */
    private long expireTime;

    /** Entry version. */
    private GridCacheVersion ver;

    /** New flag. */
    @GridDirectTransient
    private boolean isNew;

    /** Deleted flag. */
    @GridDirectTransient
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
     * @param val Entry value.
     */
    public void value(CacheObject val) {
        this.val = val;
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
     * @param ver Version.
     */
    public void version(GridCacheVersion ver) {
        this.ver = ver;
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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeInt("cacheId", cacheId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("expireTime", expireTime))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeMessage("key", key))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeLong("ttl", ttl))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("val", val))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("ver", ver))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                cacheId = reader.readInt("cacheId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                expireTime = reader.readLong("expireTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                key = reader.readMessage("key");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                ttl = reader.readLong("ttl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                val = reader.readMessage("val");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                ver = reader.readMessage("ver");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridCacheEntryInfo.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 91;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 6;
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
