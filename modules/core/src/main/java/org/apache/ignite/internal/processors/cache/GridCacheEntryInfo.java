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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;

/**
 * Entry information that gets passed over wire.
 */
public class GridCacheEntryInfo implements Externalizable, Message {
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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        // TODO IGNITE-51: field 'remaining'.
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
        // TODO IGNITE-51: field 'remaining'.
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

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
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
// TODO IGNITE-51
//        if (val == null && valBytes != null)
//            val = ctx.marshaller().unmarshal(valBytes, ldr);
    }

    /**
     * @return Marshalled size.
     */
    public int marshalledSize() {
        // TODO IGNITE-51.
        return 0;
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException In case of error.
     */
    public void marshal(GridCacheContext ctx) throws IgniteCheckedException {
        key.prepareMarshal(ctx.cacheObjectContext());

        if (val != null)
            val.prepareMarshal(ctx.cacheObjectContext());
// TODO IGNITE-51
//        boolean depEnabled = ctx.gridDeploy().enabled();
//
//        boolean valIsByteArr = val != null && val instanceof byte[];
//
//        if (keyBytes == null && depEnabled)
//            keyBytes = CU.marshal(ctx, key);
//
//        keyBytesSent = depEnabled || key == null;
//
//        if (valBytes == null && val != null && !valIsByteArr)
//            valBytes = CU.marshal(ctx, val);
//
//        valBytesSent = (valBytes != null && !valIsByteArr) || val == null;
    }

    /**
     * Unmarshalls entry.
     *
     * @param ctx Cache context.
     * @param clsLdr Class loader.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public void unmarshal(GridCacheContext ctx, ClassLoader clsLdr) throws IgniteCheckedException {
        key.finishUnmarshal(ctx.cacheObjectContext(), clsLdr);

        if (val != null)
            val.finishUnmarshal(ctx.cacheObjectContext(), clsLdr);
// TODO IGNITE-51
//        Marshaller mrsh = ctx.marshaller();
//
//        if (key == null)
//            key = mrsh.unmarshal(keyBytes, clsLdr);
//
//        if (ctx.isUnmarshalValues() && val == null && valBytes != null)
//            val = mrsh.unmarshal(valBytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
// TODO IGNITE-51.
//        out.writeInt(cacheId);
//        out.writeBoolean(keyBytesSent);
//        out.writeBoolean(valBytesSent);
//
//        if (keyBytesSent)
//            IgniteByteUtils.writeByteArray(out, keyBytes);
//        else
//            out.writeObject(key);
//
//        if (valBytesSent)
//            IgniteByteUtils.writeByteArray(out, valBytes);
//        else {
//            if (val != null && val instanceof byte[]) {
//                out.writeBoolean(true);
//
//                IgniteByteUtils.writeByteArray(out, (byte[]) val);
//            }
//            else {
//                out.writeBoolean(false);
//
//                out.writeObject(val);
//            }
//        }
//
//        out.writeLong(ttl);
//
//        long remaining;
//
//        // 0 means never expires.
//        if (expireTime == 0)
//            remaining = -1;
//        else {
//            remaining = expireTime - U.currentTimeMillis();
//
//            if (remaining < 0)
//                remaining = 0;
//        }
//
//        // Write remaining time.
//        out.writeLong(remaining);
//
//        CU.writeVersion(out, ver);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
// TODO IGNITE-51.
//        cacheId = in.readInt();
//        keyBytesSent = in.readBoolean();
//        valBytesSent = in.readBoolean();
//
//        if (keyBytesSent)
//            keyBytes = IgniteByteUtils.readByteArray(in);
//        else
//            key = (K)in.readObject();
//
//        if (valBytesSent)
//            valBytes = IgniteByteUtils.readByteArray(in);
//        else
//            val = in.readBoolean() ? (V) IgniteByteUtils.readByteArray(in) : (V)in.readObject();
//
//        ttl = in.readLong();
//
//        long remaining = in.readLong();
//
//        expireTime = remaining < 0 ? 0 : U.currentTimeMillis() + remaining;
//
//        // Account for overflow.
//        if (expireTime < 0)
//            expireTime = 0;
//
//        ver = CU.readVersion(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEntryInfo.class, this);

//        return S.toString(GridCacheEntryInfo.class, this,
//            "isNull", val == null,
//            "keyBytesSize", (keyBytes == null ? "null" : Integer.toString(keyBytes.length)),
//            "valBytesSize", (valBytes == null ? "null" : Integer.toString(valBytes.length)));
    }
}
