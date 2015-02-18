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
import org.apache.ignite.marshaller.*;

import java.io.*;

/**
 * Entry information that gets passed over wire.
 */
public class GridCacheEntryInfo<K, V> implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache key. */
    @GridToStringInclude
    @GridDirectTransient
    private K key;

    /** Cache ID. */
    private int cacheId;

    /** Key bytes. */
    private byte[] keyBytes;

    /** Key bytes sent. */
    private boolean keyBytesSent;

    /** Cache value. */
    @GridDirectTransient
    private V val;

    /** Value bytes. */
    private byte[] valBytes;

    /** Value bytes sent. */
    private boolean valBytesSent;

    /** Time to live. */
    private long ttl;

    /** Expiration time. */
    private long expireTime;

    /** Entry version. */
    private GridCacheVersion ver;

    /** New flag. */
    private boolean isNew;

    /** Deleted flag. */
    private transient boolean deleted;

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
    public void key(K key) {
        this.key = key;
    }

    /**
     * @return Entry key.
     */
    public K key() {
        return key;
    }

    /**
     * @return Key bytes.
     */
    public byte[] keyBytes() {
        return keyBytes;
    }

    /**
     * @param keyBytes Key bytes.
     */
    public void keyBytes(byte[] keyBytes) {
        this.keyBytes = keyBytes;
    }

    /**
     * @return Entry value.
     */
    public V value() {
        return val;
    }

    /**
     * @param val Entry value.
     */
    public void value(V val) {
        this.val = val;
    }

    /**
     * @return Value bytes.
     */
    public byte[] valueBytes() {
        return valBytes;
    }

    /**
     * @param valBytes Value bytes.
     */
    public void valueBytes(byte[] valBytes) {
        this.valBytes = valBytes;
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

    /**
     * @param ctx Context.
     * @param ldr Loader.
     * @throws IgniteCheckedException If failed.
     */
    public void unmarshalValue(GridCacheContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (val == null && valBytes != null)
            val = ctx.marshaller().unmarshal(valBytes, ldr);
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException In case of error.
     */
    public void marshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        boolean depEnabled = ctx.gridDeploy().enabled();

        boolean valIsByteArr = val != null && val instanceof byte[];

        if (keyBytes == null && depEnabled)
            keyBytes = CU.marshal(ctx, key);

        keyBytesSent = depEnabled || key == null;

        if (valBytes == null && val != null && !valIsByteArr)
            valBytes = CU.marshal(ctx, val);

        valBytesSent = (valBytes != null && !valIsByteArr) || val == null;
    }

    /**
     * Unmarshalls entry.
     *
     * @param ctx Cache context.
     * @param clsLdr Class loader.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public void unmarshal(GridCacheContext<K, V> ctx, ClassLoader clsLdr) throws IgniteCheckedException {
        Marshaller mrsh = ctx.marshaller();

        if (key == null)
            key = mrsh.unmarshal(keyBytes, clsLdr);

        if (ctx.isUnmarshalValues() && val == null && valBytes != null)
            val = mrsh.unmarshal(valBytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(cacheId);
        out.writeBoolean(keyBytesSent);
        out.writeBoolean(valBytesSent);

        if (keyBytesSent)
            U.writeByteArray(out, keyBytes);
        else
            out.writeObject(key);

        if (valBytesSent)
            U.writeByteArray(out, valBytes);
        else {
            if (val != null && val instanceof byte[]) {
                out.writeBoolean(true);

                U.writeByteArray(out, (byte[])val);
            }
            else {
                out.writeBoolean(false);

                out.writeObject(val);
            }
        }

        out.writeLong(ttl);

        long remaining;

        // 0 means never expires.
        if (expireTime == 0)
            remaining = -1;
        else {
            remaining = expireTime - U.currentTimeMillis();

            if (remaining < 0)
                remaining = 0;
        }

        // Write remaining time.
        out.writeLong(remaining);

        CU.writeVersion(out, ver);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cacheId = in.readInt();
        keyBytesSent = in.readBoolean();
        valBytesSent = in.readBoolean();

        if (keyBytesSent)
            keyBytes = U.readByteArray(in);
        else
            key = (K)in.readObject();

        if (valBytesSent)
            valBytes = U.readByteArray(in);
        else
            val = in.readBoolean() ? (V)U.readByteArray(in) : (V)in.readObject();

        ttl = in.readLong();

        long remaining = in.readLong();

        expireTime = remaining < 0 ? 0 : U.currentTimeMillis() + remaining;

        // Account for overflow.
        if (expireTime < 0)
            expireTime = 0;

        ver = CU.readVersion(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheEntryInfo.class, this,
            "isNull", val == null,
            "keyBytesSize", (keyBytes == null ? "null" : Integer.toString(keyBytes.length)),
            "valBytesSize", (valBytes == null ? "null" : Integer.toString(valBytes.length)));
    }
}
