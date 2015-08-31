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

package org.apache.ignite.internal.processors.cache.version;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Raw versioned entry.
 */
public class GridCacheRawVersionedEntry<K, V> extends DataStreamerEntry implements
    GridCacheVersionedEntry<K, V>, GridCacheVersionable, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key bytes. */
    @GridDirectTransient
    private byte[] keyBytes;

    /** Value bytes. */
    private byte[] valBytes;

    /** TTL. */
    private long ttl;

    /** Expire time. */
    private long expireTime;

    /** Version. */
    private GridCacheVersion ver;

    /**
     * {@code Externalizable} support.
     */
    public GridCacheRawVersionedEntry() {
        // No-op.
    }

    /**
     * Constructor used for local store load when key and value are available.
     *
     * @param key Key.
     * @param val Value.
     * @param expireTime Expire time.
     * @param ttl TTL.
     * @param ver Version.
     */
    public GridCacheRawVersionedEntry(KeyCacheObject key,
        @Nullable CacheObject val,
        long ttl,
        long expireTime,
        GridCacheVersion ver) {
        assert key != null;

        this.key = key;
        this.val = val;
        this.ttl = ttl;
        this.expireTime = expireTime;
        this.ver = ver;
    }

    /**
     * Constructor used in receiver hub where marshalled key and value are available and we do not want to
     * unmarshal value.
     *
     * @param keyBytes Key.
     * @param valBytes Value bytes.
     * @param expireTime Expire time.
     * @param ttl TTL.
     * @param ver Version.
     */
    public GridCacheRawVersionedEntry(byte[] keyBytes,
        byte[] valBytes,
        long ttl,
        long expireTime,
        GridCacheVersion ver) {
        this.keyBytes = keyBytes;
        this.valBytes = valBytes;
        this.ttl = ttl;
        this.expireTime = expireTime;
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public K key() {
        assert key != null : "Entry is being improperly processed.";

        return key.value(null, false);
    }

    /**
     * @param key Key.
     */
    public void key(KeyCacheObject key) {
        this.key = key;
    }

    /**
     * @return Key bytes.
     */
    public byte[] keyBytes() {
        return keyBytes;
    }

    /** {@inheritDoc} */
    @Override public V value() {
        return val != null ? val.<V>value(null, false) : null;
    }

    /**
     * @return Value bytes.
     */
    public byte[] valueBytes() {
        return valBytes;
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return ttl;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expireTime;
    }

    /** {@inheritDoc} */
    @Override public byte dataCenterId() {
        return ver.dataCenterId();
    }

    /** {@inheritDoc} */
    @Override public int topologyVersion() {
        return ver.topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return ver.order();
    }

    /** {@inheritDoc} */
    @Override public long globalTime() {
        return ver.globalTime();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * Perform internal unmarshal of this entry. It must be performed after entry is deserialized and before
     * its restored key/value are needed.
     *
     * @param ctx Context.
     * @param marsh Marshaller.
     * @throws IgniteCheckedException If failed.
     */
    public void unmarshal(CacheObjectContext ctx, Marshaller marsh) throws IgniteCheckedException {
        unmarshalKey(ctx, marsh);

        if (val == null && valBytes != null) {
            val = marsh.unmarshal(valBytes, null);

            val.finishUnmarshal(ctx, null);
        }
    }

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void unmarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        assert key != null;

        key.finishUnmarshal(ctx, null);

        if (val != null)
            val.finishUnmarshal(ctx, null);
    }

    /**
     * Perform internal key unmarshal of this entry. It must be performed after entry is deserialized and before
     * its restored key/value are needed.
     *
     * @param ctx Context.
     * @param marsh Marshaller.
     * @throws IgniteCheckedException If failed.
     */
    public void unmarshalKey(CacheObjectContext ctx, Marshaller marsh) throws IgniteCheckedException {
        if (key == null) {
            assert keyBytes != null;

            key = marsh.unmarshal(keyBytes, null);

            key.finishUnmarshal(ctx, null);
        }
    }

    /**
     * Perform internal marshal of this entry before it will be serialized.
     *
     * @param ctx Context.
     * @param marsh Marshaller.
     * @throws IgniteCheckedException If failed.
     */
    public void marshal(CacheObjectContext ctx, Marshaller marsh) throws IgniteCheckedException {
        if (keyBytes == null) {
            key.prepareMarshal(ctx);

            keyBytes = marsh.marshal(key);
        }

        if (valBytes == null && val != null) {
            val.prepareMarshal(ctx);

            valBytes = marsh.marshal(val);
        }
    }

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void prepareDirectMarshal(CacheObjectContext ctx) throws IgniteCheckedException {
        key.prepareMarshal(ctx);

        if (val != null)
            val.prepareMarshal(ctx);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 103;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 2:
                expireTime = reader.readLong("expireTime");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                ttl = reader.readLong("ttl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                valBytes = reader.readByteArray("valBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                ver = reader.readMessage("ver");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        assert key != null;
        assert !(val != null && valBytes != null);

        return reader.afterMessageRead(GridCacheRawVersionedEntry.class);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        assert key != null;
        assert !(val != null && valBytes != null);

        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 2:
                if (!writer.writeLong("expireTime", expireTime))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeLong("ttl", ttl))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByteArray("valBytes", valBytes))
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
    @Override public byte fieldsCount() {
        return 6;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheRawVersionedEntry.class, this,
            "keyBytesLen", keyBytes != null ? keyBytes.length : "n/a",
            "valBytesLen", valBytes != null ? valBytes.length : "n/a",
            "super", super.toString());
    }
}