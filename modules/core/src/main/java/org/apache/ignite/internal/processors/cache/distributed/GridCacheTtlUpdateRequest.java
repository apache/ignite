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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;
import java.util.*;

/**
 *
 */
public class GridCacheTtlUpdateRequest<K, V> extends GridCacheMessage<K, V> {
    /** Entries keys. */
    @GridToStringInclude
    @GridDirectTransient
    private List<K> keys;

    /** Keys bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> keysBytes;

    /** Entries versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> vers;

    /** Near entries keys. */
    @GridToStringInclude
    @GridDirectTransient
    private List<K> nearKeys;

    /** Near entries bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> nearKeysBytes;

    /** Near entries versions. */
    @GridDirectCollection(GridCacheVersion.class)
    private List<GridCacheVersion> nearVers;

    /** New TTL. */
    private long ttl;

    /** Topology version. */
    private long topVer;

    /**
     * Required empty constructor.
     */
    public GridCacheTtlUpdateRequest() {
        // No-op.
    }

    /**
     * @param topVer Topology version.
     * @param ttl TTL.
     */
    public GridCacheTtlUpdateRequest(long topVer, long ttl) {
        assert ttl >= 0 : ttl;

        this.topVer = topVer;
        this.ttl = ttl;
    }

    /**
     * @return Topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return TTL.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @param keyBytes Key bytes.
     * @param ver Version.
     */
    public void addEntry(byte[] keyBytes, GridCacheVersion ver) {
        if (keysBytes == null) {
            keysBytes = new ArrayList<>();

            vers = new ArrayList<>();
        }

        keysBytes.add(keyBytes);

        vers.add(ver);
    }

    /**
     * @param keyBytes Key bytes.
     * @param ver Version.
     */
    public void addNearEntry(byte[] keyBytes, GridCacheVersion ver) {
        if (nearKeysBytes == null) {
            nearKeysBytes = new ArrayList<>();

            nearVers = new ArrayList<>();
        }

        nearKeysBytes.add(keyBytes);

        nearVers.add(ver);
    }

    /**
     * @return Keys.
     */
    public List<K> keys() {
        return keys;
    }

    /**
     * @return Versions.
     */
    public List<GridCacheVersion > versions() {
        return vers;
    }

    /**
     * @param idx Entry index.
     * @return Version.
     */
    public GridCacheVersion version(int idx) {
        assert idx >= 0 && idx < vers.size() : idx;

        return vers.get(idx);
    }

    /**
     * @return Keys for near cache.
     */
    public List<K> nearKeys() {
        return nearKeys;
    }

    /**
     * @return Versions for near cache entries.
     */
    public List<GridCacheVersion > nearVersions() {
        return nearVers;
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr)
        throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (keys == null && keysBytes != null)
            keys = unmarshalCollection(keysBytes, ctx, ldr);

        if (nearKeys == null && nearKeysBytes != null)
            nearKeys = unmarshalCollection(nearKeysBytes, ctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 20;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override public MessageAdapter clone() {
        GridCacheTtlUpdateRequest _clone = new GridCacheTtlUpdateRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!typeWritten) {
            if (!writer.writeByte(null, directType()))
                return false;

            typeWritten = true;
        }

        switch (state) {
            case 3:
                if (!writer.writeCollection("keysBytes", keysBytes, byte[].class))
                    return false;

                state++;

            case 4:
                if (!writer.writeCollection("nearKeysBytes", nearKeysBytes, byte[].class))
                    return false;

                state++;

            case 5:
                if (!writer.writeCollection("nearVers", nearVers, GridCacheVersion.class))
                    return false;

                state++;

            case 6:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                state++;

            case 7:
                if (!writer.writeLong("ttl", ttl))
                    return false;

                state++;

            case 8:
                if (!writer.writeCollection("vers", vers, GridCacheVersion.class))
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (state) {
            case 3:
                keysBytes = reader.readCollection("keysBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 4:
                nearKeysBytes = reader.readCollection("nearKeysBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 5:
                nearVers = reader.readCollection("nearVers", GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                state++;

            case 6:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 7:
                ttl = reader.readLong("ttl");

                if (!reader.isLastRead())
                    return false;

                state++;

            case 8:
                vers = reader.readCollection("vers", GridCacheVersion.class);

                if (!reader.isLastRead())
                    return false;

                state++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheTtlUpdateRequest _clone = (GridCacheTtlUpdateRequest)_msg;

        _clone.keys = keys;
        _clone.keysBytes = keysBytes;
        _clone.vers = vers;
        _clone.nearKeys = nearKeys;
        _clone.nearKeysBytes = nearKeysBytes;
        _clone.nearVers = nearVers;
        _clone.ttl = ttl;
        _clone.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTtlUpdateRequest.class, this, "super", super.toString());
    }
}
