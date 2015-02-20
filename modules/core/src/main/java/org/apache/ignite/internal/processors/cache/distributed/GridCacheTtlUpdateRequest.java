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
    /** */
    private static final long serialVersionUID = 0L;

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
        assert ttl >= 0 || ttl == CU.TTL_ZERO : ttl;

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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeCollection("keysBytes", keysBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeCollection("nearKeysBytes", nearKeysBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("nearVers", nearVers, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("topVer", topVer))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeLong("ttl", ttl))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeCollection("vers", vers, MessageCollectionItemType.MSG))
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

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                keysBytes = reader.readCollection("keysBytes", MessageCollectionItemType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                nearKeysBytes = reader.readCollection("nearKeysBytes", MessageCollectionItemType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                nearVers = reader.readCollection("nearVers", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                topVer = reader.readLong("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                ttl = reader.readLong("ttl");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                vers = reader.readCollection("vers", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 20;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 9;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTtlUpdateRequest.class, this, "super", super.toString());
    }
}
