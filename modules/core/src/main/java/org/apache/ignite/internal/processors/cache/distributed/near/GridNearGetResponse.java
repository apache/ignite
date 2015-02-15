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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Get response.
 */
public class GridNearGetResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable,
    GridCacheVersionable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Sub ID. */
    private IgniteUuid miniId;

    /** Version. */
    private GridCacheVersion ver;

    /** Result. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<GridCacheEntryInfo<K, V>> entries;

    /** */
    private byte[] entriesBytes;

    /** Keys to retry due to ownership shift. */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Collection<Integer> invalidParts = new GridLeanSet<>();

    /** Topology version if invalid partitions is not empty. */
    private long topVer;

    /** Error. */
    @GridDirectTransient
    private Throwable err;

    /** Serialized error. */
    private byte[] errBytes;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearGetResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param miniId Sub ID.
     * @param ver Version.
     */
    public GridNearGetResponse(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion ver
    ) {
        assert futId != null;
        assert miniId != null;
        assert ver != null;

        this.cacheId = cacheId;
        this.futId = futId;
        this.miniId = miniId;
        this.ver = ver;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Sub ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /**
     * @return Entries.
     */
    public Collection<GridCacheEntryInfo<K, V>> entries() {
        return entries;
    }

    /**
     * @param entries Entries.
     */
    public void entries(Collection<GridCacheEntryInfo<K, V>> entries) {
        this.entries = entries;
    }

    /**
     * @return Failed filter set.
     */
    public Collection<Integer> invalidPartitions() {
        return invalidParts;
    }

    /**
     * @param invalidParts Partitions to retry due to ownership shift.
     * @param topVer Topology version.
     */
    public void invalidPartitions(Collection<Integer> invalidParts, long topVer) {
        this.invalidParts = invalidParts;
        this.topVer = topVer;
    }

    /**
     * @return Topology version if this response has invalid partitions.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Error.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @param err Error.
     */
    public void error(Throwable err) {
        this.err = err;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (entries != null) {
            marshalInfos(entries, ctx);

            entriesBytes = ctx.marshaller().marshal(entries);
        }

        if (err != null)
            errBytes = ctx.marshaller().marshal(err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (entriesBytes != null) {
            entries = ctx.marshaller().unmarshal(entriesBytes, ldr);

            unmarshalInfos(entries, ctx.cacheContext(cacheId()), ldr);
        }

        if (errBytes != null)
            err = ctx.marshaller().unmarshal(errBytes, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeMessageType(directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeField("entriesBytes", entriesBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeField("errBytes", errBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeField("futId", futId, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeCollectionField("invalidParts", invalidParts, MessageFieldType.INT))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeField("miniId", miniId, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeField("topVer", topVer, MessageFieldType.LONG))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeField("ver", ver, MessageFieldType.MSG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 3:
                entriesBytes = reader.readField("entriesBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                errBytes = reader.readField("errBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 5:
                futId = reader.readField("futId", MessageFieldType.IGNITE_UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 6:
                invalidParts = reader.readCollectionField("invalidParts", MessageFieldType.INT);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 7:
                miniId = reader.readField("miniId", MessageFieldType.IGNITE_UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 8:
                topVer = reader.readField("topVer", MessageFieldType.LONG);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                ver = reader.readField("ver", MessageFieldType.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 50;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearGetResponse.class, this);
    }
}
