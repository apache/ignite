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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class GridNearSingleGetResponse extends GridCacheIdMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int INVALID_PART_FLAG_MASK = 0x1;

    /** */
    public static final int CONTAINS_VAL_FLAG_MASK = 0x2;

    /** Future ID. */
    private long futId;

    /** */
    private Message res;

    /** */
    private AffinityTopologyVersion topVer;

    /** Error. */
    @GridDirectTransient
    private IgniteCheckedException err;

    /** Serialized error. */
    private byte[] errBytes;

    /** */
    private byte flags;

    /**
     * Empty constructor required for {@link Message}.
     */
    public GridNearSingleGetResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param topVer Topology version.
     * @param res Result.
     * @param invalidPartitions {@code True} if invalid partitions error occurred.
     * @param addDepInfo Deployment info.
     */
    public GridNearSingleGetResponse(
        int cacheId,
        long futId,
        AffinityTopologyVersion topVer,
        @Nullable Message res,
        boolean invalidPartitions,
        boolean addDepInfo
    ) {
        this.cacheId = cacheId;
        this.futId = futId;
        this.topVer = topVer;
        this.res = res;
        this.addDepInfo = addDepInfo;

        if (invalidPartitions)
            flags = (byte)(flags | INVALID_PART_FLAG_MASK);
    }

    /**
     * @param err Error.
     */
    public void error(IgniteCheckedException err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException error() {
        return err;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer != null ? topVer : super.topologyVersion();
    }

    /**
     * @return {@code True} if invalid partitions error occurred.
     */
    public boolean invalidPartitions() {
        return (flags & INVALID_PART_FLAG_MASK) != 0;
    }

    /**
     * @return Results for request with set flag {@link GridNearSingleGetRequest#skipValues()}.
     */
    public boolean containsValue() {
        return (flags & CONTAINS_VAL_FLAG_MASK) != 0;
    }

    /**
     *
     */
    public void setContainsValue() {
        flags = (byte)(flags | CONTAINS_VAL_FLAG_MASK);
    }

    /**
     * @return Result.
     */
    public Message result() {
        return res;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (res != null) {
            GridCacheContext cctx = ctx.cacheContext(cacheId);

            if (res instanceof CacheObject)
                prepareMarshalCacheObject((CacheObject) res, cctx);
            else if (res instanceof CacheVersionedValue)
                ((CacheVersionedValue)res).prepareMarshal(cctx.cacheObjectContext());
            else if (res instanceof GridCacheEntryInfo)
                ((GridCacheEntryInfo)res).marshal(cctx);
        }

        if (err != null && errBytes == null)
            errBytes = U.marshal(ctx, err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (res != null) {
            GridCacheContext cctx = ctx.cacheContext(cacheId());

            if (res instanceof CacheObject)
                ((CacheObject)res).finishUnmarshal(cctx.cacheObjectContext(), ldr);
            else if (res instanceof CacheVersionedValue)
                ((CacheVersionedValue)res).finishUnmarshal(cctx, ldr);
            else if (res instanceof GridCacheEntryInfo)
                ((GridCacheEntryInfo)res).unmarshal(cctx, ldr);
        }

        if (errBytes != null && err == null)
            err = U.unmarshal(ctx, errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
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
            case 4:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("res", res))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
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
            case 4:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                res = reader.readMessage("res");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearSingleGetResponse.class);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 117;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 9;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearSingleGetResponse.class, this);
    }
}
