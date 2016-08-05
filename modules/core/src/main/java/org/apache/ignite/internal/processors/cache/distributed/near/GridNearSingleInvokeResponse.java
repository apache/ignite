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
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class GridNearSingleInvokeResponse extends GridCacheMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int INVALID_PART_FLAG_MASK = 0x1;

    /** */
    public static final int CONTAINS_VAL_FLAG_MASK = 0x2;

    /** Future ID. */
    private long futId;

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private CacheObject invokeResObj;

    /** Error. */
    @GridDirectTransient
    private IgniteCheckedException err;

    /** Serialized error. */
    private byte[] errBytes;

    /** */
    private byte flags;

    /** */
    @GridToStringExclude
    private GridCacheReturn ret;

    /** */
    private byte cacheOper;

    /**
     * Empty constructor required for {@link Message}.
     */
    public GridNearSingleInvokeResponse() {
        // No-op.
    }

    /**
     * @param ctx Cache context.
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param topVer Topology version.
     * @param invalidPartitions {@code True} if invalid partitions error occurred.
     * @param addDepInfo Deployment info.
     */
    public GridNearSingleInvokeResponse(
        GridCacheContext ctx,
        KeyCacheObject key,
        int cacheId,
        long futId,
        AffinityTopologyVersion topVer,
        @Nullable CacheObject invokeResObj,
        boolean invalidPartitions,
        boolean addDepInfo,
        CacheObject invokeRes,
        IgniteCheckedException err,
        Exception invErr,
        GridCacheOperation cacheOper
    ) {
        this.cacheId = cacheId;
        this.futId = futId;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;
        this.cacheOper = (byte)cacheOper.ordinal();
        this.invokeResObj = invokeResObj;

        this.ret = new GridCacheReturn(false);

        if (invErr != null || invokeRes != null)
            this.ret.addEntryProcessResult(ctx, key, null, invokeRes, invErr, true);
        else
            this.ret.invokeResult(true);

        this.err = err;

        if (invalidPartitions)
            flags = (byte)(flags | INVALID_PART_FLAG_MASK);
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param topVer Topology version.
     * @param invalidPartitions {@code True} if invalid partitions error occurred.
     */
    public GridNearSingleInvokeResponse(
        int cacheId,
        long futId,
        AffinityTopologyVersion topVer,
        boolean invalidPartitions
    ) {
        this(null, null, cacheId, futId, topVer, null, true, false, null, null, null, null);
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param topVer Topology version.
     */
    public GridNearSingleInvokeResponse(
        int cacheId,
        long futId,
        AffinityTopologyVersion topVer,
        IgniteCheckedException err
    ) {
        this(null, null, cacheId, futId, topVer, null, false, false, null, err, null, null);
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
        return topVer;
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
    public CacheObject result() {
        return invokeResObj;
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /**
     * @return Cache operation.
     */
    public GridCacheOperation op() {
        return GridCacheOperation.fromOrdinal(cacheOper);
    }

    /**
     * @return Value.
     */
    public CacheObject value() {
        return invokeResObj;
    }

    /**
     * @return Cache invoke result.
     */
    public GridCacheReturn invokeResult() {
        return ret;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (invokeResObj != null)
            prepareMarshalCacheObject(invokeResObj, cctx);

        if (ret != null)
            ret.prepareMarshal(cctx);

        if (err != null && errBytes == null)
            errBytes = ctx.marshaller().marshal(err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (invokeResObj != null)
            invokeResObj.finishUnmarshal(cctx.cacheObjectContext(), ldr);

        if (ret != null)
            ret.finishUnmarshal(cctx, ldr);

        if (errBytes != null && err == null)
            err = ctx.marshaller().unmarshal(errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
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
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("ret", ret))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeMessage("invokeResObj", invokeResObj))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeByte("cacheOper", cacheOper))
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
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                ret = reader.readMessage("ret");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                invokeResObj = reader.readMessage("invokeResObj");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                cacheOper = reader.readByte("cacheOper");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridNearSingleInvokeResponse.class);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -111;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearSingleInvokeResponse.class, this);
    }
}
