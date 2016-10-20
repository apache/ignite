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

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionable;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;

/**
 * Get response.
 */
public class GridNearInvokeResponse extends GridNearGetResponse implements GridCacheDeployable,
    GridCacheVersionable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringExclude
    private GridCacheReturn ret;

    /** */
    private byte[] cacheOper;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearInvokeResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param futId Future ID.
     * @param miniId Sub ID.
     * @param ver Version.
     * @param addDepInfo Deployment info.
     */
    public GridNearInvokeResponse(
        int cacheId,
        IgniteUuid futId,
        IgniteUuid miniId,
        GridCacheVersion ver,
        boolean addDepInfo
    ) {
        super(cacheId, futId, miniId, ver, addDepInfo);
    }

    /**
     * @return Cache return.
     */
    public GridCacheReturn cacheReturn() {
        return ret;
    }

    /**
     * @param ret Cache return.
     */
    public void cacheReturn(GridCacheReturn ret) {
        this.ret = ret;
    }

    /**
     * @return Cache operation.
     */
    public GridCacheOperation cacheOperation(int idx) {
        return GridCacheOperation.fromOrdinal(cacheOper[idx]);
    }

    /**
     * @param cacheOper Cache operations.
     */
    public void cacheOperation(byte[] cacheOper) {
        this.cacheOper = cacheOper;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (ret != null)
            ret.prepareMarshal(cctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        GridCacheContext cctx = ctx.cacheContext(cacheId());

        if (ret != null)
            ret.finishUnmarshal(cctx, ldr);
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
            case 10:
                if (!writer.writeMessage("ret", ret))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeByteArray("cacheOper", cacheOper))
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
            case 10:
                ret = reader.readMessage("ret");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                cacheOper = reader.readByteArray("cacheOper");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridNearInvokeResponse.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -113;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 12;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearInvokeResponse.class, this);
    }
}
