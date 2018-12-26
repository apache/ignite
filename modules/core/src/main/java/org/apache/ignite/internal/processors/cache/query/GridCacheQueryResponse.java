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

package org.apache.ignite.internal.processors.cache.query;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Query request.
 */
public class GridCacheQueryResponse extends GridCacheIdMessage implements GridCacheDeployable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean finished;

    /** */
    private long reqId;

    /** */
    @GridDirectTransient
    private Throwable err;

    /** */
    private byte[] errBytes;

    /** */
    private boolean fields;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> metaDataBytes;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private List<GridQueryFieldMetadata> metadata;

    /** */
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> dataBytes;

    /** */
    @GridDirectTransient
    private Collection<Object> data;

    /**
     * Empty constructor for {@link Externalizable}
     */
    public GridCacheQueryResponse() {
        //No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param reqId Request id.
     * @param finished Last response or not.
     * @param fields Fields query or not.
     * @param addDepInfo Deployment info flag.
     */
    public GridCacheQueryResponse(int cacheId, long reqId, boolean finished, boolean fields, boolean addDepInfo) {
        this.cacheId = cacheId;
        this.reqId = reqId;
        this.finished = finished;
        this.fields = fields;
        this.addDepInfo = addDepInfo;
    }

    /**
     * @param cacheId Cache ID.
     * @param reqId Request id.
     * @param err Error.
     * @param addDepInfo Deployment info flag.
     */
    public GridCacheQueryResponse(int cacheId, long reqId, Throwable err, boolean addDepInfo) {
        this.cacheId = cacheId;
        this.reqId = reqId;
        this.err = err;
        this.addDepInfo = addDepInfo;

        finished = true;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);

        if (err != null && errBytes == null)
            errBytes = U.marshal(ctx, err);

        if (metaDataBytes == null && metadata != null)
            metaDataBytes = marshalCollection(metadata, cctx);

        if (dataBytes == null && data != null)
            dataBytes = marshalCollection(data, cctx);

        if (addDepInfo && !F.isEmpty(data)) {
            for (Object o : data) {
                if (o instanceof Map.Entry) {
                    Map.Entry e = (Map.Entry)o;

                    prepareObject(e.getKey(), cctx);
                    prepareObject(e.getValue(), cctx);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (errBytes != null && err == null)
            err = U.unmarshal(ctx, errBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        if (metadata == null)
            metadata = unmarshalCollection(metaDataBytes, ctx, ldr);

        if (data == null)
            data = unmarshalCollection0(dataBytes, ctx, ldr);
    }

    /**
     * @param byteCol Collection to unmarshal.
     * @param ctx Context.
     * @param ldr Loader.
     * @return Unmarshalled collection.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable protected <T> List<T> unmarshalCollection0(@Nullable Collection<byte[]> byteCol,
        GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        assert ldr != null;
        assert ctx != null;

        if (byteCol == null)
            return null;

        List<T> col = new ArrayList<>(byteCol.size());

        Marshaller marsh = ctx.marshaller();

        ClassLoader ldr0 = U.resolveClassLoader(ldr, ctx.gridConfig());

        CacheObjectContext cacheObjCtx = null;

        for (byte[] bytes : byteCol) {
            Object obj = bytes == null ? null : marsh.<T>unmarshal(bytes, ldr0);

            if (obj instanceof Map.Entry) {
                Object key = ((Map.Entry)obj).getKey();

                if (key instanceof KeyCacheObject) {
                    if (cacheObjCtx == null)
                        cacheObjCtx = ctx.cacheContext(cacheId).cacheObjectContext();

                    ((KeyCacheObject)key).finishUnmarshal(cacheObjCtx, ldr0);
                }
            }

            col.add((T)obj);
        }

        return col;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @return Metadata.
     */
    public List<GridQueryFieldMetadata> metadata() {
        return metadata;
    }

    /**
     * @param metadata Metadata.
     */
    public void metadata(@Nullable List<GridQueryFieldMetadata> metadata) {
        this.metadata = metadata;
    }

    /**
     * @return Query data.
     */
    public Collection<Object> data() {
        return data;
    }

    /**
     * @param data Query data.
     */
    public void data(Collection<?> data) {
        this.data = (Collection<Object>)data;
    }

    /**
     * @return If this is last response for this request or not.
     */
    public boolean isFinished() {
        return finished;
    }

    /**
     * @param finished If this is last response for this request or not.
     */
    public void finished(boolean finished) {
        this.finished = finished;
    }

    /**
     * @return Request id.
     */
    public long requestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public Throwable error() {
        return err;
    }

    /**
     * @return If fields query.
     */
    public boolean fields() {
        return fields;
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
                if (!writer.writeCollection("dataBytes", dataBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeBoolean("fields", fields))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeBoolean("finished", finished))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeCollection("metaDataBytes", metaDataBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeLong("reqId", reqId))
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
                dataBytes = reader.readCollection("dataBytes", MessageCollectionItemType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                fields = reader.readBoolean("fields");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                finished = reader.readBoolean("finished");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                metaDataBytes = reader.readCollection("metaDataBytes", MessageCollectionItemType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                reqId = reader.readLong("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridCacheQueryResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 59;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryResponse.class, this);
    }
}
