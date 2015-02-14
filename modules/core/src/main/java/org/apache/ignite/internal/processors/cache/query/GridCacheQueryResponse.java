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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Query request.
 */
public class GridCacheQueryResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
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
     */
    public GridCacheQueryResponse(int cacheId, long reqId, boolean finished, boolean fields) {
        this.cacheId = cacheId;
        this.reqId = reqId;
        this.finished = finished;
        this.fields = fields;
    }

    /**
     * @param cacheId Cache ID.
     * @param reqId Request id.
     * @param err Error.
     */
    public GridCacheQueryResponse(int cacheId, long reqId, Throwable err) {
        this.cacheId = cacheId;
        this.reqId = reqId;
        this.err = err;
        finished = true;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (err != null)
            errBytes = ctx.marshaller().marshal(err);

        metaDataBytes = marshalCollection(metadata, ctx);
        dataBytes = marshalCollection(data, ctx);

        if (ctx.deploymentEnabled() && !F.isEmpty(data)) {
            for (Object o : data) {
                if (o instanceof Map.Entry) {
                    Map.Entry e = (Map.Entry)o;

                    prepareObject(e.getKey(), ctx);
                    prepareObject(e.getValue(), ctx);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (errBytes != null)
            err = ctx.marshaller().unmarshal(errBytes, ldr);

        metadata = unmarshalCollection(metaDataBytes, ctx, ldr);
        data = unmarshalCollection(dataBytes, ctx, ldr);
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
    @SuppressWarnings("unchecked")
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

    /**
     * @return Error.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @return If fields query.
     */
    public boolean fields() {
        return fields;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheQueryResponse _clone = (GridCacheQueryResponse)_msg;

        _clone.finished = finished;
        _clone.reqId = reqId;
        _clone.err = err;
        _clone.errBytes = errBytes;
        _clone.fields = fields;
        _clone.metaDataBytes = metaDataBytes;
        _clone.metadata = metadata;
        _clone.dataBytes = dataBytes;
        _clone.data = data;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf, MessageWriteState state) {
        MessageWriter writer = state.writer();

        writer.setBuffer(buf);

        if (!super.writeTo(buf, state))
            return false;

        if (!state.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            state.setTypeWritten();
        }

        switch (state.index()) {
            case 3:
                if (!writer.writeCollection("dataBytes", dataBytes, byte[].class))
                    return false;

                state.increment();

            case 4:
                if (!writer.writeByteArray("errBytes", errBytes))
                    return false;

                state.increment();

            case 5:
                if (!writer.writeBoolean("fields", fields))
                    return false;

                state.increment();

            case 6:
                if (!writer.writeBoolean("finished", finished))
                    return false;

                state.increment();

            case 7:
                if (!writer.writeCollection("metaDataBytes", metaDataBytes, byte[].class))
                    return false;

                state.increment();

            case 8:
                if (!writer.writeLong("reqId", reqId))
                    return false;

                state.increment();

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 3:
                dataBytes = reader.readCollection("dataBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                errBytes = reader.readByteArray("errBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 5:
                fields = reader.readBoolean("fields");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 6:
                finished = reader.readBoolean("finished");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 7:
                metaDataBytes = reader.readCollection("metaDataBytes", byte[].class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 8:
                reqId = reader.readLong("reqId");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 59;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryResponse.class, this);
    }
}
