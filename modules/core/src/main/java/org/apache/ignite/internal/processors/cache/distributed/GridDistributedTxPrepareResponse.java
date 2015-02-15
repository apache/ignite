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
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Response to prepare request.
 */
public class GridDistributedTxPrepareResponse<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collections of local lock candidates. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<K, Collection<GridCacheMvccCandidate<K>>> cands;

    /** */
    private byte[] candsBytes;

    /** Error. */
    @GridToStringExclude
    @GridDirectTransient
    private Throwable err;

    /** Serialized error. */
    private byte[] errBytes;

    /**
     * Empty constructor (required by {@link Externalizable}).
     */
    public GridDistributedTxPrepareResponse() {
        /* No-op. */
    }

    /**
     * @param xid Transaction ID.
     */
    public GridDistributedTxPrepareResponse(GridCacheVersion xid) {
        super(xid, 0);
    }

    /**
     * @param xid Lock ID.
     * @param err Error.
     */
    public GridDistributedTxPrepareResponse(GridCacheVersion xid, Throwable err) {
        super(xid, 0);

        this.err = err;
    }

    /**
     * @return Error.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @param err Error to set.
     */
    public void error(Throwable err) {
        this.err = err;
    }

    /**
     * @return Rollback flag.
     */
    public boolean isRollback() {
        return err != null;
    }

    /**
     * @param cands Candidates map to set.
     */
    public void candidates(Map<K, Collection<GridCacheMvccCandidate<K>>> cands) {
        this.cands = cands;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (candsBytes == null && cands != null) {
            if (ctx.deploymentEnabled()) {
                for (K k : cands.keySet())
                    prepareObject(k, ctx);
            }

            candsBytes = CU.marshal(ctx, cands);
        }

        if (err != null)
            errBytes = ctx.marshaller().marshal(err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (candsBytes != null && cands == null)
            cands = ctx.marshaller().unmarshal(candsBytes, ldr);

        if (errBytes != null)
            err = ctx.marshaller().unmarshal(errBytes, ldr);
    }

    /**
     *
     * @param key Candidates key.
     * @return Collection of lock candidates at given index.
     */
    @Nullable public Collection<GridCacheMvccCandidate<K>> candidatesForKey(K key) {
        assert key != null;

        if (cands == null)
            return null;

        return cands.get(key);
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
            case 8:
                if (!writer.writeField("candsBytes", candsBytes, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeField("errBytes", errBytes, MessageFieldType.BYTE_ARR))
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
            case 8:
                candsBytes = reader.readField("candsBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 9:
                errBytes = reader.readField("errBytes", MessageFieldType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 26;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxPrepareResponse.class, this, "err",
            err == null ? "null" : err.toString(), "super", super.toString());
    }
}
