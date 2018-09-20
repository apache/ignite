/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * A response to {@link GridNearTxQueryResultsEnlistRequest}.
 */
public class GridNearTxQueryResultsEnlistResponse extends GridNearTxQueryEnlistResponse {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridCacheVersion dhtVer;

    /** */
    private IgniteUuid dhtFutId;

    /**
     * Default-constructor.
     */
    public GridNearTxQueryResultsEnlistResponse() {
        // No-op.
    }

    /**
     * @param cacheId Cache id.
     * @param futId Future id.
     * @param miniId Mini future id.
     * @param lockVer Lock version.
     * @param res Result.
     * @param dhtFutId Dht future id.
     * @param dhtVer Dht version.
     * @param newDhtNodes New
     */
    public GridNearTxQueryResultsEnlistResponse(int cacheId,
        IgniteUuid futId,
        int miniId,
        GridCacheVersion lockVer,
        long res,
        GridCacheVersion dhtVer,
        IgniteUuid dhtFutId,
        Set<UUID> newDhtNodes) {
        super(cacheId, futId, miniId, lockVer, res, false, newDhtNodes);

        this.dhtVer = dhtVer;
        this.dhtFutId = dhtFutId;
    }

    /**
     * @param cacheId Cache id.
     * @param futId Future id.
     * @param miniId Mini future id.
     * @param lockVer Lock version.
     * @param err Error.
     */
    public GridNearTxQueryResultsEnlistResponse(int cacheId,
        IgniteUuid futId,
        int miniId,
        GridCacheVersion lockVer,
        Throwable err) {
        super(cacheId, futId, miniId, lockVer, err);
    }

    /**
     * @return Dht version.
     */
    public GridCacheVersion dhtVersion() {
        return dhtVer;
    }

    /**
     * @return Dht future id.
     */
    public IgniteUuid dhtFutureId() {
        return dhtFutId;
    }


    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 12;
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
                if (!writer.writeIgniteUuid("dhtFutId", dhtFutId))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeMessage("dhtVer", dhtVer))
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
                dhtFutId = reader.readIgniteUuid("dhtFutId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                dhtVer = reader.readMessage("dhtVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(GridNearTxQueryResultsEnlistResponse.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 154;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxQueryResultsEnlistResponse.class, this);
    }
}
