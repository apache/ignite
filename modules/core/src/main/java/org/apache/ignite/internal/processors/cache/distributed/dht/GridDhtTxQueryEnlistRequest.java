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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridDhtTxQueryEnlistRequest extends GridCacheIdMessage {
    /** */
    private static final long serialVersionUID = 5103887309729425173L;

    /** */
    private IgniteUuid dhtFutId;

    /** */
    private int batchId;

    /** DHT tx version. */
    private GridCacheVersion lockVer;

    /** */
    private EnlistOperation op;

    /** */
    private int mvccOpCnt;

    /** */
    @GridDirectCollection(KeyCacheObject.class)
    private List<KeyCacheObject> keys;

    /** */
    @GridDirectCollection(Message.class)
    private List<Message> vals;

    /**
     *
     */
    public GridDhtTxQueryEnlistRequest() {
    }

    /**
     * @param cacheId Cache id.
     * @param dhtFutId DHT future id.
     * @param lockVer Lock version.
     * @param op Operation.
     * @param batchId Batch id.
     * @param mvccOpCnt Mvcc operation counter.
     * @param keys Keys.
     * @param vals Values.
     */
    GridDhtTxQueryEnlistRequest(int cacheId,
        IgniteUuid dhtFutId,
        GridCacheVersion lockVer,
        EnlistOperation op,
        int batchId,
        int mvccOpCnt,
        List<KeyCacheObject> keys,
        List<Message> vals) {
        this.cacheId = cacheId;
        this.dhtFutId = dhtFutId;
        this.lockVer = lockVer;
        this.op = op;
        this.batchId = batchId;
        this.mvccOpCnt = mvccOpCnt;
        this.keys = keys;
        this.vals = vals;
    }

    /**
     * Returns request rows number.
     *
     * @return Request rows number.
     */
    public int batchSize() {
        return keys == null ? 0  : keys.size();
    }

    /**
     * @return Dht future id.
     */
    public IgniteUuid dhtFutureId() {
        return dhtFutId;
    }

    /**
     * @return Lock version.
     */
    public GridCacheVersion version() {
        return lockVer;
    }

    /**
     * @return Mvcc operation counter.
     */
    public int operationCounter() {
        return mvccOpCnt;
    }

    /**
     * @return Operation.
     */
    public EnlistOperation op() {
        return op;
    }

    /**
     * @return Keys.
     */
    public List<KeyCacheObject> keys() {
        return keys;
    }

    /**
     * @return Values.
     */
    public List<Message> values() {
        return vals;
    }

    /**
     * @return Batch id.
     */
    public int batchId() {
        return batchId;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 155;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        CacheObjectContext objCtx = ctx.cacheContext(cacheId).cacheObjectContext();

        if (keys != null) {
            for (int i = 0; i < keys.size(); i++) {

                keys.get(i).prepareMarshal(objCtx);

                if (vals != null) {
                    Message val = vals.get(i);

                    if (val instanceof CacheObject)
                        ((CacheObject)val).prepareMarshal(objCtx);
                    else if (val instanceof CacheEntryInfoCollection) {
                        for (GridCacheEntryInfo entry : ((CacheEntryInfoCollection)val).infos()) {
                            CacheObject entryVal = entry.value();

                            if (entryVal != null)
                                entryVal.prepareMarshal(objCtx);
                        }
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        CacheObjectContext objCtx = ctx.cacheContext(cacheId).cacheObjectContext();

        if (keys != null) {
            for (int i = 0; i < keys.size(); i++) {
                keys.get(i).finishUnmarshal(objCtx, ldr);

                if (vals != null) {
                    Message val = vals.get(i);

                    if (val instanceof CacheObject)
                        ((CacheObject)val).finishUnmarshal(objCtx, ldr);
                    else if (val instanceof CacheEntryInfoCollection) {
                        for (GridCacheEntryInfo entry : ((CacheEntryInfoCollection)val).infos()) {
                            CacheObject entryVal = entry.value();

                            if (entryVal != null)
                                entryVal.finishUnmarshal(objCtx, ldr);
                        }
                    }
                }
            }
        }
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
                if (!writer.writeInt("batchId", batchId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeIgniteUuid("dhtFutId", dhtFutId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMessage("lockVer", lockVer))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeInt("mvccOpCnt", mvccOpCnt))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection("vals", vals, MessageCollectionItemType.MSG))
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
                batchId = reader.readInt("batchId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                dhtFutId = reader.readIgniteUuid("dhtFutId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                keys = reader.readCollection("keys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                lockVer = reader.readMessage("lockVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                mvccOpCnt = reader.readInt("mvccOpCnt");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = EnlistOperation.fromOrdinal(opOrd);

                reader.incrementState();

            case 9:
                vals = reader.readCollection("vals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtTxQueryEnlistRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxQueryEnlistRequest.class, this);
    }
}
