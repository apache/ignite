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
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Near cache prepare response.
 */
public class GridNearTxPrepareResponse<K, V> extends GridDistributedTxPrepareResponse<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collection of versions that are pending and less than lock version. */
    @GridToStringInclude
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> pending;

    /** Future ID.  */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** DHT version. */
    private GridCacheVersion dhtVer;

    /** */
    @GridToStringInclude
    @GridDirectCollection(int.class)
    private Collection<Integer> invalidParts;

    /** Map of owned values to set on near node. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<IgniteTxKey<K>, GridTuple3<GridCacheVersion, V, byte[]>> ownedVals;

    /** Marshalled owned bytes. */
    @GridToStringExclude
    @GridDirectCollection(byte[].class)
    private Collection<byte[]> ownedValsBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearTxPrepareResponse() {
        // No-op.
    }

    /**
     * @param xid Xid version.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param dhtVer DHT version.
     * @param invalidParts Invalid partitions.
     * @param err Error.
     */
    public GridNearTxPrepareResponse(GridCacheVersion xid, IgniteUuid futId, IgniteUuid miniId, GridCacheVersion dhtVer,
        Collection<Integer> invalidParts, Throwable err) {
        super(xid, err);

        assert futId != null;
        assert miniId != null;
        assert dhtVer != null;

        this.futId = futId;
        this.miniId = miniId;
        this.dhtVer = dhtVer;
        this.invalidParts = invalidParts;
    }

    /**
     * Gets pending versions that are less than {@link #version()}.
     *
     * @return Pending versions.
     */
    public Collection<GridCacheVersion> pending() {
        return pending == null ? Collections.<GridCacheVersion>emptyList() : pending;
    }

    /**
     * Sets pending versions that are less than {@link #version()}.
     *
     * @param pending Pending versions.
     */
    public void pending(Collection<GridCacheVersion> pending) {
        this.pending = pending;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return DHT version.
     */
    public GridCacheVersion dhtVersion() {
        return dhtVer;
    }

    /**
     * Adds owned value.
     *
     * @param key Key.
     * @param ver DHT version.
     * @param val Value.
     * @param valBytes Value bytes.
     */
    public void addOwnedValue(IgniteTxKey<K> key, GridCacheVersion ver, V val, byte[] valBytes) {
        if (ownedVals == null)
            ownedVals = new HashMap<>();

        ownedVals.put(key, F.t(ver, val, valBytes));
    }

    /**
     * @return Owned values map.
     */
    public Map<IgniteTxKey<K>, GridTuple3<GridCacheVersion, V, byte[]>> ownedValues() {
        return ownedVals == null ? Collections.<IgniteTxKey<K>, GridTuple3<GridCacheVersion,V,byte[]>>emptyMap() :
            Collections.unmodifiableMap(ownedVals);
    }

    /**
     * @param key Key.
     * @return {@code True} if response has owned value for given key.
     */
    public boolean hasOwnedValue(IgniteTxKey<K> key) {
        return ownedVals != null && ownedVals.containsKey(key);
    }

    /**
     * @return Invalid partitions.
     */
    public Collection<Integer> invalidPartitions() {
        return invalidParts;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (ownedVals != null && ownedValsBytes == null) {
            ownedValsBytes = new ArrayList<>(ownedVals.size());

            for (Map.Entry<IgniteTxKey<K>, GridTuple3<GridCacheVersion, V, byte[]>> entry : ownedVals.entrySet()) {
                GridTuple3<GridCacheVersion, V, byte[]> tup = entry.getValue();

                boolean rawBytes = false;

                byte[] valBytes = tup.get3();

                if (valBytes == null) {
                    if (tup.get2() != null && tup.get2() instanceof byte[]) {
                        rawBytes = true;

                        valBytes = (byte[])tup.get2();
                    }
                    else
                        valBytes = ctx.marshaller().marshal(tup.get2());
                }

                ownedValsBytes.add(ctx.marshaller().marshal(F.t(entry.getKey(), tup.get1(), valBytes, rawBytes)));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (ownedValsBytes != null && ownedVals == null) {
            ownedVals = new HashMap<>();

            for (byte[] bytes : ownedValsBytes) {
                GridTuple4<IgniteTxKey<K>, GridCacheVersion, byte[], Boolean> tup = ctx.marshaller().unmarshal(bytes, ldr);

                V val = tup.get4() ? (V)tup.get3() : ctx.marshaller().<V>unmarshal(tup.get3(), ldr);

                ownedVals.put(tup.get1(), F.t(tup.get2(), val, tup.get4() ? null : tup.get3()));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 10:
                if (!writer.writeMessage("dhtVer", dhtVer))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeIgniteUuid("futId", futId))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeCollection("invalidParts", invalidParts, Type.INT))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeIgniteUuid("miniId", miniId))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeCollection("ownedValsBytes", ownedValsBytes, Type.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeCollection("pending", pending, Type.MSG))
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
            case 10:
                dhtVer = reader.readMessage("dhtVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 11:
                futId = reader.readIgniteUuid("futId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 12:
                invalidParts = reader.readCollection("invalidParts", Type.INT);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 13:
                miniId = reader.readIgniteUuid("miniId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 14:
                ownedValsBytes = reader.readCollection("ownedValsBytes", Type.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 15:
                pending = reader.readCollection("pending", Type.MSG);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 56;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxPrepareResponse.class, this, "super", super.toString());
    }
}
