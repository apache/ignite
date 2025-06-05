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

package org.apache.ignite.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteDiagnosticMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int REQUEST_FLAG_MASK = 0x01;

    /** */
    private byte flags;

    /** */
    private long futId;

    /** */
    private byte[] bytes;

    /**
     * Required by {@link GridIoMessageFactory}.
     */
    public IgniteDiagnosticMessage() {
        // No-op.
    }

    /**
     * @param reqBytes Marshalled request.
     * @param futId Future ID.
     * @return Request message.
     */
    public static IgniteDiagnosticMessage createRequest(byte[] reqBytes, long futId) {
        IgniteDiagnosticMessage msg = new IgniteDiagnosticMessage();

        msg.futId = futId;
        msg.bytes = reqBytes;
        msg.flags |= REQUEST_FLAG_MASK;

        return msg;
    }

    /**
     * @param resBytes Marshalled result.
     * @param futId Future ID.
     * @return Response message.
     */
    public static IgniteDiagnosticMessage createResponse(byte[] resBytes, long futId) {
        IgniteDiagnosticMessage msg = new IgniteDiagnosticMessage();

        msg.futId = futId;
        msg.bytes = resBytes;

        return msg;
    }

    /**
     * @param marsh Marshaller.
     * @return Unmarshalled payload.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <T> T unmarshal(Marshaller marsh)
        throws IgniteCheckedException {
        if (bytes == null)
            return null;

        return U.unmarshal(marsh, bytes, null);
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /**
     * @return {@code True} if this is request message.
     */
    public boolean request() {
        return (flags & REQUEST_FLAG_MASK) != 0;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray(bytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeByte(flags))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong(futId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        switch (reader.state()) {
            case 0:
                bytes = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                flags = reader.readByte();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                futId = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -61;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     *
     */
    public abstract static class DiagnosticBaseInfo {
        /**
         * @return Key to group similar messages.
         */
        public abstract Object mergeKey();

        /**
         * @param other Another info of the same type.
         */
        public void merge(DiagnosticBaseInfo other) {
            // No-op.
        }

        /**
         * @param sb String builder.
         * @param ctx Grid context.
         */
        public abstract void appendInfo(StringBuilder sb, GridKernalContext ctx);
    }

    /**
     *
     */
    public static final class TxEntriesInfo extends DiagnosticBaseInfo implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int cacheId;

        /** */
        private Collection<KeyCacheObject> keys;

        /** Empty constructor required by {@link Externalizable}. */
        public TxEntriesInfo() {
            // No-op.
        }

        /**
         * @param cacheId Cache ID.
         * @param keys Keys.
         */
        TxEntriesInfo(int cacheId, Collection<KeyCacheObject> keys) {
            this.cacheId = cacheId;
            this.keys = new HashSet<>(keys);
        }

        /** {@inheritDoc} */
        @Override public void appendInfo(StringBuilder sb, GridKernalContext ctx) {
            sb.append(U.nl());

            GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(cacheId);

            if (cctx == null) {
                sb.append("Failed to find cache with id: ").append(cacheId);

                return;
            }

            try {
                for (KeyCacheObject key : keys)
                    key.finishUnmarshal(cctx.cacheObjectContext(), null);
            }
            catch (IgniteCheckedException e) {
                ctx.cluster().diagnosticLog().error("Failed to unmarshal key: " + e, e);

                sb.append("Failed to unmarshal key: ").append(e).append(U.nl());
            }

            sb.append("Cache entries [cacheId=").append(cacheId)
                .append(", cacheName=").append(cctx.name()).append("]: ");

            for (KeyCacheObject key : keys) {
                GridCacheMapEntry e = (GridCacheMapEntry)cctx.cache().peekEx(key);

                sb.append(U.nl()).append("    Key [key=").append(key).append(", entry=").append(e).append("]");
            }
        }

        /** {@inheritDoc} */
        @Override public Object mergeKey() {
            return new T2<>(getClass(), cacheId);
        }

        /** {@inheritDoc} */
        @Override public void merge(DiagnosticBaseInfo other) {
            TxEntriesInfo other0 = (TxEntriesInfo)other;

            assert other0 != null && cacheId == other0.cacheId : other;

            this.keys.addAll(other0.keys);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            this.keys = new ArrayList<>(keys);

            U.writeCollection(out, this.keys);
            out.writeInt(cacheId);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            keys = U.readCollection(in);
            cacheId = in.readInt();
        }
    }

    /**
     *
     */
    public static final class ExchangeInfo extends DiagnosticBaseInfo implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private AffinityTopologyVersion topVer;

        /** Empty constructor required by {@link Externalizable}. */
        public ExchangeInfo() {
            // No-op.
        }

        /**
         * @param topVer Exchange version.
         */
        ExchangeInfo(AffinityTopologyVersion topVer) {
            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override public void appendInfo(StringBuilder sb, GridKernalContext ctx) {
            sb.append(U.nl());

            List<GridDhtPartitionsExchangeFuture> futs = ctx.cache().context().exchange().exchangeFutures();

            for (GridDhtPartitionsExchangeFuture fut : futs) {
                if (topVer.equals(fut.initialVersion())) {
                    sb.append("Exchange future: ").append(fut);

                    return;
                }
            }

            sb.append("Failed to find exchange future: ").append(topVer);
        }

        /** {@inheritDoc} */
        @Override public Object mergeKey() {
            return new T2<>(getClass(), topVer);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(topVer);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            topVer = (AffinityTopologyVersion)in.readObject();
        }
    }

    /**
     *
     */
    public static final class TxInfo extends DiagnosticBaseInfo implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private GridCacheVersion dhtVer;

        /** */
        private GridCacheVersion nearVer;

        /** Empty constructor required by {@link Externalizable}. */
        public TxInfo() {
            // No-op.
        }

        /**
         * @param dhtVer Tx dht version.
         * @param nearVer Tx near version.
         */
        TxInfo(GridCacheVersion dhtVer, GridCacheVersion nearVer) {
            this.dhtVer = dhtVer;
            this.nearVer = nearVer;
        }

        /** {@inheritDoc} */
        @Override public void appendInfo(StringBuilder sb, GridKernalContext ctx) {
            sb.append(U.nl())
                .append("Related transactions [dhtVer=").append(dhtVer)
                .append(", nearVer=").append(nearVer).append("]: ");

            boolean found = false;

            for (IgniteInternalTx tx : ctx.cache().context().tm().activeTransactions()) {
                if (dhtVer.equals(tx.xidVersion()) || nearVer.equals(tx.nearXidVersion())) {
                    sb.append(U.nl())
                        .append("    ")
                        .append(tx.getClass().getSimpleName())
                        .append(" [ver=").append(tx.xidVersion())
                        .append(", nearVer=").append(tx.nearXidVersion())
                        .append(", topVer=").append(tx.topologyVersion())
                        .append(", state=").append(tx.state())
                        .append(", fullTx=").append(tx).append(']');

                    found = true;
                }
            }

            if (!found)
                sb.append(U.nl()).append("Failed to find related transactions.");
        }

        /** {@inheritDoc} */
        @Override public Object mergeKey() {
            return new T3<>(getClass(), nearVer, dhtVer);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(dhtVer);
            out.writeObject(nearVer);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            dhtVer = (GridCacheVersion)in.readObject();
            nearVer = (GridCacheVersion)in.readObject();
        }
    }

    /**
     * @param sb String builder.
     * @param ctx Context.
     */
    static void dumpNodeBasicInfo(StringBuilder sb, GridKernalContext ctx) {
        sb.append("General node info [id=").append(ctx.localNodeId())
            .append(", client=").append(ctx.clientNode())
            .append(", discoTopVer=").append(ctx.discovery().topologyVersionEx())
            .append(", time=").append(formatTime(U.currentTimeMillis())).append(']');
    }

    /**
     * @param sb String builder.
     * @param ctx Context.
     */
    static void dumpExchangeInfo(StringBuilder sb, GridKernalContext ctx) {
        GridCachePartitionExchangeManager<?, ?> exchMgr = ctx.cache().context().exchange();
        GridDhtTopologyFuture fut = exchMgr.lastTopologyFuture();

        sb.append("Partitions exchange info [readyVer=").append(exchMgr.readyAffinityVersion()).append(']').append(U.nl())
            .append("Last initialized exchange future: ").append(fut);
    }

    /**
     * @param sb String builder.
     * @param ctx Context.
     */
    static void dumpPendingCacheMessages(StringBuilder sb, GridKernalContext ctx) {
        ctx.cache().context().io().dumpPendingMessages(sb);
    }

    /**
     * @param ctx Context.
     * @param nodeId Target node ID.
     * @return Communication information future.
     */
    public static IgniteInternalFuture<String> dumpCommunicationInfo(GridKernalContext ctx, UUID nodeId) {
        if (ctx.config().getCommunicationSpi() instanceof TcpCommunicationSpi)
            return ((TcpCommunicationSpi)ctx.config().getCommunicationSpi()).dumpNodeStatistics(nodeId);
        else
            return new GridFinishedFuture<>("Unexpected communication SPI: " + ctx.config().getCommunicationSpi());
    }

    /**
     * @param time Time.
     * @return Time string.
     */
    private static String formatTime(long time) {
        return IgniteUtils.DEBUG_DATE_FMT.format(Instant.ofEpochMilli(time));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDiagnosticMessage.class, this);
    }
}
