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

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
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
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 *
 */
public class IgniteDiagnosticMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final ThreadLocal<DateFormat> dateFormat = new ThreadLocal<DateFormat>() {
        @Override protected DateFormat initialValue() {
            return new SimpleDateFormat("HH:mm:ss.SSS");
        }
    };

    /** */
    private long futId;

    /** */
    private String msg;

    /** */
    private byte[] cBytes;

    /**
     * Required by {@link GridIoMessageFactory}.
     */
    public IgniteDiagnosticMessage() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param c Closure to run.
     * @param futId Future ID.
     * @return Request message.
     * @throws IgniteCheckedException If failed.
     */
    public static IgniteDiagnosticMessage createRequest(GridKernalContext ctx,
        IgniteClosure<GridKernalContext, String> c,
        long futId)
        throws IgniteCheckedException
    {
        byte[] cBytes = U.marshal(ctx.config().getMarshaller(), c);

        IgniteDiagnosticMessage msg = new IgniteDiagnosticMessage();

        msg.futId = futId;
        msg.cBytes = cBytes;

        return msg;
    }

    /**
     * @param msg0 Message.
     * @param futId Future ID.
     * @return Response message.
     */
    public static IgniteDiagnosticMessage createResponse(String msg0, long futId) {
        IgniteDiagnosticMessage msg = new IgniteDiagnosticMessage();

        msg.futId = futId;
        msg.msg = msg0;

        return msg;
    }

    /**
     * @param ctx Context.
     * @return Unmarshalled closure.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteClosure<GridKernalContext, String> unmarshalClosure(GridKernalContext ctx)
        throws IgniteCheckedException {
        assert cBytes != null;

        return U.unmarshal(ctx, cBytes, null);
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
        return cBytes != null;
    }

    /**
     * @return Message string.
     */
    public String message() {
        return msg;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeByteArray("cBytes", cBytes))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeString("msg", msg))
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

        switch (reader.state()) {
            case 0:
                cBytes = reader.readByteArray("cBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                msg = reader.readString("msg");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(IgniteDiagnosticMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -61;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     *
     */
    public static class BaseClosure implements IgniteClosure<GridKernalContext, String> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected final UUID nodeId;

        /**
         * @param ctx Local node context.
         */
        public BaseClosure(GridKernalContext ctx) {
            this.nodeId = ctx.localNodeId();
        }

        /** {@inheritDoc} */
        @Override public final String apply(GridKernalContext ctx) {
            try {
                StringBuilder sb = new StringBuilder();

                IgniteInternalFuture<String> commInfo = dumpCommunicationInfo(ctx, nodeId);

                sb.append(dumpNodeBasicInfo(ctx));

                sb.append(U.nl()).append(dumpExchangeInfo(ctx));

                String moreInfo = dumpInfo(ctx);

                sb.append(U.nl()).append(commInfo.get());

                if (moreInfo != null)
                    sb.append(U.nl()).append(moreInfo);

                return sb.toString();
            }
            catch (Exception e) {
                ctx.cluster().diagnosticLog().error("Failed to execute diagnostic message closure: " + e, e);

                return "Failed to execute diagnostic message closure: " + e;
            }
        }

        /**
         * @param ctx Context.
         * @return Message.
         */
        protected String dumpInfo(GridKernalContext ctx) {
            return null;
        }
    }

    /**
     *
     */
    public static class TxEntriesInfoClosure extends BaseClosure {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final int cacheId;

        /** */
        private final Collection<KeyCacheObject> keys;

        /**
         * @param ctx Context.
         * @param cacheId Cache ID.
         * @param keys Keys.
         */
        public TxEntriesInfoClosure(GridKernalContext ctx, int cacheId, Collection<KeyCacheObject> keys) {
            super(ctx);

            this.cacheId = cacheId;
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override protected String dumpInfo(GridKernalContext ctx) {
            GridCacheContext cctx = ctx.cache().context().cacheContext(cacheId);

            if (cctx == null)
                return "Failed to find cache with id: " + cacheId;

            try {
                for (KeyCacheObject key : keys)
                    key.finishUnmarshal(cctx.cacheObjectContext(), null);
            }
            catch (IgniteCheckedException e) {
                ctx.cluster().diagnosticLog().error("Failed to unmarshal key: " + e, e);

                return "Failed to unmarshal key: " + e;
            }

            StringBuilder sb = new StringBuilder("Cache entries [cacheId=" + cacheId + ", cacheName=" + cctx.name() + "]: ");

            for (KeyCacheObject key : keys) {
                sb.append(U.nl());

                GridCacheMapEntry e = (GridCacheMapEntry)cctx.cache().peekEx(key);

                sb.append("Key [key=").append(key).append(", entry=").append(e).append("]");
            }

            return sb.toString();
        }
    }

    /**
     *
     */
    public static class ExchangeInfoClosure extends BaseClosure {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final AffinityTopologyVersion topVer;

        /**
         * @param ctx Context.
         * @param topVer Exchange version.
         */
        public ExchangeInfoClosure(GridKernalContext ctx, AffinityTopologyVersion topVer) {
            super(ctx);

            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override protected String dumpInfo(GridKernalContext ctx) {
            List<GridDhtPartitionsExchangeFuture> futs = ctx.cache().context().exchange().exchangeFutures();

            for (GridDhtPartitionsExchangeFuture fut : futs) {
                if (topVer.equals(fut.topologyVersion()))
                    return "Exchange future: " + fut;
            }

            return "Failed to find exchange future: " + topVer;
        }
    }

    /**
     *
     */
    public static class TxInfoClosure extends BaseClosure {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final GridCacheVersion dhtVer;

        /** */
        private final GridCacheVersion nearVer;

        /**
         * @param ctx Context.
         * @param dhtVer Tx dht version.
         * @param nearVer Tx near version.
         */
        public TxInfoClosure(GridKernalContext ctx,
            GridCacheVersion dhtVer,
            GridCacheVersion nearVer) {
            super(ctx);

            this.dhtVer = dhtVer;
            this.nearVer = nearVer;
        }

        /** {@inheritDoc} */
        @Override protected String dumpInfo(GridKernalContext ctx) {
            StringBuilder b = new StringBuilder();

            b.append("Related transactions [dhtVer=").append(dhtVer).
                append(", nearVer=").append(nearVer).append("]: ");

            boolean found = false;

            for (IgniteInternalTx tx : ctx.cache().context().tm().activeTransactions()) {
                if (dhtVer.equals(tx.xidVersion()) || nearVer.equals(tx.nearXidVersion())) {
                    found = true;

                    b.append(U.nl());
                    b.append("Found related ttx [ver=").append(tx.xidVersion()).
                        append(", nearVer=").append(tx.nearXidVersion()).
                        append(", topVer=").append(tx.topologyVersion()).
                        append(", state=").append(tx.state()).
                        append(", fullTx=").append(tx).
                        append("]");
                }
            }

            if (!found) {
                b.append(U.nl());
                b.append("Failed to find related transactions.");
            }

            return b.toString();
        }
    }

    /**
     * @param ctx Context.
     * @return Node information string.
     */
    static String dumpNodeBasicInfo(GridKernalContext ctx) {
        StringBuilder sb = new StringBuilder("General node info [id=").append(ctx.localNodeId());

        sb.append(", client=").append(ctx.clientNode());
        sb.append(", discoTopVer=").append(ctx.discovery().topologyVersionEx());
        sb.append(", time=").append(formatTime(U.currentTimeMillis()));

        sb.append(']');

        return sb.toString();
    }

    /**
     * @param ctx Context.
     * @return Exchange information string.
     */
    static String dumpExchangeInfo(GridKernalContext ctx) {
        GridCachePartitionExchangeManager exchMgr = ctx.cache().context().exchange();

        StringBuilder sb = new StringBuilder("Partitions exchange info [readyVer=").append(exchMgr.readyAffinityVersion());
        sb.append("]");

        GridDhtTopologyFuture fut = exchMgr.lastTopologyFuture();

        sb.append(U.nl()).append("Last initialized exchange future: ").append(fut);
        
        return sb.toString();
    }

    /**
     * @param ctx Context.
     * @param nodeId Target node ID.
     * @return Communication information future.
     */
    public static IgniteInternalFuture<String> dumpCommunicationInfo(GridKernalContext ctx, UUID nodeId) {
        if (ctx.config().getCommunicationSpi() instanceof TcpCommunicationSpi)
            return ((TcpCommunicationSpi) ctx.config().getCommunicationSpi()).dumpNodeStatistics(nodeId);
        else
            return new GridFinishedFuture<>("Unexpected communication SPI: " + ctx.config().getCommunicationSpi());
    }
    /**
     * @param time Time.
     * @return Time string.
     */
    private static String formatTime(long time) {
        return dateFormat.get().format(new Date(time));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDiagnosticMessage.class, this);
    }
}
